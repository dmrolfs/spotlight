package spotlight.analysis

import akka.Done

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import akka.actor.{ Actor, ActorRef, Props }
import akka.agent.Agent
import akka.event.LoggingReceive

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ ask, kleisli }
import com.persist.logging._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import demesne.{ AggregateRootType, BoundedContext, DomainModel, StartTask }
import omnibus.akka.envelope._
import omnibus.akka.metrics.InstrumentedActor
import omnibus.commons.{ TryV, Valid }
import omnibus.commons.concurrent._
import spotlight.Settings
import spotlight.analysis.algorithm._
import spotlight.analysis.algorithm.statistical._
import spotlight.model.outlier.AnalysisPlan

/** Created by rolfsd on 9/29/15.
  */
object DetectionAlgorithmRouter extends ClassLogging {

  sealed trait RouterProtocol

  case class RegisterAlgorithmReference( algorithm: String, handler: ActorRef ) extends RouterProtocol

  case class RegisterAlgorithmRootType(
    algorithm: String,
    algorithmRootType: AggregateRootType,
    model: DomainModel,
    sharded: Boolean = true
  ) extends RouterProtocol

  case class AlgorithmRegistered( algorithm: String ) extends RouterProtocol

  val ContextKey = 'DetectionAlgorithmRouter

  def startTask( configuration: Config )( implicit ec: ExecutionContext ): StartTask = {
    StartTask.withBoundTask( "load user algorithms" ) { bc: BoundedContext ⇒
      val t: Future[StartTask.Result] = {
        for {
          loaded ← Registry.loadAlgorithms( bc, configuration.resolve() )
          registered ← Registry.registerWithRouter( loaded )
          _ = log.info( Map( "@msg" → "starting with new algorithm routes", "loaded" → loaded.mkString( "[", ", ", "]" ) ) )
        } yield {
          log.info( Map( "@msg" → "DetectionAlgorithmRouter routing table", "routing-table" → registered.mkString( "[", ", ", "]" ) ) )
          StartTask.Result( rootTypes = registered.values.toSet )
        }
      }

      t.toTask
    }
  }

  def props( plan: AnalysisPlan.Summary, routingTable: Map[String, AlgorithmRoute] ): Props = {
    Props( new Default( plan, routingTable ) )
  }

  val DispatcherPath: String = OutlierDetection.DispatcherPath

  def name( suffix: String ): String = "router" //"router:" + suffix  //todo suffix is redundant given it's inclusion in foreman naming

  trait Provider {
    def initialRoutingTable: Map[String, AlgorithmRoute]
    def plan: AnalysisPlan.Summary
  }

  private class Default(
    override val plan: AnalysisPlan.Summary,
    override val initialRoutingTable: Map[String, AlgorithmRoute]
  ) extends DetectionAlgorithmRouter with Provider

  object Registry {
    private lazy val algorithmRootTypes: Agent[Map[String, AggregateRootType]] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      Agent( Map.empty[String, AggregateRootType] )
    }

    def registerWithRouter( algorithmRoots: List[( String, AggregateRootType )] ): Future[Map[String, AggregateRootType]] = {
      //TODO: DMR: WORK HERE TO LOG
      algorithmRootTypes alter { _ ++ algorithmRoots }
    }

    def rootTypeFor( algorithm: String )( implicit ec: ExecutionContext ): Option[AggregateRootType] = {
      import scala.concurrent.duration._
      unsafeRootTypeFor( algorithm ) orElse { scala.concurrent.Await.result( futureRootTypeFor( algorithm ), 30.seconds ) }
    }

    def unsafeRootTypeFor( algorithm: String ): Option[AggregateRootType] = {
      log.debug(
        Map(
          "@msg" → "unsafe pull of algorithm root types",
          "root-types" → algorithmRootTypes.get.keySet.mkString( "[", ", ", "]" )
        )
      )
      algorithmRootTypes.get() get algorithm
    }

    def futureRootTypeFor( algorithm: String )( implicit ec: ExecutionContext ): Future[Option[AggregateRootType]] = {
      algorithmRootTypes.future() map { rootTable ⇒
        log.debug(
          Map(
            "@msg" → "safe pull of algorithm root types",
            "root-types" → rootTable.keySet.mkString( "[", ", ", "]" )
          )
        )
        rootTable get algorithm
      }
    }

    type AlgorithmRootType = ( String, AggregateRootType )
    type EC[_] = ExecutionContext
    def loadAlgorithms[_: EC]( boundedContext: BoundedContext, configuration: Config ): Future[List[AlgorithmRootType]] = {
      type AlgorithmClass = Class[_ <: Algorithm[_]]
      type AlgorithmClasses = Map[String, AlgorithmClass]

      def algorithmRootTypeFor( clazz: AlgorithmClass ): Future[AggregateRootType] = {
        Future.fromTry[AggregateRootType] {
          Try {
            import scala.reflect.runtime.{ universe ⇒ ru }
            val loader = getClass.getClassLoader
            val mirror = ru runtimeMirror loader
            val algorithmSymbol = mirror moduleSymbol clazz
            val algorithmMirror = mirror reflectModule algorithmSymbol
            val algorithm = algorithmMirror.instance.asInstanceOf[Algorithm[_]]
            algorithm.module.rootType
          }
        }
      }

      val userAlgorithms = kleisli[Future, ( DomainModel, Config ), ( DomainModel, AlgorithmClasses )] {
        case ( m, c ) ⇒
          Settings.userAlgorithmClassesFrom( c ) match {
            case scalaz.Success( as ) ⇒ Future successful ( m, as )
            case scalaz.Failure( exs ) ⇒ {
              exs foreach { ex ⇒ log.error( "loading user algorithm failed", ex ) }
              Future failed exs.head
            }
          }
      }

      val collectRootTypes = kleisli[Future, ( DomainModel, AlgorithmClasses ), List[AlgorithmRootType]] {
        case ( m, acs ) ⇒ {
          acs
            .toList
            .traverseU {
              case ( a, c ) ⇒ {
                algorithmRootTypeFor( c ).map { rt ⇒
                  log.debug(
                    Map(
                      "@msg" → "collecting unknown algorithms",
                      "algorithm" → a,
                      "fqcn" → c.getName,
                      "identified-root-type" → rt.toString,
                      "is-known" → m.rootTypes.contains( rt )
                    )
                  )

                  ( a, rt )
                }
              }
            }
          //              .map { arts ⇒ arts.flatten }
        }
      }

      val loadRootTypesIntoModel = kleisli[Future, List[AlgorithmRootType], List[AlgorithmRootType]] { arts ⇒
        boundedContext
          .addAggregateTypes( arts.map( _._2 ).toSet )
          .map { _ ⇒
            log.debug(
              Map(
                "@msg" → "loaded algorithm root types into bounded context",
                "algorithm root types" → arts.mkString( "[", ", ", "]" )
              )
            )

            arts
          }
      }

      //      val registerWithRouter = kleisli[Future, List[AlgorithmRootType], List[AlgorithmRootType]] { arts ⇒
      //        for {
      //          registered ← Registry.registerWithRouter( arts )
      //          _ = log.debug( Map( "@msg" → "registered with router", "algorithm root types" → registered.mkString( "[", ", ", "]" ) ) )
      //        } yield registered.toList
      //      }

      val addModel = kleisli[Future, ( BoundedContext, Config ), ( DomainModel, Config )] {
        case ( bc, c ) ⇒ bc.futureModel map { ( _, c ) }
      }

      val load = addModel >=> userAlgorithms >=> collectRootTypes >=> loadRootTypesIntoModel // >=> registerWithRouter

      load.run( boundedContext, configuration )
    }
  }
}

class DetectionAlgorithmRouter extends Actor with EnvelopingActor with InstrumentedActor with ActorLogging {
  provider: DetectionAlgorithmRouter.Provider ⇒

  import DetectionAlgorithmRouter._

  var routingTable: Map[String, AlgorithmRoute] = provider.initialRoutingTable
  def addRoute( algorithm: String, resolver: AlgorithmRoute ): Unit = { routingTable += ( algorithm → resolver ) }
  log.debug(
    Map(
      "@msg" → "created routing-table",
      "self" → self.path.name,
      "routing-table" → routingTable.mkString( "[", ", ", "]" )
    )
  )

  def contains( algorithm: String ): Boolean = {
    val found = routingTable contains algorithm

    log.debug(
      Map(
        "@msg" → "looking for",
        "self" → self.path.name,
        "algorithm" → algorithm,
        "routing-keys" → routingTable.keys.mkString( "[", ", ", "]" ),
        "found" → found
      )
    )

    found
  }

  val registration: Receive = {
    case RegisterAlgorithmReference( algorithm, handler ) ⇒ {
      addRoute( algorithm, AlgorithmRoute.DirectRoute( handler ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, algorithmRootType, model, true ) ⇒ {
      log.debug(
        Map(
          "@msg" → "received RegisterAlgorithmRootType with sharding",
          "algorithm" → algorithm,
          "algorithm-root-type" → algorithmRootType.name,
          "model" → model.toString
        )
      )
      addRoute( algorithm, AlgorithmRoute.routeFor( plan, algorithmRootType )( model ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, algorithmRootType, model, _ ) ⇒ {
      addRoute( algorithm, AlgorithmRoute.RootTypeRoute( plan, algorithmRootType, model ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }
  }

  val routing: Receive = {
    case m: DetectUsing if contains( m.algorithm ) ⇒ routingTable( m.algorithm ) forward m
  }

  override val receive: Receive = LoggingReceive { around( routing orElse registration ) }

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing ⇒ {
        log.error(
          Map(
            "@msg" → "cannot route unregistered algorithm",
            "algorithm" → m.algorithm,
            "routing-keys" → routingTable.keySet.mkString( "[", ", ", "]" ),
            "found" → contains( m.algorithm )
          )
        )
      }

      case m ⇒ {
        log.error(
          Map(
            "@msg" → "router ignoring unrecognized message",
            "message" → m.toString,
            "routing-keys" → routingTable.keySet.mkString( "[", ", ", "]" )
          )
        )
      }
    }
  }
}
