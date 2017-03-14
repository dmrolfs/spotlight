package spotlight.analysis

import scala.concurrent.{ ExecutionContext, Future }
import akka.actor.{ Actor, ActorRef, Props }
import akka.agent.Agent
import akka.event.LoggingReceive

import scalaz._
import Scalaz._
import com.persist.logging._
import com.typesafe.config.{ Config, ConfigObject, ConfigValue }
import net.ceedubs.ficus.Ficus._
import demesne.{ AggregateRootType, BoundedContext, DomainModel, StartTask }
import omnibus.akka.envelope._
import omnibus.akka.metrics.InstrumentedActor
import omnibus.commons.{ TryV, Valid }
import omnibus.commons.concurrent._
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
          algoRoots ← Registry.loadAlgorithms( bc, configuration.resolve() )
          roots ← Registry.registerWithRouter( algoRoots )
          unknowns = algoRoots.map { case ( _, rt ) ⇒ rt }.toSet
          _ = log.info( Map( "@msg" → "starting with new algorithm routes", "unknowns" → unknowns.mkString( "[", ", ", "]" ) ) )
        } yield {
          log.info( Map( "@msg" → "DetectionAlgorithmRouter routing table", "routing-table" → roots.mkString( "[", ", ", "]" ) ) )
          StartTask.Result( rootTypes = unknowns )
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

      Agent(
        Map(
          //        SeriesDensityAnalyzer.Algorithm -> ???, // SeriesDensityAnalyzer.props( router ),
          //        SeriesCentroidDensityAnalyzer.Algorithm -> ???, // SeriesCentroidDensityAnalyzer.props( router ),
          //        CohortDensityAnalyzer.Algorithm -> ???, // CohortDensityAnalyzer.props( router ),
          //        FirstHourAverageAnalyzer.Algorithm -> ???, // FirstHourAverageAnalyzer.props( router ),
          //        HistogramBinsAnalyzer.Algorithm -> ???, // HistogramBinsAnalyzer.props( router ),
          //        KolmogorovSmirnovAnalyzer.Algorithm -> ???, // KolmogorovSmirnovAnalyzer.props( router ),
          //        LeastSquaresAnalyzer.Algorithm -> ???, // LeastSquaresAnalyzer.props( router ),
          //        MeanSubtractionCumulationAnalyzer.Algorithm -> ???, // MeanSubtractionCumulationAnalyzer.props( router ),
          //        MedianAbsoluteDeviationAnalyzer.Algorithm -> ???, // MedianAbsoluteDeviationAnalyzer.props( router ),
          //        SeasonalExponentialMovingAverageAnalyzer.Algorithm -> ??? // SeasonalExponentialMovingAverageAnalyzer.props( router )
          ExponentialMovingAverageAlgorithm.label → ExponentialMovingAverageAlgorithm.module.rootType,
          GrubbsAlgorithm.label → GrubbsAlgorithm.module.rootType,
          SimpleMovingAverageAlgorithm.label → SimpleMovingAverageAlgorithm.module.rootType
        )
      )
    }

    def registerWithRouter( algorithmRoots: List[( String, AggregateRootType )] ): Future[Map[String, AggregateRootType]] = {
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

    def userAlgorithms( configuration: Config ): Valid[Map[String, Class[_ <: Algorithm[_]]]] = {
      def loadClass( algorithm: String, fqcn: String ): TryV[Class[_ <: Algorithm[_]]] = {
        \/ fromTryCatchNonFatal { Class.forName( fqcn, true, getClass.getClassLoader ).asInstanceOf[Class[_ <: Algorithm[_]]] }
      }

      def unwrapAlgorithmFQCN( algorithm: String, cv: ConfigValue ): TryV[( String, String )] = {
        import scala.reflect._
        val ConfigObjectType = classTag[ConfigObject]
        val ClassPath = "class"
        val value = cv.unwrapped()
        log.debug(
          Map(
            "@msg" → "unwrapping algorithm FQCN",
            "algorithm" → algorithm,
            "config-value-type" → cv.valueType,
            "config-value" → cv.toString,
            "is-object" → ConfigObjectType.unapply( cv ).isDefined
          )
        )

        val algoFQCN = for {
          algoConfig ← ConfigObjectType.unapply( cv ) map { _.toConfig }
          fqcn ← algoConfig.as[Option[String]]( ClassPath )
          _ = log.debug( Map( "@msg" → "fqcn from algo config", "algorithm" → algorithm, "fqcn" → fqcn ) )
        } yield ( algorithm, fqcn ).right

        algoFQCN getOrElse InsufficientAlgorithmError( algorithm, value.toString ).left
      }

      val AlgorithmPath = "spotlight.algorithms"
      configuration.as[Option[Config]]( AlgorithmPath )
        .map { algoConfig ⇒
          import scala.collection.JavaConverters._
          algoConfig.root.entrySet.asScala.toList
            .map { entry ⇒ ( entry.getKey, entry.getValue ) }
            .traverseU {
              case ( a, cv ) ⇒
                val ac = for {
                  algorithmFqcn ← unwrapAlgorithmFQCN( a, cv )
                  ( algorithm, fqcn ) = algorithmFqcn
                  clazz ← loadClass( algorithm, fqcn )
                } yield ( algorithm, clazz )
                ac.validationNel
            }
            .map { algorithmClasses ⇒ Map( algorithmClasses: _* ) }
        }
        .getOrElse {
          Map.empty[String, Class[_ <: Algorithm[_]]].successNel
        }
    }

    def loadAlgorithms(
      boundedContext: BoundedContext,
      configuration: Config
    )(
      implicit
      ec: ExecutionContext
    ): Future[List[( String, AggregateRootType )]] = {
      def algorithmRootTypeFor( clazz: Class[_ <: Algorithm[_]] ): TryV[AggregateRootType] = {
        \/ fromTryCatchNonFatal {
          import scala.reflect.runtime.{ universe ⇒ ru }
          val loader = getClass.getClassLoader
          val mirror = ru runtimeMirror loader
          val algorithmSymbol = mirror moduleSymbol clazz
          val algorithmMirror = mirror reflectModule algorithmSymbol
          val algorithm = algorithmMirror.instance.asInstanceOf[Algorithm[_]]
          algorithm.module.rootType
        }
      }

      def isKnownAlgorithm( rootType: AggregateRootType )( implicit model: DomainModel ): Boolean = {
        model.rootTypes contains rootType
      }

      val userAlgos: TryV[Map[String, Class[_ <: Algorithm[_]]]] = {
        userAlgorithms( configuration ).disjunction
          .leftMap { exs ⇒
            exs foreach { ex ⇒ log.error( "loading user algorithm failed", ex ) }
            exs.head
          }
      }

      def collectUnknowns(
        algoClasses: Map[String, Class[_ <: Algorithm[_]]]
      )(
        implicit
        model: DomainModel
      ): TryV[List[( String, AggregateRootType )]] = {
        algoClasses
          .toList
          .traverseU {
            case ( a, c ) ⇒
              algorithmRootTypeFor( c ) map { rt ⇒
                if ( isKnownAlgorithm( rt ) ) None
                else {
                  log.info( Map( "@msg" → "loaded algorithm root type", "algorithm" → a, "root-type" → rt.name ) )
                  Some( ( a, rt ) )
                }
              }
          }
          .map { _.flatten }
      }

      boundedContext.futureModel
        .flatMap { implicit model ⇒
          val unknownRoots = {
            for {
              algorithmClasses ← userAlgos
              unknowns ← collectUnknowns( algorithmClasses )
            } yield {
              unknowns
            }
          }

          unknownRoots match {
            case \/-( us ) ⇒ Future successful us
            case -\/( ex ) ⇒ Future failed ex
          }
        }
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
      "routing-table" → routingTable
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
