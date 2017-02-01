package spotlight.analysis

import scala.concurrent.{ExecutionContext, Future}
import akka.actor.{Actor, ActorRef, Props}
import akka.agent.Agent
import akka.event.LoggingReceive
import com.persist.logging._

import scalaz._
import Scalaz._
import com.typesafe.config.{Config, ConfigValue, ConfigValueType}
import demesne.{AggregateRootType, BoundedContext, DomainModel, StartTask}
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor
import peds.commons.{TryV, Valid}
import peds.commons.concurrent._
import spotlight.analysis.algorithm._
import spotlight.analysis.algorithm.statistical._
import spotlight.model.outlier.AnalysisPlan


/**
 * Created by rolfsd on 9/29/15.
 */
object DetectionAlgorithmRouter extends ClassLogging {

  sealed trait RouterProtocol

  case class RegisterAlgorithmReference(algorithm: Symbol, handler: ActorRef) extends RouterProtocol

  case class RegisterAlgorithmRootType(
    algorithm: Symbol,
    algorithmRootType: AggregateRootType,
    model: DomainModel,
    sharded: Boolean = true
  ) extends RouterProtocol

  case class AlgorithmRegistered(algorithm: Symbol) extends RouterProtocol


  val ContextKey = 'DetectionAlgorithmRouter

  def startTask( configuration: Config )( implicit ec: ExecutionContext ): StartTask = {
    StartTask.withBoundTask( "load user algorithms" ) { bc: BoundedContext =>
      val t: Future[StartTask.Result] = {
        for {
          algoRoots <- Registry.loadAlgorithms( bc, configuration )
          roots <- Registry.registerWithRouter( algoRoots )
          unknowns = algoRoots.map { case (_, rt) => rt }.toSet
          _ = log.info( Map( "@msg" -> "#TEST unknown algorithm routes", "unkowns" -> unknowns.mkString( "[", ", ", "]" ) ) )
        } yield {
          log
          .info( Map( "@msg" -> "DetectionAlgorithmRouter routing table", "routing-table" -> roots.mkString( "[", ", ", "]" ) ) )
          StartTask.Result( rootTypes = unknowns )
        }
      }

      t.toTask
    }
  }


  def props( plan: AnalysisPlan.Summary, routingTable: Map[Symbol, AlgorithmRoute] ): Props = {
    Props( new Default( plan, routingTable ) )
  }

  val DispatcherPath: String = OutlierDetection.DispatcherPath

  def name( suffix: String ): String = "router-" + suffix


  trait Provider {
    def initialRoutingTable: Map[Symbol, AlgorithmRoute]

    def plan: AnalysisPlan.Summary
  }

  private class Default(
    override val plan: AnalysisPlan.Summary,
    override val initialRoutingTable: Map[Symbol, AlgorithmRoute]
  ) extends DetectionAlgorithmRouter with Provider



  object Registry {
    private lazy val algorithmRootTypes: Agent[Map[Symbol, AggregateRootType]] = {
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
                  ExponentialMovingAverageAlgorithm.algorithm.label -> ExponentialMovingAverageAlgorithm.rootType,
                  GrubbsAlgorithm.algorithm.label -> GrubbsAlgorithm.rootType,
                  SimpleMovingAverageAlgorithm.algorithm.label -> SimpleMovingAverageAlgorithm.rootType
                )
           )
    }

    def registerWithRouter( algorithmRoots: List[(Symbol, AggregateRootType)] ): Future[Map[Symbol, AggregateRootType]] = {
      algorithmRootTypes alter { _ ++ algorithmRoots }
    }

    def rootTypeFor( algorithm: Symbol ) (implicit ec: ExecutionContext ): Option[AggregateRootType] = {
      import scala.concurrent.duration._
      unsafeRootTypeFor( algorithm ) orElse {scala.concurrent.Await.result( futureRootTypeFor( algorithm ), 30.seconds )}
    }

    def unsafeRootTypeFor( algorithm: Symbol ): Option[AggregateRootType] = {
      log.debug(
        Map("@msg" -> "unsafe algorithm root types", "root-types" -> algorithmRootTypes.get.keySet.mkString( "[", ", ", "]" ))
      )
      algorithmRootTypes.get() get algorithm
    }

    def futureRootTypeFor( algorithm: Symbol )( implicit ec: ExecutionContext ): Future[Option[AggregateRootType]] = {
      algorithmRootTypes.future() map { rootTable =>
        log.debug( Map( "@msg" -> "safe algorithm root types", "root-types" -> rootTable.keySet.mkString( "[", ", ", "]" ) ) )
        rootTable get algorithm
      }
    }

    def userAlgorithms( configuration: Config ): Valid[Map[Symbol, Class[_ <: AlgorithmModule]]] = {
      def loadClass(algorithm: Symbol, fqcn: String): TryV[Class[_ <: AlgorithmModule]] = {
        \/ fromTryCatchNonFatal {Class.forName( fqcn, true, getClass.getClassLoader ).asInstanceOf[Class[_ <: AlgorithmModule]]}
      }

      def unwrapAlgorithmFQCN( entry: (String, ConfigValue) ): TryV[(Symbol, String)] = {
        val algorithm = Symbol( entry._1 )
        val value = entry._2.unwrapped()

        entry._2.valueType() match {
          case ConfigValueType.STRING => (algorithm, value.asInstanceOf[String]).right
          case _ => InsufficientAlgorithmModuleError( algorithm, value.toString ).left
        }
      }

      val AlgorithmPath = "spotlight.algorithms"
      if ( configuration hasPath AlgorithmPath ) {
        import scala.collection.JavaConversions._

        configuration.getConfig( AlgorithmPath ).entrySet().toList
        .map { entry => (entry.getKey, entry.getValue) }
        .traverseU { entry =>
          val ac = for {
            algorithmFqcn <- unwrapAlgorithmFQCN( entry )
            (algorithm, fqcn) = algorithmFqcn
            clazz <- loadClass( algorithm, fqcn )
          } yield (algorithm, clazz)
          ac.validationNel
        }
        .map { algorithmClasses => Map( algorithmClasses: _* ) }
      } else {
        Map.empty[Symbol, Class[_ <: AlgorithmModule]].successNel
      }
    }


    def loadAlgorithms(
      boundedContext: BoundedContext,
      configuration: Config
    )(
      implicit ec: ExecutionContext
    ): Future[List[(Symbol, AggregateRootType)]] = {
      def algorithmRootTypeFor(clazz: Class[_ <: AlgorithmModule]): TryV[AggregateRootType] = {
        \/ fromTryCatchNonFatal {
          import scala.reflect.runtime.{universe => ru}
          val loader = getClass.getClassLoader
          val mirror = ru runtimeMirror loader
          val moduleSymbol = mirror moduleSymbol clazz
          val moduleMirror = mirror reflectModule moduleSymbol
          val module = moduleMirror.instance.asInstanceOf[AlgorithmModule]
          module.rootType
        }
      }

      def isKnownAlgorithm(rootType: AggregateRootType)(implicit model: DomainModel): Boolean = {
        model.rootTypes contains rootType
      }

      val userAlgos: TryV[Map[Symbol, Class[_ <: AlgorithmModule]]] = userAlgorithms( configuration ).disjunction leftMap { exs =>
        exs foreach { ex => log.error( "loading user algorithm failed", ex ) }
        exs.head
      }

      def collectUnknowns(
        algoClasses: Map[Symbol, Class[_ <: AlgorithmModule]]
      )(
        implicit model: DomainModel
      ): TryV[List[(Symbol, AggregateRootType)]] = {
        algoClasses
        .toList
        .traverseU { case (a, c) =>
          algorithmRootTypeFor( c ) map { rt =>
            if ( isKnownAlgorithm( rt ) ) None
            else {
              log.info( Map( "@msg" -> "loaded algorithm root type", "algorithm" -> a.name, "root-type" -> rt.name ) )
              Some( (a, rt) )
            }
          }
        }
        .map {_.flatten}
      }

      boundedContext.futureModel
      .flatMap { implicit model =>
        val unknownRoots = {
          for {
            algorithmClasses <- userAlgos
            unknowns <- collectUnknowns( algorithmClasses )
          } yield {
            unknowns
          }
        }

        unknownRoots match {
          case \/-( us ) => Future successful us
          case -\/( ex ) => Future failed ex
        }
      }
    }
  }
}


class DetectionAlgorithmRouter extends Actor with EnvelopingActor with InstrumentedActor with ActorLogging {
  provider: DetectionAlgorithmRouter.Provider =>

  import DetectionAlgorithmRouter._

  var routingTable: Map[Symbol, AlgorithmRoute] = provider.initialRoutingTable
  def addRoute( algorithm: Symbol, resolver: AlgorithmRoute ): Unit = { routingTable += ( algorithm -> resolver ) }
  log.debug(Map("@msg"->"created routing-table", "self"->self.path.name, "routing-table"->routingTable.mkString("[", ", ", "]")))

  def contains( algorithm: Symbol ): Boolean = {
    log.debug(
      Map(
        "@msg" -> "looking for",
        "self" -> self.path.name,
        "algorithm" -> algorithm.name,
        "routing-table" -> routingTable.keys.mkString("[", ", ", "]")
      )
    )

    routingTable contains algorithm
  }

  val registration: Receive = {
//    case class RegisterAlgorithmRootType( algorithm: Symbol, handler: AggregateRootType ) extends RouterProtocol
    case RegisterAlgorithmReference( algorithm, handler ) => {
      addRoute( algorithm, AlgorithmRoute.DirectRoute( handler ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, algorithmRootType, model, true ) => {
      log.debug(
        Map(
          "@msg" -> "received RegisterAlgorithmRootType with sharding",
          "algorithm" -> algorithm.name,
          "algorithm-root-type" -> algorithmRootType.name,
          "model" -> model.toString
        )
      )
      addRoute( algorithm, AlgorithmRoute.routeFor(plan, algorithmRootType)( model ) )
//      addRoute( algorithm, ShardedRootTypeProxy( plan, algorithmRootType, model ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, algorithmRootType, model, _ ) => {
      addRoute( algorithm, AlgorithmRoute.RootTypeRoute( algorithmRootType, model ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }
  }

  val routing: Receive = {
    case m: DetectUsing if contains( m.algorithm ) => routingTable( m.algorithm ) forward m
  }

  override val receive: Receive = LoggingReceive{ around( registration orElse routing ) }

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => {
        log.error(
            Map(
            "@msg" -> "cannot route unregistered algorithm",
            "algorithm" -> m.algorithm.name,
            "routing-keys" -> routingTable.keySet.mkString( "[", ", ", "]" )
          )
        )
      }

      case m => {
        log.error(
          Map(
            "@msg" -> "router ignoring unrecognized message",
            "message" -> m.toString,
            "routing-keys" -> routingTable.keySet.mkString( "[", ", ", "]" )
          )
        )
      }
    }
  }
}
