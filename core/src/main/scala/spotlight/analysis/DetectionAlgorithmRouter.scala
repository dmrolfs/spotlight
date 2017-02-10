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
    StartTask.withBoundTask( "load user algorithms" ) { bc: BoundedContext =>
      val t: Future[StartTask.Result] = {
        for {
          algoRoots <- Registry.loadAlgorithms( bc, configuration )
          roots <- Registry.registerWithRouter( algoRoots )
          unknowns = algoRoots.map { case (_, rt) => rt }.toSet
          _ = log.info( Map( "@msg" -> "#TEST unknown algorithm routes", "unknowns" -> unknowns.mkString( "[", ", ", "]" ) ) )
        } yield {
          log
          .info( Map( "@msg" -> "DetectionAlgorithmRouter routing table", "routing-table" -> roots.mkString( "[", ", ", "]" ) ) )
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
                  ExponentialMovingAverageAlgorithm.algorithm.label -> ExponentialMovingAverageAlgorithm.rootType,
                  GrubbsAlgorithm.algorithm.label -> GrubbsAlgorithm.rootType,
                  SimpleMovingAverageAlgorithm.algorithm.label -> SimpleMovingAverageAlgorithm.rootType
                )
           )
    }

    def registerWithRouter( algorithmRoots: List[(String, AggregateRootType)] ): Future[Map[String, AggregateRootType]] = {
      algorithmRootTypes alter { _ ++ algorithmRoots }
    }

    def rootTypeFor( algorithm: String )( implicit ec: ExecutionContext ): Option[AggregateRootType] = {
      import scala.concurrent.duration._
      unsafeRootTypeFor( algorithm ) orElse {scala.concurrent.Await.result( futureRootTypeFor( algorithm ), 30.seconds )}
    }

    def unsafeRootTypeFor( algorithm: String ): Option[AggregateRootType] = {
      log.debug(
        Map("@msg" -> "unsafe algorithm root types", "root-types" -> algorithmRootTypes.get.keySet.mkString( "[", ", ", "]" ))
      )
      algorithmRootTypes.get() get algorithm
    }

    def futureRootTypeFor( algorithm: String )( implicit ec: ExecutionContext ): Future[Option[AggregateRootType]] = {
      algorithmRootTypes.future() map { rootTable =>
        log.debug( Map( "@msg" -> "safe algorithm root types", "root-types" -> rootTable.keySet.mkString( "[", ", ", "]" ) ) )
        rootTable get algorithm
      }
    }

    def userAlgorithms( configuration: Config ): Valid[Map[String, Class[_ <: AlgorithmModule]]] = {
      def loadClass( algorithm: String, fqcn: String ): TryV[Class[_ <: AlgorithmModule]] = {
        \/ fromTryCatchNonFatal { Class.forName( fqcn, true, getClass.getClassLoader ).asInstanceOf[Class[_ <: AlgorithmModule]] }
      }

      def unwrapAlgorithmFQCN( entry: (String, ConfigValue) ): TryV[(String, String)] = {
        val algorithm = entry._1
        val value = entry._2.unwrapped()

        entry._2.valueType() match {
          case ConfigValueType.STRING => ( algorithm, value.asInstanceOf[String] ).right
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
        Map.empty[String, Class[_ <: AlgorithmModule]].successNel
      }
    }


    def loadAlgorithms(
      boundedContext: BoundedContext,
      configuration: Config
    )(
      implicit ec: ExecutionContext
    ): Future[List[(String, AggregateRootType)]] = {
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

      def isKnownAlgorithm( rootType: AggregateRootType )( implicit model: DomainModel ): Boolean = {
        model.rootTypes contains rootType
      }

      val userAlgos: TryV[Map[String, Class[_ <: AlgorithmModule]]] = userAlgorithms( configuration ).disjunction leftMap { exs =>
        exs foreach { ex => log.error( "loading user algorithm failed", ex ) }
        exs.head
      }

      def collectUnknowns(
        algoClasses: Map[String, Class[_ <: AlgorithmModule]]
      )(
        implicit model: DomainModel
      ): TryV[List[(String, AggregateRootType)]] = {
        algoClasses
        .toList
        .traverseU { case (a, c) =>
          algorithmRootTypeFor( c ) map { rt =>
            if ( isKnownAlgorithm( rt ) ) None
            else {
              log.info( Map( "@msg" -> "loaded algorithm root type", "algorithm" -> a, "root-type" -> rt.name ) )
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

  var routingTable: Map[String, AlgorithmRoute] = provider.initialRoutingTable
  def addRoute( algorithm: String, resolver: AlgorithmRoute ): Unit = { routingTable += ( algorithm -> resolver ) }
  log.debug(Map("@msg"->"created routing-table", "self"->self.path.name, "routing-table"->routingTable.mkString("[", ", ", "]")))

  def contains( algorithm: String ): Boolean = {
    val found = routingTable contains algorithm

    log.debug(
      Map(
        "@msg" -> "looking for",
        "self" -> self.path.name,
        "algorithm" -> algorithm,
        "routing-table" -> routingTable.keys.mkString("[", ", ", "]"),
        "found" -> found
      )
    )

    found
  }

  val registration: Receive = {
    case RegisterAlgorithmReference( algorithm, handler ) => {
      addRoute( algorithm, AlgorithmRoute.DirectRoute( handler ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, algorithmRootType, model, true ) => {
      log.debug(
        Map(
          "@msg" -> "received RegisterAlgorithmRootType with sharding",
          "algorithm" -> algorithm,
          "algorithm-root-type" -> algorithmRootType.name,
          "model" -> model.toString
        )
      )
      addRoute( algorithm, AlgorithmRoute.routeFor(plan, algorithmRootType)( model ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, algorithmRootType, model, _ ) => {
      addRoute( algorithm, AlgorithmRoute.RootTypeRoute(plan, algorithmRootType, model) )
      sender() !+ AlgorithmRegistered( algorithm )
    }
  }

  val routing: Receive = {
    case m: DetectUsing if contains( m.algorithm ) => routingTable( m.algorithm ) forward m
  }

  override val receive: Receive = LoggingReceive{ around( routing orElse registration ) }

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => {
        log.error(
            Map(
            "@msg" -> "cannot route unregistered algorithm",
            "algorithm" -> m.algorithm,
            "routing-keys" -> routingTable.keySet.mkString("[", ", ", "]"),
            "found" -> contains(m.algorithm)
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
