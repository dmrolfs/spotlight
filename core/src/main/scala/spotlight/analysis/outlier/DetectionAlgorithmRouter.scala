package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
import akka.Done
import akka.actor._
import akka.agent.Agent
import akka.event.LoggingReceive
import scalaz._
import Scalaz._
import com.typesafe.config.{Config, ConfigValue, ConfigValueType}
import com.typesafe.scalalogging.LazyLogging
import demesne.{AggregateRootType, BoundedContext, DomainModel, StartTask}
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor
import peds.commons.{TryV, Valid}
import peds.commons.log.Trace
import peds.commons.concurrent._
import spotlight.analysis.outlier.algorithm.{AlgorithmModule, InsufficientAlgorithmModuleError}
import spotlight.analysis.outlier.algorithm.statistical._



/**
 * Created by rolfsd on 9/29/15.
 */
object DetectionAlgorithmRouter extends LazyLogging {
  private val trace = Trace[DetectionAlgorithmRouter.type]


  sealed trait RouterProtocol
  case class RegisterDetectionAlgorithm( algorithm: Symbol, handler: ActorRef ) extends RouterProtocol
  case class AlgorithmRegistered( algorithm: Symbol ) extends RouterProtocol

  val ContextKey = 'DetectionAlgorithmRouter

  def startTask( configuration: Config )( implicit ec: ExecutionContext ): StartTask = {
    StartTask.withBoundTask( "load user algorithms" ){ bc: BoundedContext =>
      val t = {
        for {
          algoRoots <- loadAlgorithms( bc, configuration )
          roots <- registerWithRouter( algoRoots )
        } yield {
          logger.info( "DetectionAlgorithmRouter master table: [{}]", roots.mkString("\n", ", \n", "\n") )
          Done
        }
      }
      t.toTask
    }
  }


  def props( routingTable: Map[Symbol, ActorRef] ): Props = Props( new Default( routingTable ) )
  val DispatcherPath: String = OutlierDetection.DispatcherPath

  def name( scope: String ): String = "DetectionRouter-" + scope

  private class Default( override val initialRoutingTable: Map[Symbol, ActorRef] ) extends DetectionAlgorithmRouter with Provider

  trait Provider {
    def initialRoutingTable: Map[Symbol, ActorRef]
  }


  def unsafeRootTypeFor( algorithm: Symbol ): Option[AggregateRootType] = algorithmRootTypes.get() get algorithm

  def futureRootTypeFor( algorithm: Symbol )( implicit ec: ExecutionContext ): Future[Option[AggregateRootType]] = {
    algorithmRootTypes.future() map { _ get algorithm }
  }

  private lazy val algorithmRootTypes: Agent[Map[Symbol, AggregateRootType]] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    Agent(
      Map(
//        SeriesDensityAnalyzer.Algorithm -> ???, // SeriesDensityAnalyzer.props( router ),
//        SeriesCentroidDensityAnalyzer.Algorithm -> ???, // SeriesCentroidDensityAnalyzer.props( router ),
//        CohortDensityAnalyzer.Algorithm -> ???, // CohortDensityAnalyzer.props( router ),
//        ExponentialMovingAverageAnalyzer.Algorithm -> ???, // ExponentialMovingAverageAnalyzer.props( router ),
//        FirstHourAverageAnalyzer.Algorithm -> ???, // FirstHourAverageAnalyzer.props( router ),
//        HistogramBinsAnalyzer.Algorithm -> ???, // HistogramBinsAnalyzer.props( router ),
//        KolmogorovSmirnovAnalyzer.Algorithm -> ???, // KolmogorovSmirnovAnalyzer.props( router ),
//        LeastSquaresAnalyzer.Algorithm -> ???, // LeastSquaresAnalyzer.props( router ),
//        MeanSubtractionCumulationAnalyzer.Algorithm -> ???, // MeanSubtractionCumulationAnalyzer.props( router ),
//        MedianAbsoluteDeviationAnalyzer.Algorithm -> ???, // MedianAbsoluteDeviationAnalyzer.props( router ),
//        SeasonalExponentialMovingAverageAnalyzer.Algorithm -> ??? // SeasonalExponentialMovingAverageAnalyzer.props( router )
           GrubbsAlgorithm.algorithm.label -> GrubbsAlgorithm.rootType,
           SimpleMovingAverageAlgorithm.algorithm.label -> SimpleMovingAverageAlgorithm.rootType
      )
    )
  }


  def userAlgorithms( configuration: Config ): Valid[Map[Symbol, Class[_ <: AlgorithmModule]]] = trace.block( "userAlgorithms" ) {
    def loadClass( algorithm: Symbol, fqcn: String ): TryV[Class[_ <: AlgorithmModule]] = {
      \/ fromTryCatchNonFatal { Class.forName( fqcn, true, getClass.getClassLoader ).asInstanceOf[Class[_ <: AlgorithmModule]] }
    }

    def unwrapAlgorithmFQCN( entry: (String, ConfigValue) ): TryV[(Symbol, String)] = {
      val algorithm = Symbol( entry._1 )
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
        } yield ( algorithm, clazz )
        ac.validationNel
      }
      .map { algorithmClasses => Map( algorithmClasses:_* ) }
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
    def algorithmRootType( clazz: Class[_ <: AlgorithmModule] ): TryV[AggregateRootType] = {
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

    val userAlgos: TryV[Map[Symbol, Class[_ <: AlgorithmModule]]] = userAlgorithms( configuration ).disjunction leftMap { exs =>
      exs foreach { ex => logger.error( "loading user algorithm failed", ex ) }
      exs.head
    }

    boundedContext.futureModel
    .flatMap { implicit model =>
      val unknownRoots = {
        for {
          algorithmClasses <- userAlgos
          unknowns <- {
            algorithmClasses.toList
            .traverseU { case (a, c) =>
              algorithmRootType( c ) map { rt =>
                if ( isKnownAlgorithm( rt ) ) None
                else {
                  logger.info( "loaded algorithm root type for [{}]: [{}]", a.name, rt )
                  Some( (a, rt) )
                }
              }
            }
            .map { _.flatten }
          }
        } yield {
          val bc = unknowns.foldLeft( boundedContext ){ case (b, (_, rt)) => b :+ rt }
          //todo set new bounded context here if needed -- don't think it is per cell model.
          //todo if needed, would need to wrap actor's boundedcontext in agent to protect from concurrency issues

          unknowns
        }
      }

      unknownRoots match {
        case \/-(u) => Future successful u
        case -\/(ex) => Future failed ex
      }
    }
  }

  def registerWithRouter( algorithmRoots: List[(Symbol, AggregateRootType)] ): Future[Map[Symbol, AggregateRootType]] = {
    algorithmRootTypes alter { _ ++ algorithmRoots }
  }
}

class DetectionAlgorithmRouter extends Actor with EnvelopingActor with InstrumentedActor with ActorLogging {
  provider: DetectionAlgorithmRouter.Provider =>

  var routingTable: Map[Symbol, ActorRef] = provider.initialRoutingTable
  log.debug( "DetectionAlgorithmRouter[{}] created supporting algorithms:[{}]", routingTable.keys.mkString(",") )

  import DetectionAlgorithmRouter.{ RegisterDetectionAlgorithm, AlgorithmRegistered }

  val registration: Receive = {
    case RegisterDetectionAlgorithm( algorithm, handler ) => {
      routingTable += ( algorithm -> handler )
      sender() !+ AlgorithmRegistered( algorithm )
    }
  }

  val routing: Receive = {
    case m: DetectUsing if routingTable contains m.algorithm => routingTable( m.algorithm ) forwardEnvelope m
  }

  override val receive: Receive = LoggingReceive{ around( registration orElse routing ) }

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => log.error( s"cannot route unregistered algorithm [${m.algorithm}]" )
      case m => log.error( s"ROUTER UNAWARE OF message: [{}]\nrouting table:{}", m, routingTable.toSeq.mkString("\n",",","\n") ) // super.unhandled( m )
    }
  }
}
