package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
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
import spotlight.analysis.outlier.algorithm.{AlgorithmModule, AlgorithmProtocol, InsufficientAlgorithmModuleError}
import spotlight.analysis.outlier.algorithm.statistical._
import spotlight.model.outlier.OutlierPlan
import spotlight.model.outlier.OutlierPlan.Scope



/**
 * Created by rolfsd on 9/29/15.
 */
object DetectionAlgorithmRouter extends LazyLogging {
  private val trace = Trace[DetectionAlgorithmRouter.type]


  sealed trait RouterProtocol
  case class RegisterAlgorithmReference( algorithm: Symbol, handler: ActorRef ) extends RouterProtocol
  case class RegisterAlgorithmRootType( algorithm: Symbol, handler: AggregateRootType, model: DomainModel ) extends RouterProtocol
  case class AlgorithmRegistered( algorithm: Symbol ) extends RouterProtocol

  val ContextKey = 'DetectionAlgorithmRouter

  def startTask( configuration: Config )( implicit ec: ExecutionContext ): StartTask = {
    StartTask.withBoundTask( "load user algorithms" ){ bc: BoundedContext =>
      val t: Future[StartTask.Result] = {
        for {
          algoRoots <- loadAlgorithms( bc, configuration )
          roots <- registerWithRouter( algoRoots )
          unknowns = algoRoots.map{ case (_, rt) => rt }.toSet
          _ = logger.info( "TEST: DetectionAlgorithmRouter: unknowns:[{}]", unknowns )
          _ = logger.info( "TEST: EC = [{}]", implicitly[ExecutionContext] )
        } yield {
          logger.info( "DetectionAlgorithmRouter master table: [{}]", roots.mkString("\n", ", \n", "\n") )
          StartTask.Result( rootTypes = unknowns )
        }
      }

      t.toTask
    }
  }


  def props( routingTable: Map[Symbol, AlgorithmResolver] ): Props = Props( new Default( routingTable ) )
  val DispatcherPath: String = OutlierDetection.DispatcherPath

  def name( suffix: String ): String = "DetectionRouter-" + suffix


  trait Provider {
    def initialRoutingTable: Map[Symbol, AlgorithmResolver]
  }

  private class Default( override val initialRoutingTable: Map[Symbol, AlgorithmResolver] )
  extends DetectionAlgorithmRouter with Provider


  def unsafeRootTypeFor( algorithm: Symbol ): Option[AggregateRootType] = {
    logger.debug( "DetectionAlgorithmRouter unsafe algorithmRootTypes:[{}]", algorithmRootTypes.get.keySet )
    algorithmRootTypes.get() get algorithm
  }

  def futureRootTypeFor( algorithm: Symbol )( implicit ec: ExecutionContext ): Future[Option[AggregateRootType]] = {
    algorithmRootTypes.future() map { rootTable =>
      logger.debug( "DetectionAlgorithmRouter safe algorithmRootTypes:[{}]", rootTable.keySet )
      rootTable get algorithm
    }
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
    def algorithmRootTypeFor( clazz: Class[_ <: AlgorithmModule] ): TryV[AggregateRootType] = {
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

    def collectUnknowns(
      algoClasses: Map[Symbol, Class[_ <: AlgorithmModule]]
    )(
      implicit model: DomainModel
    ): TryV[List[(Symbol, AggregateRootType)]] = {
      algoClasses
      .toList
      .traverseU { case (a, c) =>
        algorithmRootTypeFor( c ) map { rt =>
          if ( isKnownAlgorithm(rt) ) None
          else {
            logger.info( "loaded algorithm root type for [{}]: [{}]", a.name, rt )
            Some( (a, rt) )
          }
        }
      }
      .map { _.flatten }
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
        case -\/(ex) => Future failed ex
      }
    }
  }

  def registerWithRouter( algorithmRoots: List[(Symbol, AggregateRootType)] ): Future[Map[Symbol, AggregateRootType]] = {
    algorithmRootTypes alter { _ ++ algorithmRoots }
  }


  sealed abstract class AlgorithmResolver {
    def referenceFor( scope: OutlierPlan.Scope ): ActorRef
  }

  case class DirectResolver( reference: ActorRef ) extends AlgorithmResolver {
    override def referenceFor( scope: Scope ): ActorRef = reference
  }

  case class RootTypeResolver( rootType: AggregateRootType, model: DomainModel ) extends AlgorithmResolver {
    override def referenceFor( scope: Scope ): ActorRef = model( rootType, scope )
  }
}

class DetectionAlgorithmRouter extends Actor with EnvelopingActor with InstrumentedActor with ActorLogging {
  provider: DetectionAlgorithmRouter.Provider =>

  import DetectionAlgorithmRouter._

  var routingTable: Map[Symbol, AlgorithmResolver] = provider.initialRoutingTable
  def addRoute( algorithm: Symbol, resolver: AlgorithmResolver ): Unit = { routingTable += ( algorithm -> resolver ) }
  log.debug( "DetectionAlgorithmRouter[{}] created with routing keys:[{}]", self.path.name, routingTable.keys.mkString(",") )

  def contains( algorithm: Symbol ): Boolean = {
    log.debug( "DetectionAlgorithmRouter[{}] looking for {} in algorithms:[{}]", self.path.name, algorithm, routingTable.keys.mkString(",") )
    routingTable contains algorithm
  }

  val registration: Receive = {
//    case class RegisterAlgorithmRootType( algorithm: Symbol, handler: AggregateRootType ) extends RouterProtocol
    case RegisterAlgorithmReference( algorithm, handler ) => {
      addRoute( algorithm, DirectResolver(handler) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, handler, model ) => {
      addRoute( algorithm, RootTypeResolver(handler, model) )
      sender() !+ AlgorithmRegistered( algorithm )
    }
  }

  val routing: Receive = {
    case m: DetectUsing if contains( m.algorithm ) => routingTable( m.algorithm ).referenceFor( m.scope ) forwardEnvelope m
  }

  override val receive: Receive = LoggingReceive{ around( registration orElse routing ) }

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => {
        log.error(
          "cannot route unregistered algorithm [{}] routing-table:[{}]",
          m.algorithm,
          routingTable.mkString("\n", "\n", "\n")
        )
      }

      case m => {
        log.error( "ROUTER UNAWARE OF message: [{}]\nrouting table:{}", m, routingTable.toSeq.mkString("\n",",","\n") )
        // super.unhandled( m )
      }
    }
  }
}
