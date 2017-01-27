package spotlight.analysis

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect._
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
import peds.commons.concurrent._
import peds.commons.identifier.{Identifying, ShortUUID, TaggedID}
import spotlight.analysis.algorithm._
import spotlight.analysis.shard._
import spotlight.analysis.algorithm.statistical._
import spotlight.model.outlier.OutlierPlan


/**
 * Created by rolfsd on 9/29/15.
 */
object DetectionAlgorithmRouter extends LazyLogging {
  sealed trait RouterProtocol

  case class RegisterAlgorithmReference( algorithm: Symbol, handler: ActorRef ) extends RouterProtocol

  case class RegisterAlgorithmRootType(
    algorithm: Symbol,
    algorithmRootType: AggregateRootType,
    model: DomainModel,
    sharded: Boolean = true
  ) extends RouterProtocol

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


  def props( plan: OutlierPlan.Summary, routingTable: Map[Symbol, AlgorithmProxy] ): Props = {
    Props( new Default( plan, routingTable ) )
  }

  val DispatcherPath: String = OutlierDetection.DispatcherPath

  def name( suffix: String ): String = "DetectionRouter-" + suffix


  trait Provider {
    def initialRoutingTable: Map[Symbol, AlgorithmProxy]
    def plan: OutlierPlan.Summary
  }

  private class Default(
    override val plan: OutlierPlan.Summary,
    override val initialRoutingTable: Map[Symbol, AlgorithmProxy]
  ) extends DetectionAlgorithmRouter with Provider

  def rootTypeFor( algorithm: Symbol )( implicit ec: ExecutionContext ): Option[AggregateRootType] = {
    import scala.concurrent.duration._
    unsafeRootTypeFor( algorithm ) orElse { scala.concurrent.Await.result( futureRootTypeFor(algorithm), 30.seconds ) }
  }

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


  def userAlgorithms( configuration: Config ): Valid[Map[Symbol, Class[_ <: AlgorithmModule]]] = {
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


  object AlgorithmProxy {
    def proxyFor(
      plan: OutlierPlan.Summary,
      algorithmRootType: AggregateRootType
    )(
      implicit model: DomainModel
    ): AlgorithmProxy = {
      val r = ShardingStrategy
      .from( plan )
      .map { strategy => ShardedRootTypeProxy( plan, algorithmRootType, strategy, model ) }
      .getOrElse { RootTypeProxy( algorithmRootType, model ) }

      logger.warn( "#TEST AlgorithmProxy.fromProxy( plan:[{}] algorithm:[{}] ) = [{}]", plan.name, algorithmRootType.name, r )
      r
    }
  }

  sealed abstract class AlgorithmProxy {
    def forward( message: Any )( implicit sender: ActorRef, context: ActorContext ): Unit = {
      referenceFor( message ) forwardEnvelope message
    }

    def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef
  }

  case class DirectProxy( reference: ActorRef ) extends AlgorithmProxy {
    override def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef = reference
  }

  case class RootTypeProxy( algorithmRootType: AggregateRootType, model: DomainModel ) extends AlgorithmProxy {
    val TidType: ClassTag[TaggedID[ShortUUID]] = classTag[TaggedID[ShortUUID]]

    override def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef = {
      message match {
        case m: OutlierDetectionMessage => model( algorithmRootType, m.plan.id )
        case id: ShortUUID => model( algorithmRootType, id )
        case TidType(tid) => model( algorithmRootType, tid )
        case _ => model.system.deadLetters
      }
    }
  }

  case class ShardedRootTypeProxy(
    plan: OutlierPlan.Summary,
    algorithmRootType: AggregateRootType,
    strategy: ShardingStrategy,
    implicit val model: DomainModel
  ) extends AlgorithmProxy {
    implicit val scIdentifying: Identifying[ShardCatalog.ID] = strategy.identifying
    val shardingId: ShardCatalog#TID = strategy.idFor( plan, algorithmRootType.name )
    val shardingRef = strategy.actorFor( plan, algorithmRootType )( model )

    override def forward( message: Any )( implicit sender: ActorRef, context: ActorContext ): Unit = {
      referenceFor(message) forwardEnvelope ShardProtocol.RouteMessage( shardingId, message )
    }

    override def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef = shardingRef
  }
}

class DetectionAlgorithmRouter extends Actor with EnvelopingActor with InstrumentedActor with ActorLogging {
  provider: DetectionAlgorithmRouter.Provider =>

  import DetectionAlgorithmRouter._

  var routingTable: Map[Symbol, AlgorithmProxy] = provider.initialRoutingTable
  def addRoute( algorithm: Symbol, resolver: AlgorithmProxy ): Unit = { routingTable += ( algorithm -> resolver ) }
  log.debug( "DetectionAlgorithmRouter[{}] created with routing-table:[{}]", self.path.name, routingTable.mkString("\n", ",", "\n") )

  def contains( algorithm: Symbol ): Boolean = {
    log.debug( "DetectionAlgorithmRouter[{}] looking for {} in algorithms:[{}]", self.path.name, algorithm, routingTable.keys.mkString(",") )
    routingTable contains algorithm
  }

  val registration: Receive = {
//    case class RegisterAlgorithmRootType( algorithm: Symbol, handler: AggregateRootType ) extends RouterProtocol
    case RegisterAlgorithmReference( algorithm, handler ) => {
      addRoute( algorithm, DirectProxy( handler ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, algorithmRootType, model, true ) => {
      log.debug( "#TEST received RegisterAlgorithmRootType for algo:[{}] rt:[{}], model:[{}]", algorithm, algorithmRootType, model )
      addRoute( algorithm, AlgorithmProxy.proxyFor(plan, algorithmRootType)( model ) )
      log.debug( "#TEST route added... notifying sender:[{}]", sender() )
//      addRoute( algorithm, ShardedRootTypeProxy( plan, algorithmRootType, model ) )
      sender() !+ AlgorithmRegistered( algorithm )
    }

    case RegisterAlgorithmRootType( algorithm, algorithmRootType, model, _ ) => {
      addRoute( algorithm, RootTypeProxy( algorithmRootType, model ) )
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
