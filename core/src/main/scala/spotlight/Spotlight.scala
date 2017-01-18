package spotlight

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props, SupervisorStrategy}
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Sink}
import akka.util.Timeout

import scalaz.Kleisli.kleisli
import scalaz.Scalaz._
import scalaz._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import demesne.{AggregateRootType, BoundedContext, DomainModel, StartTask}
import nl.grons.metrics.scala.{Meter, MetricName}
import org.slf4j.LoggerFactory
import peds.akka.metrics.{Instrumented, Reporter}
import peds.akka.supervision.IsolatedLifeCycleSupervisor.{ChildStarted, StartChild}
import peds.akka.supervision.{IsolatedLifeCycleSupervisor, OneForOneStrategyFactory}
import peds.commons.builder.HasBuilder
import peds.commons.util._
import shapeless.{Generic, HNil}
import spotlight.analysis._
import spotlight.analysis.{PlanCatalogProtocol => CP}
import spotlight.analysis.algorithm._
import spotlight.analysis.algorithm.statistical._


/**
  * Created by rolfsd on 11/7/16.
  */
final case class SpotlightContext(
  name: String,
  rootTypes: Set[AggregateRootType],
  system: Option[ActorSystem],
  resources: Map[Symbol, Any],
  startTasks: Set[StartTask],
  timeout: Timeout
)

object SpotlightContext extends HasBuilder[SpotlightContext] {
  object Name extends Param[String]
  object RootTypes extends OptParam[Set[AggregateRootType]]( Set.empty[AggregateRootType] )
  object System extends OptParam[Option[ActorSystem]]( None )
  object Resources extends OptParam[Map[Symbol, Any]]( Map.empty[Symbol, Any] )
  object StartTasks extends OptParam[Set[StartTask]]( Set.empty[StartTask] )
  object Timeout extends OptParam[Timeout]( 30.seconds )

  // Establish HList <=> SpotlightContext isomorphism
  val gen = Generic[SpotlightContext]
  // Establish Param[_] <=> constructor parameter correspondence
  override val fieldsContainer = createFieldsContainer(
    Name ::
    RootTypes ::
    System ::
    Resources ::
    StartTasks ::
    Timeout ::
    HNil
  )
}


object Spotlight extends Instrumented with StrictLogging {
  override lazy val metricBaseName: MetricName = MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  lazy val workflowFailuresMeter: Meter = metrics meter "workflow.failures"

  type SystemSettings = ( ActorSystem, Settings )
  type BoundedSettings = ( BoundedContext, Settings )
  type SpotlightModel = ( BoundedContext, Settings, DetectFlow )


  def apply(
    context: SpotlightContext,
    finishSubscriberOnComplete: Boolean = true
  )(
    implicit ec: ExecutionContext
  ): Kleisli[Future, Array[String], SpotlightModel] = {
    systemConfiguration( context ) >=> startBoundedContext( context ) >=> makeFlow( finishSubscriberOnComplete )
  }

  val systemRootTypes: Set[AggregateRootType] = {
    Set(
      AnalysisPlanModule.module.rootType,
      AlgorithmLookupShardCatalogModule.rootType,
      AlgorithmCellShardModule.module.rootType,
      SimpleMovingAverageAlgorithm.rootType,
      GrubbsAlgorithm.rootType,
      ExponentialMovingAverageAlgorithm.rootType
    )
  }

  def systemStartTasks( settings: Settings )( implicit ec: ExecutionContext ): Set[StartTask] = {
    Set(
      DetectionAlgorithmRouter.startTask( settings.config ),
      metricsReporterStartTask( settings.config )
    )
  }

  def metricsReporterStartTask( config: Config ): StartTask = StartTask.withFunction( "start metrics reporter" ){ bc =>
    val MetricsPath = "spotlight.metrics"

    if ( config hasPath MetricsPath ) {
      val metricsConfig = config getConfig MetricsPath
      logger.info( "starting metric reporting with config: [{}]", metricsConfig )
      val reporter = Reporter startReporter metricsConfig
      logger.info( "metric reporter: [{}]", reporter )
    } else {
      logger.warn( """metric report configuration missing at "spotlight.metrics"""" )
    }

    Done
  }

  val kamonStartTask: StartTask = StartTask.withFunction( "start Kamon monitoring" ){ bc => kamon.Kamon.start(); Done }


  def systemConfiguration( context: SpotlightContext ): Kleisli[Future, Array[String], SystemSettings] = {
    kleisli[Future, Array[String], SystemSettings] { args =>
      def spotlightConfig: String = Option( System getProperty "spotlight.config" ) getOrElse { "application.conf" }

      logger.info(
        "spotlight.config: [{}] @ URL:[{}]",
        spotlightConfig,
        scala.util.Try { Thread.currentThread.getContextClassLoader.getResource(spotlightConfig) }
      )

      Settings( args, config = ConfigFactory.load() ).disjunction map { settings =>
        logger info settings.usage
        val system = context.system getOrElse ActorSystem( context.name, settings.config )
        ( system, settings )
      } match {
        case \/-( systemConfiguration ) => Future successful systemConfiguration
        case -\/( exs ) => {
          exs foreach { ex => logger.error( "failed to create system configuration", ex ) }
          Future failed exs.head
        }
      }
    }
  }

  def startBoundedContext( context: SpotlightContext ): Kleisli[Future, SystemSettings, BoundedSettings] = {
    kleisli[Future, SystemSettings, BoundedSettings] { case (system, settings) =>
      implicit val sys = system
      implicit val ec = system.dispatcher
      implicit val timeout = context.timeout

      val makeBoundedContext = {
        BoundedContext.make(
          key = Symbol(context.name),
          configuration = settings.toConfig,
          rootTypes = context.rootTypes ++ systemRootTypes,
          userResources = context.resources,
          startTasks = context.startTasks ++ systemStartTasks(settings)
        )
      }

      for {
        made <- makeBoundedContext
        started <- made.start()
      } yield ( started, settings )
    }
  }


  def makeFlow( finishSubscriberOnComplete: Boolean ): Kleisli[Future, BoundedSettings, SpotlightModel] = {
    kleisli[Future, BoundedSettings, SpotlightModel] { case (boundedContext, settings) =>
      implicit val bc = boundedContext
      implicit val system = boundedContext.system
      implicit val dispatcher = system.dispatcher
      implicit val detectionTimeout = Timeout( 2.minutes ) //todo: define in Settings
      implicit val materializer = ActorMaterializer(
        ActorMaterializerSettings( system ) withSupervisionStrategy supervisionDecider
      )

      logger.info( "TEST:BOOTSTRAP:BEFORE BoundedContext roottypes = [{}]", boundedContext.unsafeModel.rootTypes )

      for {
        catalog <- makeCatalog( settings )
        catalogFlow <- PlanCatalog.flow( catalog, settings.parallelism )
      } yield ( boundedContext, settings, detectFlowFrom(catalogFlow, settings) )
    }
  }

  def makeCatalog( settings: Settings )( implicit bc: BoundedContext ): Future[ActorRef] = {
    val catalogSupervisor = bc.system.actorOf(
      Props(
        new IsolatedLifeCycleSupervisor with OneForOneStrategyFactory {
          override def childStarter(): Unit = { }
          override val supervisorStrategy: SupervisorStrategy = makeStrategy( 3, 1.minute ) {
            case _: DomainModel.NoIndexForAggregateError => SupervisorStrategy.Stop
            case _: akka.actor.ActorInitializationException => SupervisorStrategy.Stop
            case _: akka.actor.ActorKilledException => SupervisorStrategy.Stop
            case _: Exception => SupervisorStrategy.Stop
            case _ => SupervisorStrategy.Escalate
          }
        }
      ),
      "CatalogSupervisor"
    )

    val catalogProps = PlanCatalog.props(
      configuration = settings.toConfig,
      maxInFlightCpuFactor = settings.parallelismFactor, //todo different yet same - refactor to be parallelsimFactor
      applicationDetectionBudget = Some( settings.detectionBudget ),
      applicationPlans = settings.plans
    )

    import akka.pattern.ask
    implicit val ec = bc.system.dispatcher
    implicit val timeout = Timeout( 30.seconds )

    for {
      ChildStarted( catalog ) <- ( catalogSupervisor ? StartChild(catalogProps, PlanCatalog.name) ).mapTo[ChildStarted]
      _ = logger.info( "catalog initialization started: [{}]", catalog )
      _ <- ( catalog ? CP.WaitForStart ).mapTo[CP.Started.type]
    } yield {
      logger.debug( "catalog initialization completed" )
      catalog
    }
  }

  def detectFlowFrom(
    catalogFlow: DetectFlow,
    settings: Settings
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): DetectFlow = {
    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      //todo add support to watch FlowShapes
      val scoring = b.add( OutlierScoringModel.scoringGraph( catalogFlow, settings) )
      val logUnrecognized = b.add(
        OutlierScoringModel.logMetric( Logger( LoggerFactory getLogger "Unrecognized" ), settings.plans )
      )

      val termUnrecognized = b.add( Sink.ignore )

      scoring.in
      scoring.out1 ~> logUnrecognized ~> termUnrecognized

      FlowShape( scoring.in, scoring.out0 )
    }

    Flow
    .fromGraph( graph )
    .named( "DetectionFlow" )
    .withAttributes( ActorAttributes supervisionStrategy supervisionDecider )
  }


  val supervisionDecider: Supervision.Decider = {
    case ex => {
      logger.error( "Error caught by Supervisor:", ex )
      workflowFailuresMeter.mark( )
      Supervision.Restart
    }
  }
}
