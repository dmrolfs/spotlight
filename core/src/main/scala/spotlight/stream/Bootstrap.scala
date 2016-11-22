package spotlight.stream

import akka.actor.SupervisorStrategy.Decider

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.{Done, NotUsed}
import akka.actor.{ActorPath, ActorRef, ActorSystem, Props, SupervisorStrategy}
import akka.stream.scaladsl.{Flow, GraphDSL, Sink}
import akka.stream._
import akka.util.Timeout

import scalaz._
import Scalaz._
import scalaz.Kleisli.kleisli
import scalaz.concurrent.Task
import shapeless.{Generic, HNil}
import com.typesafe.config.Config
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.slf4j.LoggerFactory
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.metrics.{Instrumented, Reporter}
import peds.commons.builder.HasBuilder
import peds.commons.util._
import demesne.{AggregateRootType, BoundedContext, DomainModel, StartTask}
import peds.akka.supervision.{IsolatedDefaultSupervisor, IsolatedLifeCycleSupervisor, IsolatedStopSupervisor, OneForOneStrategyFactory}
import peds.akka.supervision.IsolatedLifeCycleSupervisor.{ChildStarted, StartChild}
import spotlight.analysis.outlier.{AnalysisPlanModule, DetectionAlgorithmRouter, PlanCatalog, PlanCatalogProxy}
import spotlight.analysis.outlier.algorithm.statistical.SimpleMovingAverageAlgorithm
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.TimeSeries


/**
  * Created by rolfsd on 11/7/16.
  */
case class BootstrapContext(
  name: String,
  rootTypes: Set[AggregateRootType],
  system: Option[ActorSystem],
  resources: Map[Symbol, Any],
  startTasks: Set[StartTask],
  timeout: Timeout
)

object BootstrapContext extends HasBuilder[BootstrapContext] {
  object Name extends Param[String]
  object RootTypes extends OptParam[Set[AggregateRootType]]( Set.empty[AggregateRootType] )
  object System extends OptParam[Option[ActorSystem]]( None )
  object Resources extends OptParam[Map[Symbol, Any]]( Map.empty[Symbol, Any] )
  object StartTasks extends OptParam[Set[StartTask]]( Set.empty[StartTask] )
  object Timeout extends OptParam[Timeout]( 30.seconds )

  // Establish HList <=> BootstrapContext isomorphism
  val gen = Generic[BootstrapContext]
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


object Bootstrap extends Instrumented with StrictLogging {
  override lazy val metricBaseName: MetricName = MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  lazy val workflowFailuresMeter: Meter = metrics meter "workflow.failures"

  type SystemSettings = ( ActorSystem, Settings )
  type BoundedSettings = ( BoundedContext, Settings )
  type SpotlightContext = ( BoundedContext, Settings, Flow[TimeSeries, Outliers, NotUsed] )


  def apply(
    context: BootstrapContext
  )(
    implicit ec: ExecutionContext
  ): Kleisli[Future, Array[String], SpotlightContext] = {
    systemConfiguration( context ) >=> startBoundedContext( context ) >=> makeFlow()
  }

  val systemRootTypes: Set[AggregateRootType] = {
    Set(
      AnalysisPlanModule.module.rootType,
      SimpleMovingAverageAlgorithm.rootType
    )
  }

  def systemStartTasks( settings: Settings )( implicit ec: ExecutionContext ): Set[StartTask] = {
    Set(
      DetectionAlgorithmRouter.startTask( settings.config ),
      metricsReporterStartTask( settings.config )
    )
  }

  def metricsReporterStartTask( config: Config ): StartTask = StartTask.withUnitTask( "start metrics reporter" ){
    val MetricsPath = "spotlight.metrics"

    Task {
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
  }

  val kamonStartTask: StartTask = StartTask.withUnitTask( "start Kamon monitoring" ){ Task { kamon.Kamon.start(); Done } }


  def systemConfiguration( context: BootstrapContext ): Kleisli[Future, Array[String], SystemSettings] = {
    kleisli[Future, Array[String], SystemSettings] { args =>
      def spotlightConfig: String = Option( System getProperty "spotlight.config" ) getOrElse { "application.conf" }

      logger.info(
        "spotlight.config: [{}] @ URL:[{}]",
        spotlightConfig,
        scala.util.Try { Thread.currentThread.getContextClassLoader.getResource(spotlightConfig) }
      )

      Settings( args, config = com.typesafe.config.ConfigFactory.load() ).disjunction map { settings =>
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

  def startBoundedContext( context: BootstrapContext ): Kleisli[Future, SystemSettings, BoundedSettings] = {
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


  def makeCatalog( settings: Settings )( implicit bc: BoundedContext ): ActorRef = {
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
      maxInFlightCpuFactor = settings.maxInDetectionCpuFactor, //todo different yet same
      applicationDetectionBudget = Some( settings.detectionBudget ),
      applicationPlans = settings.plans
    )

    import akka.pattern.ask
    implicit val timeout = Timeout( 30.seconds )
    val catalogChild = scala.concurrent.Await.result(
      ( catalogSupervisor ? StartChild(catalogProps, PlanCatalog.name) ).mapTo[ChildStarted],
      timeout.duration
    )
    catalogChild.child
  }

  def makeFlow(): Kleisli[Future, BoundedSettings, SpotlightContext] = {
    kleisli[Future, BoundedSettings, SpotlightContext] { case (boundedContext, settings) =>
      implicit val bc = boundedContext
      implicit val system = boundedContext.system
      implicit val dispatcher = system.dispatcher
      implicit val timeout = Timeout( 5.seconds )
      implicit val materializer = ActorMaterializer(
        ActorMaterializerSettings( system ) withSupervisionStrategy supervisionDecider
      )

      logger.info( "TEST:BOOTSTRAP:BEFORE BoundedContext roottypes = [{}]", boundedContext.unsafeModel.rootTypes )

//        val catalogRef = boundedContext.resources( Symbol(PlanCatalog.name) ).asInstanceOf[ActorRef]
      val catalogRef = makeCatalog( settings )
      val catalogProxyProps = PlanCatalogProxy.props(
        underlying = catalogRef,
        configuration = settings.config,
        maxInFlightCpuFactor = settings.maxInDetectionCpuFactor,
        applicationDetectionBudget = Some(settings.detectionBudget)
      )

      Future successful { ( boundedContext, settings, detectionModel(catalogProxyProps, settings) ) }
    }
  }

  def detectionModel(
    catalogProxyProps: Props,
    settings: Settings
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[TimeSeries, Outliers, NotUsed] = {
    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      //todo add support to watch FlowShapes

      val scoring = b.add( OutlierScoringModel.scoringGraph( catalogProxyProps, settings) )
      val logUnrecognized = b.add(
        OutlierScoringModel.logMetric( Logger( LoggerFactory getLogger "Unrecognized" ), settings.plans )
      )

      val termUnrecognized = b.add( Sink.ignore )

      scoring.in
      scoring.out1 ~> logUnrecognized ~> termUnrecognized

      FlowShape( scoring.in, scoring.out0 )
    }

    Flow.fromGraph( graph ).withAttributes( ActorAttributes supervisionStrategy supervisionDecider )
  }

  val supervisionDecider: Supervision.Decider = {
    case ex => {
      logger.error( "Error caught by Supervisor:", ex )
      workflowFailuresMeter.mark( )
      Supervision.Restart
    }
  }
}
