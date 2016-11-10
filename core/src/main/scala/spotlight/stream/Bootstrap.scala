package spotlight.stream

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, Props}
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
import demesne.{AggregateRootType, BoundedContext, StartTask}
import spotlight.analysis.outlier.{AnalysisPlanModule, DetectionAlgorithmRouter, PlanCatalog}
import spotlight.analysis.outlier.algorithm.statistical.SimpleMovingAverageAlgorithm
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.TimeSeries


/**
  * Created by rolfsd on 11/7/16.
  */
case class BootstrapContext(
  name: String,
  rootTypes: Set[AggregateRootType],
  resources: Map[Symbol, Any],
  startTasks: Set[StartTask],
  timeout: Timeout
)

object BootstrapContext extends HasBuilder[BootstrapContext] {
  object Name extends Param[String]
  object RootTypes extends OptParam[Set[AggregateRootType]]( Set.empty[AggregateRootType] )
  object Resources extends OptParam[Map[Symbol, Any]]( Map.empty[Symbol, Any] )
  object StartTasks extends OptParam[Set[StartTask]]( Set.empty[StartTask] )
  object Timeout extends OptParam[Timeout]( 30.seconds )

  // Establish HList <=> BootstrapContext isomorphism
  val gen = Generic[BootstrapContext]
  // Establish Param[_] <=> constructor parameter correspondence
  override val fieldsContainer = createFieldsContainer( Name :: RootTypes :: Resources :: StartTasks :: Timeout :: HNil )
}


object Bootstrap extends Instrumented with StrictLogging {
  override lazy val metricBaseName: MetricName = MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  lazy val workflowFailuresMeter: Meter = metrics meter "workflow.failures"

  type SystemConfiguration = ( ActorSystem, Configuration )
  type BoundedConfiguration = ( BoundedContext, Configuration )
  type SpotlightContext = ( BoundedContext, Configuration, Flow[TimeSeries, Outliers, NotUsed] )


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

  def systemStartTasks( configuration: Configuration )( implicit ec: ExecutionContext ): Set[StartTask] = {
    Set(
      DetectionAlgorithmRouter.startTask( configuration ),
      metricsReporterStartTask( configuration )
    )
  }

  def metricsReporterStartTask( config: Config ): StartTask = StartTask.withUnitTask( "start metrics reporter" ){
    Task {
      if ( config hasPath "spotlight.metrics" ) {
        val metricsConfig = config getConfig "spotlight.metrics"
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


  def systemConfiguration( context: BootstrapContext ): Kleisli[Future, Array[String], SystemConfiguration] = {
    kleisli[Future, Array[String], SystemConfiguration] { args =>
      Configuration( args ).disjunction map { config =>
        logger info config.usage
        ( ActorSystem(context.name, config), config )
      } match {
        case \/-( systemConfiguration ) => Future successful systemConfiguration
        case -\/( exs ) => {
          exs foreach { ex => logger.error( "failed to create system configuration", ex ) }
          Future failed exs.head
        }
      }
    }
  }

  def startBoundedContext( context: BootstrapContext ): Kleisli[Future, SystemConfiguration, BoundedConfiguration] = {
    kleisli[Future, SystemConfiguration, BoundedConfiguration] { case (system, config) =>
      implicit val sys = system
      implicit val ec = system.dispatcher
      implicit val timeout = context.timeout

      val makeBoundedContext = {
        BoundedContext.make(
          key = Symbol(context.name),
          configuration = config,
          rootTypes = context.rootTypes ++ systemRootTypes,
          userResources = context.resources,
          startTasks = context.startTasks ++ systemStartTasks(config)
        )
      }

      for {
        made <- makeBoundedContext
        started <- made.start()
      } yield ( started, config )
    }
  }

  def makeFlow(): Kleisli[Future, BoundedConfiguration, SpotlightContext] = {
    kleisli[Future, BoundedConfiguration, SpotlightContext] { case (boundedContext, configuration) =>
      implicit val bc = boundedContext
      implicit val system = boundedContext.system
      implicit val materializer = ActorMaterializer(
        ActorMaterializerSettings( system ) withSupervisionStrategy supervisionDecider
      )

      val catalogProps = PlanCatalog.props(
        configuration = configuration,
        maxInFlightCpuFactor = configuration.maxInDetectionCpuFactor,
        applicationDetectionBudget = Some(configuration.detectionBudget)
      )

      Future successful ( boundedContext, configuration, detectionModel(catalogProps, configuration) )
    }
  }

  def detectionModel(
    catalogProps: Props,
    conf: Configuration
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[TimeSeries, Outliers, NotUsed] = {
    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      //todo add support to watch FlowShapes

      val scoring = b.add( OutlierScoringModel.scoringGraph( catalogProps, conf) )
      val logUnrecognized = b.add(
        OutlierScoringModel.logMetric( Logger( LoggerFactory getLogger "Unrecognized" ), conf.plans )
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