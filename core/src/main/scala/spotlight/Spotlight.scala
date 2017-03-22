package spotlight

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import akka.Done
import akka.actor.{ ActorRef, ActorSystem, Props, SupervisorStrategy }
import akka.stream._
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source }
import akka.util.Timeout

import scalaz.Kleisli.kleisli
import scalaz.Scalaz._
import scalaz.{ Source ⇒ _, _ }
import shapeless.{ Generic, HNil }
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._
import com.persist.logging._
import nl.grons.metrics.scala.{ Meter, MetricName }
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import omnibus.akka.metrics.{ Instrumented, Reporter }
import omnibus.akka.supervision.IsolatedLifeCycleSupervisor.{ ChildStarted, StartChild }
import omnibus.akka.supervision.{ IsolatedLifeCycleSupervisor, OneForOneStrategyFactory }
import omnibus.commons.builder.HasBuilder
import omnibus.commons.util._
import demesne.{ AggregateRootType, BoundedContext, DomainModel, StartTask }
import omnibus.akka.stream.{ StreamEgress, StreamIngress }
import spotlight.analysis._
import spotlight.analysis.{ PlanCatalogProtocol ⇒ CP }
import spotlight.analysis.shard.{ CellShardModule, LookupShardModule }
import spotlight.analysis.algorithm.statistical._
import spotlight.infrastructure.ClusterRole
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.TimeSeries

/** Created by rolfsd on 11/7/16.
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
  val Key: Symbol = 'Spotlight

  object Name extends OptParam[String]( Key.name )
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

object Spotlight extends Instrumented with ClassLogging {
  override lazy val metricBaseName: MetricName = MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  lazy val workflowFailuresMeter: Meter = metrics meter "workflow.failures"

  val SystemCategory = "system"

  type SystemSettings = ( ActorSystem, Settings )
  type BoundedSettings = ( BoundedContext, Settings )
  type SpotlightModel = ( BoundedContext, Settings, Option[DetectFlow] )

  def apply(
    context: SpotlightContext,
    finishSubscriberOnComplete: Boolean = true
  )(
    implicit
    ec: ExecutionContext
  ): Kleisli[Future, Array[String], SpotlightModel] = {
    log.alternative(
      SystemCategory,
      Map(
        "@msg" → "build info",
        "spotlight" → Map(
          "name" → spotlight.BuildInfo.name,
          "version" → spotlight.BuildInfo.version,
          "scala-version" → spotlight.BuildInfo.scalaVersion,
          "sbt-version" → spotlight.BuildInfo.sbtVersion
        ),
        "demesne" → Map(
          "name" → demesne.BuildInfo.name,
          "version" → demesne.BuildInfo.version,
          "scala-version" → demesne.BuildInfo.scalaVersion,
          "sbt-version" → demesne.BuildInfo.sbtVersion
        )
      )
    )

    systemConfiguration( context ) >=> startBoundedContext( context ) >=> makeFlow( finishSubscriberOnComplete )
  }

  def startActorSystemForRole( name: String, config: Config, role: ClusterRole, port: Option[Int] = None ): ActorSystem = {
    val clusterConditioned = Settings.clusterConditionConfiguration( config, role, port, Option( name ) )
    val seeds = Settings.seedNodesFrom( config ) filter { _.systemName == name }
    ActorSystem( name, clusterConditioned )
  }

  val systemRootTypes: Set[AggregateRootType] = {
    Set(
      AnalysisPlanModule.module.rootType,
      LookupShardModule.rootType,
      CellShardModule.module.rootType,
      SimpleMovingAverageAlgorithm.module.rootType,
      GrubbsAlgorithm.module.rootType,
      ExponentialMovingAverageAlgorithm.module.rootType
    )
  }

  def systemStartTasks( settings: Settings )( implicit ec: ExecutionContext ): Set[StartTask] = {
    Set(
      DetectionAlgorithmRouter.startTask( settings.config ),
      metricsReporterStartTask( settings.config )
    )
  }

  def metricsReporterStartTask( config: Config ): StartTask = StartTask.withFunction( "start metrics reporter" ) { bc ⇒
    val MetricsPath = "spotlight.metrics"

    config
      .as[Option[Config]]( MetricsPath )
      .map { metricsConfig ⇒
        val reporter = Reporter startReporter metricsConfig
        log.alternative(
          SystemCategory,
          Map( "@msg" → "starting metric reporting", "config" → metricsConfig, "reporter" → reporter.toString )
        )
      }
      .getOrElse {
        log.alternative( SystemCategory, Map( "@msg" → """metric report configuration missing at "spotlight.metrics"""" ) )
        log.warn( """metric report configuration missing at "spotlight.metrics"""" )
      }

    Done
  }

  def systemConfiguration( context: SpotlightContext ): Kleisli[Future, Array[String], SystemSettings] = {
    kleisli[Future, Array[String], SystemSettings] { args ⇒
      val spotlightConfig: String = {
        Option( System.getProperty( "config.resource" ) )
          .orElse { Option( System.getProperty( "config.file" ) ) }
          .orElse { Option( System.getProperty( "config.url" ) ) }
          .getOrElse { "application.conf" }
      }

      log.alternative(
        SystemCategory,
        Map(
          "@msg" → "spotlight config",
          "config" → spotlightConfig.toString,
          "url" → scala.util.Try { Thread.currentThread.getContextClassLoader.getResource( spotlightConfig ) }
        )
      )

      Settings( args, config = ConfigFactory.load() ).disjunction map { settings ⇒
        log.alternative( SystemCategory, Map( "@msg" → "Settings", "usage" → settings.usage._2 ) )
        val system = context.system getOrElse startActorSystemForRole( context.name, settings.config, settings.role ) // ActorSystem( context.name, settings.config ) CHANGE !!!
        ( system, settings )
      } match {
        case \/-( systemConfiguration ) ⇒ Future successful systemConfiguration
        case -\/( exs ) ⇒ {
          exs foreach { ex ⇒ log.error( "failed to create system configuration", ex ) }
          Future failed exs.head
        }
      }
    }
  }

  def startBoundedContext( context: SpotlightContext ): Kleisli[Future, SystemSettings, BoundedSettings] = {
    kleisli[Future, SystemSettings, BoundedSettings] {
      case ( system, settings ) ⇒
        implicit val sys = system
        implicit val ec = system.dispatcher
        implicit val timeout = context.timeout

        val makeBoundedContext = {
          BoundedContext.make(
            key = SpotlightContext.Key,
            //            key = Symbol( context.name ),
            configuration = settings.toConfig,
            rootTypes = context.rootTypes ++ systemRootTypes,
            userResources = context.resources,
            startTasks = context.startTasks ++ systemStartTasks( settings )
          )
        }

        for {
          made ← makeBoundedContext
          started ← made.start()
        } yield ( started, settings )
    }
  }

  def makeFlow( finishSubscriberOnComplete: Boolean ): Kleisli[Future, BoundedSettings, SpotlightModel] = {
    kleisli[Future, BoundedSettings, SpotlightModel] {
      case ( boundedContext, settings ) if settings.role.hostsFlow ⇒ {
        implicit val bc = boundedContext
        implicit val system = boundedContext.system
        implicit val dispatcher = system.dispatcher
        implicit val detectionTimeout = Timeout( 5.minutes ) //todo: define in Settings
        implicit val materializer = ActorMaterializer(
          ActorMaterializerSettings( system ) withSupervisionStrategy supervisionDecider
        )

        for {
          catalog ← makeCatalog( settings )
          catalogFlow ← PlanCatalog.flow( catalog, settings.parallelism )
        } yield {
          val detectFlow = detectFlowFrom( catalogFlow, settings )
          val clusteredFlow = clusterFlowFrom( detectFlow, settings )
          ( boundedContext, settings, Option( clusteredFlow ) )
        }
      }

      case ( boundedContext, settings ) ⇒ Future successful ( boundedContext, settings, None )
    }
  }

  def makeCatalog( settings: Settings )( implicit bc: BoundedContext ): Future[ActorRef] = {
    val catalogSupervisor = bc.system.actorOf(
      Props(
        new IsolatedLifeCycleSupervisor with OneForOneStrategyFactory {
          override def childStarter(): Unit = {}
          override val supervisorStrategy: SupervisorStrategy = makeStrategy( 3, 1.minute ) {
            case _: DomainModel.NoIndexForAggregateError ⇒ SupervisorStrategy.Stop
            case _: akka.actor.ActorInitializationException ⇒ SupervisorStrategy.Stop
            case _: akka.actor.ActorKilledException ⇒ SupervisorStrategy.Stop
            case _: Exception ⇒ SupervisorStrategy.Stop
            case _ ⇒ SupervisorStrategy.Escalate
          }
        }
      ),
      "CatalogSupervisor"
    )

    val catalogProps = PlanCatalog.props( bc.system )

    import akka.pattern.ask
    implicit val ec = bc.system.dispatcher
    implicit val timeout = Timeout( 270.seconds ) // 90% of 5.minutes

    for {
      ChildStarted( catalog ) ← ( catalogSupervisor ? StartChild( catalogProps, PlanCatalog.name ) ).mapTo[ChildStarted]
      _ = log.info( Map( "@msg" → "catalog initialization started", "catalog" → catalog.toString ) )
      _ ← ( catalog ? CP.WaitForStart ).mapTo[CP.Started.type]
    } yield {
      log.debug( "catalog initialization completed" )
      catalog
    }
  }

  def detectFlowFrom(
    catalogFlow: DetectFlow,
    settings: Settings
  )(
    implicit
    system: ActorSystem,
    materializer: Materializer
  ): DetectFlow = {
    val graph = GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      //todo add support to watch FlowShapes
      val scoring = b.add( OutlierScoringModel.scoringGraph( catalogFlow, settings ) )
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
      .named( "SpotlightScoringModel" )
      .withAttributes( ActorAttributes supervisionStrategy supervisionDecider )
  }

  def clusterFlowFrom(
    detectFlow: DetectFlow,
    settings: Settings
  )(
    implicit
    system: ActorSystem,
    materializer: Materializer
  ): DetectFlow = {
    //      ts ~> localEgress ~> clusterIngress ~> flow ~> clusterEgress ~> localIngress ~> o
    val ( clusterIngressRef, clusterIngressPublisher ) = {
      Source
        .actorPublisher( StreamIngress.props[TimeSeries] )
        .toMat( Sink.asPublisher( false ) )( Keep.both )
        .run()
    }

    val ( localIngressRef, localIngressPublisher ) = {
      Source
        .actorPublisher( StreamIngress.props[Outliers] )
        .toMat( Sink.asPublisher( false ) )( Keep.both )
        .run()
    }

    val localEgressProps = StreamEgress.props( clusterIngressRef, high = 8 )
    val clusterEgressProps = StreamEgress.props( localIngressRef, high = 8 )

    val graph = GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val localEgress = b.add( Sink.actorSubscriber( localEgressProps ) )
      val clusterIngress = b.add( Source.fromPublisher( clusterIngressPublisher ) )
      val clusterEgress = b.add( Sink.actorSubscriber( clusterEgressProps ) )
      val localIngress = b.add( Source.fromPublisher( localIngressPublisher ) )
      val flow = b.add( detectFlow )

      clusterIngress ~> flow ~> clusterEgress

      FlowShape( localEgress.in, localIngress.out )
    }

    Flow.fromGraph( graph ).named( "ClusteredDetectFlow" )
  }

  val supervisionDecider: Supervision.Decider = {
    case ex ⇒ {
      log.error( "Error caught by Supervisor:", ex )
      workflowFailuresMeter.mark()
      Supervision.Restart
    }
  }
}

