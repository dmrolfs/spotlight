package spotlight

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import akka.Done
import akka.actor.{ ActorRef, ActorSystem, Props, SupervisorStrategy }
import akka.stream._
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source }
import akka.util.Timeout

import scalaz.{ Source ⇒ _, _ }
import scalaz.Scalaz._
import scalaz.Kleisli.kleisli
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import com.persist.logging._
import nl.grons.metrics.scala.{ Meter, MetricName }
import org.slf4j.LoggerFactory
import org.joda.{ time ⇒ joda }
import com.typesafe.scalalogging.Logger
import omnibus.akka.metrics.{ Instrumented, Reporter }
import omnibus.akka.supervision.IsolatedLifeCycleSupervisor.{ ChildStarted, StartChild }
import omnibus.akka.supervision.{ IsolatedLifeCycleSupervisor, OneForOneStrategyFactory }
import omnibus.commons.util._
import demesne.{ AggregateRootType, BoundedContext, DomainModel, StartTask }
import omnibus.akka.stream.{ StreamEgress, StreamIngress }
import omnibus.commons.Valid
import spotlight.analysis._
import spotlight.analysis.algorithm.Algorithm
import spotlight.analysis.{ PlanCatalogProtocol ⇒ CP }
import spotlight.analysis.shard.{ CellShardModule, LookupShardModule }
import spotlight.infrastructure.ClusterRole
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.TimeSeries

/** Created by rolfsd on 11/7/16.
  */
object Spotlight extends Instrumented with ClassLogging {
  type EC[_] = ExecutionContext

  @transient override lazy val metricBaseName: MetricName = MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  @transient lazy val workflowFailuresMeter: Meter = metrics meter "workflow.failures"

  type SpotlightBoundedContext = ( BoundedContext, SpotlightContext )
  type SpotlightModel = ( BoundedContext, SpotlightContext, Option[DetectFlow] )

  def apply[_: EC](): Kleisli[Future, SpotlightContext, SpotlightModel] = {
    prep >=> startBoundedContext >=> makeFlow
  }

  def apply[_: EC]( args: Array[String] ): Kleisli[Future, Array[String], SpotlightModel] = {
    makeContext >=> apply()
  }

  def systemRootTypes( settings: Settings ): Set[AggregateRootType] = {
    val base = Set(
      AnalysisPlanModule.module.rootType,
      LookupShardModule.rootType,
      CellShardModule.module.rootType
    )

    val algorithms = {
      Valid.unsafeGet( Settings.userAlgorithmClassesFrom( settings.config ) )
        .values
        .map { clazz ⇒
          import scala.reflect.runtime.{ universe ⇒ ru }
          val loader = getClass.getClassLoader
          val mirror = ru runtimeMirror loader
          val algorithmSymbol = mirror moduleSymbol clazz
          val algorithmMirror = mirror reflectModule algorithmSymbol
          val algorithm = algorithmMirror.instance.asInstanceOf[Algorithm[_]]
          algorithm.module.rootType
        }
        .toSet
    }

    val all = base ++ algorithms

    settings.role match {
      case ClusterRole.Seed ⇒ Set.empty[AggregateRootType]
      case ClusterRole.All ⇒ all
      case _ ⇒ all // even if not on host, register needed to enable cluster proxying
    }
  }

  def systemStartTasks[_: EC]( settings: Settings ): Set[StartTask] = {
    val all: Map[StartTask, Option[ClusterRole]] = Map(
      PlanCatalog.startSingleton( settings.config, settings.plans ) → Option( PlanCatalog.clusterRole ),
      DetectionAlgorithmRouter.startTask( settings.config ) → AnalysisPlanModule.module.rootType.clusterRole.map( ClusterRole withName _ ),
      metricsReporterStartTask( settings.config ) → Option( ClusterRole.All )
    )

    settings.role match {
      case ClusterRole.Seed ⇒ Set.empty[StartTask]
      case ClusterRole.All ⇒ all.keySet
      case role ⇒ all.toSet[( StartTask, Option[ClusterRole] )].collect { case ( t, r ) if r.forall( _ includes role ) ⇒ t }
      //      case ( t, r ) if r.map( _ includes role ).getOrElse( true ) ⇒ t
      //      }
    }
  }

  def metricsReporterStartTask( config: Config ): StartTask = StartTask.withFunction( "start metrics reporter" ) { bc ⇒
    val MetricsPath = "spotlight.metrics"

    config
      .as[Option[Config]]( MetricsPath )
      .map { metricsConfig ⇒
        val reporter = Reporter startReporter metricsConfig
        log.alternative(
          SpotlightContext.SystemLogCategory,
          Map( "@msg" → "starting metric reporting", "config" → metricsConfig, "reporter" → reporter.toString )
        )
      }
      .getOrElse {
        log.alternative( SpotlightContext.SystemLogCategory, Map( "@msg" → """metric report configuration missing at "spotlight.metrics"""" ) )
        log.warn( """metric report configuration missing at "spotlight.metrics"""" )
      }

    Done
  }

  val makeContext: Kleisli[Future, Array[String], SpotlightContext] = {
    kleisli[Future, Array[String], SpotlightContext] { args ⇒
      import SpotlightContext.{ Builder ⇒ B }

      Future successful {
        SpotlightContext.Builder
          .builder
          .set( B.Arguments, args )
          //      .set( SpotlightContext.StartTasks, Set( /*SharedLeveldbStore.start(true), Spotlight.kamonStartTask*/ ) )
          //      .set( SpotlightContext.System, Some( system ) )
          .build()
      }
    }
  }

  def prep[_: EC]: Kleisli[Future, SpotlightContext, SpotlightContext] = logBuildInfo >=> prepareTimeZone

  private val logBuildInfo: Kleisli[Future, SpotlightContext, SpotlightContext] = kleisli { context ⇒
    Future successful {
      log.alternative(
        SpotlightContext.SystemLogCategory,
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

      context
    }
  }

  private val prepareTimeZone: Kleisli[Future, SpotlightContext, SpotlightContext] = kleisli { context ⇒
    Future successful {
      log.debug( Map( "@msg" → "setting the spotlight timezone", "timezone" → context.settings.timeZone.toString ) )
      joda.DateTimeZone setDefault context.settings.timeZone
      context
    }
  }

  val startBoundedContext: Kleisli[Future, SpotlightContext, SpotlightBoundedContext] = {
    kleisli[Future, SpotlightContext, SpotlightBoundedContext] { context ⇒
      implicit val sys = context.system
      implicit val ec = context.system.dispatcher
      implicit val timeout = context.timeout

      val role = context.settings.role
      val nodeRootTypes = context.rootTypes ++ systemRootTypes( context.settings )
      val nodeStartTasks = context.startTasks ++ systemStartTasks( context.settings )

      log.info(
        Map(
          "@msg" → "making bounded context on node",
          "key" → context.name,
          "root-types" → nodeRootTypes.mkString( "[", ", ", "]" ),
          "user-resources" → context.resources.map( kv ⇒ ( kv._1.name, kv._2.toString ) ),
          "#TEST CONTEXT START TASKS" → context.startTasks.map( _.description ).mkString( "[", ", ", "]" ),
          "#TEST SYSTEM START TASKS" → systemStartTasks( context.settings ).map( _.description ).mkString( "[", ", ", "]" ),
          "#TEST CONTEXT ROOT TYPES" → context.rootTypes.map( _.name ).mkString( "[", ", ", "]" ),
          "#TEST SYSTEM ROOT TYPES" → systemRootTypes( context.settings ).map( _.name ).mkString( "[", ", ", "]" ),
          "start-tasks" → nodeStartTasks.map( _.description ).mkString( "[", ", ", "] " ),
          "usage" → context.settings.usage._2.mapValues( _.toString )
        )
      )

      for {
        made ← BoundedContext.make(
          key = Symbol( context.name ),
          configuration = context.settings.toConfig,
          rootTypes = nodeRootTypes,
          userResources = context.resources,
          startTasks = nodeStartTasks
        )

        started ← made.start()
      } yield ( started, context )
    }
  }

  val makeFlow: Kleisli[Future, SpotlightBoundedContext, SpotlightModel] = {
    kleisli[Future, SpotlightBoundedContext, SpotlightModel] {
      case ( boundedContext, context ) if context.settings.role.hostsFlow == false ⇒ {
        Future successful ( boundedContext, context, None )
      }

      case ( boundedContext, context ) ⇒ {
        val settings = context.settings
        implicit val bc = boundedContext
        implicit val system = context.system
        implicit val dispatcher = system.dispatcher
        implicit val detectionTimeout = Timeout( 5.minutes ) //todo: define in Settings
        implicit val materializer = ActorMaterializer(
          ActorMaterializerSettings( system ) withSupervisionStrategy supervisionDecider
        )

        for {
          catalog ← makeCatalog( bc )
          catalogFlow ← PlanCatalog.flow( catalog, settings.parallelism )
        } yield {
          val detectFlow = detectFlowFrom( catalogFlow, settings )
          val clusteredFlow = clusterFlowFrom( detectFlow, settings )
          ( boundedContext, context, Option( clusteredFlow ) )
        }
      }
    }
  }

  def makeCatalog( bc: BoundedContext ): Future[ActorRef] = {
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

  @transient val supervisionDecider: Supervision.Decider = {
    case ex ⇒ {
      log.error( "Error caught by Supervisor:", ex )
      workflowFailuresMeter.mark()
      Supervision.Restart
    }
  }
}

