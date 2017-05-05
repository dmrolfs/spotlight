package spotlight.analysis

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.reflect._
import akka.Done
import akka.actor.{ Actor, ActorRef, ActorSystem, PoisonPill, Props, Status }
import akka.agent.Agent
import akka.cluster.singleton.{ ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings }
import akka.event.LoggingReceive
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Materializer }
import akka.util.Timeout

import scalaz._
import Scalaz._
import shapeless.syntax.typeable._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import com.persist.logging._
import nl.grons.metrics.scala.MetricName
import omnibus.akka.envelope._
import omnibus.akka.envelope.pattern.ask
import omnibus.akka.metrics.InstrumentedActor
import omnibus.akka.persistence.query.{ AgentProjection, QueryJournal }
import omnibus.commons.config._
import demesne.{ BoundedContext, DomainModel, StartTask }
import spotlight.Settings
import spotlight.infrastructure.ClusterRole
import spotlight.model.outlier.AnalysisPlan
import spotlight.model.timeseries._

/** Created by rolfsd on 5/20/16.
  */
object PlanCatalog extends ClassLogging {
  val clusterRole: ClusterRole = ClusterRole.Intake

  def startSingleton(
    configuration: Config,
    applicationPlans: Set[AnalysisPlan] = Set.empty[AnalysisPlan]
  )(
    implicit
    ec: ExecutionContext
  ): StartTask = {
    StartTask.withFunction( "start plan catalog cluster singleton" ) { implicit bc ⇒
      val SettingsBase = "spotlight.settings"
      val mifFactor = configuration.as[Option[Double]]( SettingsBase + ".parallelism-factor" ) getOrElse 8.0
      val budget = configuration.as[Option[Duration]]( SettingsBase + ".detection-budget" )

      bc.system.actorOf(
        ClusterSingletonManager.props(
          singletonProps = actorProps( configuration, mifFactor, budget, applicationPlans ),
          terminationMessage = PoisonPill,
          settings = ClusterSingletonManagerSettings( bc.system ).withRole( ClusterRole.Intake.entryName )
        ),
        name = PlanCatalog.name
      )

      Done
    }
  }

  def props( implicit system: ActorSystem ): Props = {
    ClusterSingletonProxy.props(
      singletonManagerPath = "/user/" + name,
      settings = ClusterSingletonProxySettings( system ).withRole( clusterRole.entryName )
    )
  }

  private[analysis] def actorProps(
    configuration: Config,
    maxInFlightCpuFactor: Double = 8.0,
    applicationDetectionBudget: Option[Duration] = None,
    applicationPlans: Set[AnalysisPlan] = Set.empty[AnalysisPlan]
  )(
    implicit
    boundedContext: BoundedContext
  ): Props = {
    Props( new Default( boundedContext, configuration, applicationPlans, maxInFlightCpuFactor, applicationDetectionBudget ) )
  }

  val name: String = "PlanCatalog"

  def flow(
    catalogRef: ActorRef,
    parallelism: Int
  )(
    implicit
    boundedContext: BoundedContext,
    timeout: Timeout,
    materializer: Materializer
  ): Future[DetectFlow] = {
    import spotlight.analysis.{ PlanCatalogProtocol ⇒ P }
    implicit val ec = boundedContext.system.dispatcher

    for {
      _ ← ( catalogRef ?+ P.WaitForStart )
      Envelope( sf: P.SpotlightFlow, _ ) ← ( catalogRef ?+ P.MakeFlow() ).mapTo[Envelope]
      f ← sf.factory.makeFlow( parallelism )
    } yield f
  }

  object WatchPoints {
    val Catalog = 'catalog
    val Intake = Symbol( "catalog.intake" )
    val Collector = Symbol( "catalog.collector" )
    val Outlet = Symbol( "catalog.outlet" )
  }

  private[analysis] case class PlanRequest( subscriber: ActorRef, startMillis: Long = System.currentTimeMillis() )

  trait PlanProvider {
    def specifiedPlans: Set[AnalysisPlan]
  }

  trait ExecutionProvider {
    def detectionBudget: Duration
    def maxInFlight: Int
    def correlationId: WorkId
  }

  trait DefaultExecutionProvider extends ExecutionProvider { outer: EnvelopingActor with ActorLogging ⇒
    def configuration: Config
    def applicationDetectionBudget: Option[Duration]
    def maxInFlightCpuFactor: Double

    override val maxInFlight: Int = ( Runtime.getRuntime.availableProcessors() * maxInFlightCpuFactor ).toInt

    override lazy val detectionBudget: Duration = {
      applicationDetectionBudget
        .getOrElse {
          Settings.detectionBudgetFrom( configuration )
            .getOrElse { 10.seconds.toCoarsest }
        }
    }

    def correlationId: WorkId = {
      if ( workId != WorkId.unknown ) workId
      else {
        workId = WorkId()
        log.warn( Map( "@msg" → "value for message workId / correlationId is UNKNOWN", "set-work-id" → workId.toString() ) )
        workId
      }
    }
  }

  final class Default private[PlanCatalog] (
      boundedContext: BoundedContext,
      override val configuration: Config,
      override val specifiedPlans: Set[AnalysisPlan],
      override val maxInFlightCpuFactor: Double = 8.0,
      override val applicationDetectionBudget: Option[Duration] = None
  ) extends PlanCatalog( boundedContext ) with DefaultExecutionProvider with PlanProvider {
    log.debug( Map( "@msg" → "PlanCatalog constructed", "specified-plans" → specifiedPlans.map( _.name ) ) )
  }

  case object NoRegisteredPlansError extends IllegalStateException( "Cannot create detection model without registered plans" )
}

abstract class PlanCatalog( boundedContext: BoundedContext )
    extends Actor with EnvelopingActor with InstrumentedActor with ActorLogging {
  outer: PlanCatalog.ExecutionProvider with PlanCatalog.PlanProvider ⇒

  import spotlight.analysis.{ PlanCatalogProtocol ⇒ P, AnalysisPlanProtocol ⇒ AP }

  override lazy val metricBaseName: MetricName = MetricName( classOf[PlanCatalog] )

  private[this] case object Initialize extends P.CatalogMessage
  private[this] case object InitializeCompleted extends P.CatalogMessage

  override def preStart(): Unit = self ! Initialize

  val fallbackIndex: Agent[Map[String, AnalysisPlan.Summary]] = {
    Agent( Map.empty[String, AnalysisPlan.Summary] )( scala.concurrent.ExecutionContext.global )
  }

  implicit val system: ActorSystem = context.system
  implicit val materializer: Materializer = ActorMaterializer( ActorMaterializerSettings( system ) )
  implicit val ec: ExecutionContext = system.dispatcher

  type Plans = Set[AnalysisPlan.Summary]

  def knownPlans: Future[Plans] = planProjection.view.future() // Future successful { knownPlans }
  def unsafeKnownPlans: Plans = planProjection.view.get()

  val planProjection: AgentProjection[Plans] = {
    new AgentProjection[Plans](
      queryJournal = QueryJournal.fromSystem( context.system ),
      zero = Set.empty[AnalysisPlan.Summary],
      tag = AnalysisPlanModule.module.rootType.name,
      selectLensFor = {
        case AP.Added( _, Some( p: AnalysisPlan ) ) ⇒ {
          log
            .warn(
              Map(
                "@msg" → "#TEST:info: directive from AP.Added",
                "plan" → Map( "name" → p.name, "id" → p.id.id.toString )
              )
            )
          ( acc: Plans ) ⇒ acc + p.toSummary
        }
        case AP.Disabled( pid, _ ) ⇒ {
          log.warn( Map( "@msg" → "#TEST:info: READ Plan Disabled", "planId" → pid ) )
          updateKnownPlan( pid ) { _.copy( isActive = false ) }
        }
        case AP.Enabled( pid, _ ) ⇒ {
          log.warn( Map( "@msg" → "#TEST:info: READ Plan Enabled", "planId" → pid ) )
          updateKnownPlan( pid ) { _.copy( isActive = true ) }
        }
        case AP.Renamed( pid, _, newName ) ⇒ {
          log.warn( Map( "@msg" → "#TEST:info: READ Plan Renamed", "planId" → pid, "newName" → newName ) )
          updateKnownPlan( pid ) { _.copy( name = newName ) }
        }
      }
    )
  }

  def startPlanProjection(): Future[Plans] = {
    planProjection.start() map { ps ⇒
      log.info(
        Map(
          "@msg" → "start analysis plan projection",
          "initial" → ps.map { p ⇒ p.name + '@' + p.id.id.toString }.mkString( "[", ", ", "]" )
        )
      )

      ps
    }
  }

  private def renderKnownPlans(): String = {
    planProjection.view.get().map { p ⇒ Map( "name" → p.name, "id" → p.id.id.toString ) }.mkString( "[", ", ", "]" )
  }

  def updateKnownPlan( pid: AnalysisPlan#TID )( fn: AnalysisPlan.Summary ⇒ AnalysisPlan.Summary ): Plans ⇒ Plans = {
    ( acc: Plans ) ⇒ acc.find { _.id == pid }.map { fn }.map { acc + _ }.getOrElse { acc }
  }
  val EventType = classTag[AP.Event]

  def collectPlans()( implicit ec: ExecutionContext ): Future[Plans] = {
    for {
      fromIndex ← knownPlans
      _ = log.debug( Map( "@msg" → "PlanCatalog fromIndex", "index" → fromIndex.toString ) )
      fromFallback ← fallbackIndex.map { _.values.toSet }.future()
      _ = log.debug( Map( "@msg" → "PlanCatalog fromFallback", "fallback" → fromFallback.toString ) )
    } yield fromIndex ++ fromFallback
  }

  private def doesPlanApply( o: Any )( p: AnalysisPlan.Summary ): Boolean = {
    p.appliesTo map { _.apply( o ) } getOrElse true
  }

  def unsafeApplicablePlanExists( ts: TimeSeries ): Boolean = {
    implicit val ec = context.dispatcher

    log.debug(
      Map(
        "@msg" → "unsafe look at plans for one that applies to topic",
        "index-size" → Await.result( knownPlans.map( _.size ), 3.seconds ).toString,
        "topic" → ts.topic.toString,
        "indexed-plans" → Await.result( knownPlans.map( _.mkString( "[", ", ", "]" ) ), 3.seconds )
      )
    )
    val applyTest = doesPlanApply( ts )( _: AnalysisPlan.Summary )
    Await.result( knownPlans.map( _.exists( applyTest ) ), 3.seconds )
  }

  def futureApplicablePlanExists( ts: TimeSeries )( implicit ec: ExecutionContext ): Future[Boolean] = {
    knownPlans map { entries ⇒
      log.debug(
        Map(
          "@msg" → "safe look at plans for one that applies to topic",
          "index-size" → entries.size,
          "topic" → ts.topic.toString,
          "indexed-plans" → entries.mkString( "[", ", ", "]" )
        )
      )

      val applyTest = doesPlanApply( ts )( _: AnalysisPlan.Summary )
      entries exists { applyTest }
    }
  }

  var outstandingWork: Map[WorkId, ActorRef] = Map.empty[WorkId, ActorRef]

  override def receive: Receive = LoggingReceive { around( quiescent() ) }

  def quiescent( waiting: Set[ActorRef] = Set.empty[ActorRef] ): Receive = {
    case Initialize ⇒ {
      import akka.pattern.pipe
      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool
      implicit val timeout = Timeout( 30.seconds )

      val init = {
        for {
          known ← startPlanProjection()
          _ = log.info( Map( "@msg" → "known current plan loaded", "known" → renderKnownPlans() ) )
          _ ← initializePlans()
        } yield InitializeCompleted
      }

      init pipeTo self
    }

    case Status.Failure( ex ) ⇒ {
      log.error( Map( "@msg" → "failed to initialize plans", "waiting" → waiting.map( _.path.name ) ), ex )
      throw ex
    }

    case InitializeCompleted ⇒ {
      log.info( Map( "@msg" → "initialization completed - plan catalog activating", "known" → renderKnownPlans() ) )
      waiting foreach { _ ! P.Started }
      context become LoggingReceive { around( active orElse admin ) }
    }

    case P.WaitForStart ⇒ {
      log.debug( Map( "@msg" → "received WaitForStart request - adding to waiting queue", "sender" → sender.path.name ) )
      context become LoggingReceive { around( quiescent( waiting + sender() ) ) }
    }
  }

  val active: Receive = {
    case _: P.MakeFlow ⇒ {
      import omnibus.akka.envelope.pattern.pipe
      val requester = sender()
      collectPlans() map { ps ⇒ P.SpotlightFlow( ps, new SpotlightFlowFactory( ps ) ) } pipeEnvelopeTo requester
    }
  }

  val admin: Receive = {
    case req @ P.GetPlansForTopic( topic ) ⇒ {
      import omnibus.akka.envelope.pattern.pipe

      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool

      knownPlans
        .map { entries ⇒
          val ps = entries filter { doesPlanApply( topic ) }
          log.debug(
            Map(
              "@msg" → "PlanCatalog: for topic returning plans",
              "topic" → topic.toString,
              "plans" → ps.map( _.name ).mkString( "[", ", ", "]" )
            )
          )
          P.CatalogedPlans( plans = ps.toSet, request = req )
        }
        .pipeEnvelopeTo( sender() )
    }

    case P.WaitForStart ⇒ sender() ! P.Started
  }

  private def dispatch( route: P.Route, interestedRef: ActorRef )( implicit ec: ExecutionContext ): Unit = {
    val cid = route.correlationId getOrElse outer.correlationId
    outstandingWork += ( cid → interestedRef ) // keep this out of future closure or use an Agent to protect against race conditions

    for {
      model ← boundedContext.futureModel
      entries ← knownPlans
    } {
      val applyTest = doesPlanApply( route.timeSeries )( _: AnalysisPlan.Summary )
      entries withFilter { applyTest } foreach { plan ⇒
        val planRef = model( AnalysisPlanModule.module.rootType, plan.id )
        log.debug(
          Map(
            "@msg" → "DISPATCH: sending topic to plan-module with sender",
            "topic" → route.timeSeries.topic.toString,
            "plan-module" → planRef.path.name,
            "sender" → interestedRef.path.name
          )
        )
        planRef !+ AP.AcceptTimeSeries( plan.id, Set( cid ), route.timeSeries )
      }
    }
  }

  private def initializePlans()( implicit ec: ExecutionContext, timeout: Timeout ): Future[Done] = {
    for {
      entries ← knownPlans
      entryNames = entries map { _.name }
      ( registered, missing ) = outer.specifiedPlans partition { entryNames contains _.name }
      _ = log.info( Map( "@msg" → "previously registered plans", "plans" → registered.map( _.name ) ) )
      _ = log.info( Map( "@msg" → "missing plans", "missing-plans" → missing.map( _.name ) ) )
      created ← makeMissingSpecifiedPlans( missing )
      recorded = created.keySet intersect entryNames
      remaining = created -- recorded
      _ ← fallbackIndex alter { plans ⇒ plans ++ remaining }
    } yield {
      log.info( Map( "@msg" → "created additional plans", "nr-created" → created.size, "created" → created ) )

      log.info(
        Map(
          "@msg" → "recorded new plans in index",
          "recorded" → recorded.mkString( "[", ", ", "]" ),
          "remaining" → remaining.map( _._2.name )
        )
      )

      log.info( Map( "@msg" → "index updated with additional plan(s)", "plans" → created ) )

      if ( remaining.nonEmpty ) {
        log.warn(
          Map(
            "@msg" → "not all newly created plans have been recorded in index yet",
            "remaining" → remaining.map( _._2.name ).mkString( "[", ", ", "]" )
          )
        )
      }

      Done
    }
  }

  def makeMissingSpecifiedPlans(
    missing: Set[AnalysisPlan]
  )(
    implicit
    ec: ExecutionContext,
    to: Timeout
  ): Future[Map[String, AnalysisPlan.Summary]] = {
    def loadSpecifiedPlans( model: DomainModel ): Future[Seq[AnalysisPlan]] = { // Seq[Future[( String, AnalysisPlan.Summary )]] = {
      val loaded: Seq[Future[Option[AnalysisPlan]]] = missing.toSeq.map { p ⇒
        log.info( Map( "@msg" → "making plan entity", "entry" → p.name ) )
        val planRef = model( AnalysisPlanModule.module.rootType, p.id )

        for {
          Envelope( added: AP.Added, _ ) ← ( planRef ?+ AP.Add( p.id, Some( p ) ) ).mapTo[Envelope]
          _ = log.debug( Map( "@msg" → "confirmed that plan is added", "added" → added.info.toString ) )
          addedPlan ← added.info map { Future.successful } getOrElse { Future.failed( new IllegalStateException( s"expected to receive AnalysisPlan but received: ${added.info.toString}" ) ) }
          //                    loaded ← loadPlan( p.id )
          //          _ = log.debug( Map( "@msg" → "loaded plan", "plan" → loaded._2.name ) )
        } yield {
          //          if ( planDirectiveForEvent isDefinedAt added ) self ! planDirectiveForEvent( added )
          //          loaded
          addedPlan.cast[AnalysisPlan]
        }
      }

      Future.sequence( loaded ) map { _.flatten }
    }

    val made = {
      for {
        m ← boundedContext.futureModel
        loaded ← loadSpecifiedPlans( m )
      } yield {
        Map( loaded.toSeq.map { l ⇒ ( l.name, l.toSummary ) }: _* )
      }
    }

    made.transform(
      identity,
      ex ⇒ {
        log.error( Map( "@msg" → "failed to make missing plans", "missing" → missing.map( _.name ) ), ex )
        ex
      }
    )
  }
}

//def flow2(
//catalogRef: ActorRef,
//parallelismCpuFactor: Double = 2.0
//)(
//implicit system: ActorSystem,
//timeout: Timeout,
//materializer: Materializer
//): Flow[TimeSeries, Outliers, NotUsed] = {
//  import PlanCatalogProtocol.Route
//  import omnibus.akka.stream.StreamMonitor._
//  import WatchPoints.{ Intake, Outlet }
//
//  implicit val ec = system.dispatcher
//
//  //todo wrap flow with Dynamic Stages & kill switch
//  val intake = Flow[TimeSeries].map{ ts => Route(ts) }.watchFlow( Intake )
//  val toOutliers = Flow[DetectionResult].map { _.outliers }.watchFlow( WatchPoints.Outlet )
//  val parallelism = ( parallelismCpuFactor * Runtime.getRuntime.availableProcessors ).toInt
//
//  Flow[TimeSeries]
//  .via( intake )
//  .mapAsync( parallelism ) { route =>
//  logger.info( "FLOW2.BEFORE: time-series=[{}]", route )
//  ( catalogRef ?+ route ) map { result =>
//  logger.info( "FLOW2.AFTER: result=[{}]", result )
//  result
//}
//}
//  .collect {
//  case m: DetectionResult => logger.info( "FLOW2.collect - DetectionResult: [{}]", m ); m
//  case Envelope( m: DetectionResult, _ ) => logger.info( "FLOW2.collect - Envelope: [{}]", m ); m
//}
//  .map { m => logger.debug( "received result from collector: [{}]", m ); m }
//  .via( toOutliers )
//  .named( "PlanCatalogFlow" )
//  .watchFlow( Symbol("CatalogDetection") )
//}
//
//
//  def flow(
//  catalogProxyProps: Props
//  )(
//  implicit system: ActorSystem,
//  materializer: Materializer
//  ): Flow[TimeSeries, Outliers, NotUsed] = {
//  import PlanCatalogProtocol.Route
//  import omnibus.akka.stream.StreamMonitor._
//  import WatchPoints.{Intake, Collector, Outlet}
//
//  val outletProps = CommonActorPublisher.props[DetectionResult]()
//
//  val g = GraphDSL.create() { implicit b =>
//  import akka.stream.scaladsl.GraphDSL.Implicits._
//
//  val (outletRef, outlet) = {
//  Source
//  .actorPublisher[DetectionResult]( outletProps ).named( "PlanCatalogFlowOutlet" )
//  .toMat( Sink.asPublisher(true) )( Keep.both )
//  .run()
//}
//
//  PlanCatalogProxy.subscribers send { _ + outletRef }
//
//  val zipWithSubscriber = b.add( Flow[TimeSeries].map{ ts => Route(ts) }.watchFlow(Intake) )
//  val planRouter = b.add( Sink.actorSubscriber[Route]( catalogProxyProps ).named( "PlanCatalogProxy" ) )
//  zipWithSubscriber ~> planRouter
//
//  val receiveOutliers = b.add( Source.fromPublisher[DetectionResult]( outlet ).named( "ResultCollector" ) )
//  val collect = b.add( Flow[DetectionResult].map{ identity }.watchFlow( Collector ) )
//  val toOutliers = b.add(
//  Flow[DetectionResult]
//  .map { m => logger.debug( "received result from collector: [{}]", m ); m }
//  .map { _.outliers }
//  .watchFlow( Outlet )
//  )
//
//  receiveOutliers ~> collect ~> toOutliers
//
//  FlowShape( zipWithSubscriber.in, toOutliers.out )
//}
//
//  Flow.fromGraph( g ).named( "PlanCatalogFlow" ).watchFlow( Symbol("CatalogDetection") )
//}
