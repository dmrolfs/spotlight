package spotlight.analysis

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.reflect._
import akka.{ Done, NotUsed }
import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Stash, Status }
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.pattern.{ ask, pipe }
import akka.persistence.query.{ EventEnvelope2, NoOffset, Offset }
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, FlowShape, Materializer }
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Keep, Merge, Sink }
import akka.util.Timeout

import scalaz._
import Scalaz._
import com.typesafe.config.Config
import com.persist.logging._
import nl.grons.metrics.scala.MetricName
import omnibus.akka.envelope._
import omnibus.akka.envelope.pattern.{ ask ⇒ envAsk }
import omnibus.akka.metrics.InstrumentedActor
import demesne.{ BoundedContext, DomainModel }
import spotlight.Settings
import spotlight.analysis.OutlierDetection.DetectionResult
import spotlight.analysis.PlanCatalog.WatchPoints
import spotlight.model.outlier.{ AnalysisPlan, Outliers }
import spotlight.model.timeseries.{ TimeSeries, Topic }

/** Created by rolfsd on 5/20/16.
  */
object PlanCatalogProtocol {
  sealed trait CatalogMessage

  case object WaitForStart extends CatalogMessage
  case object Started extends CatalogMessage

  sealed trait PlanDirective extends CatalogMessage
  case class AddPlan( plan: AnalysisPlan ) extends PlanDirective
  case class DisablePlan( pid: AnalysisPlan#TID ) extends PlanDirective
  case class EnablePlan( pid: AnalysisPlan#TID ) extends PlanDirective
  case class RenamePlan( pid: AnalysisPlan#TID, newName: String ) extends PlanDirective

  case class MakeFlow(
    parallelism: Int,
    system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ) extends CatalogMessage
  case class CatalogFlow( flow: DetectFlow ) extends CatalogMessage with ClassLogging {
    log.warn( Map( "@msg" → "Made catalog flow", "flow" → flow.toString ) )
  }

  case class Route( timeSeries: TimeSeries, correlationId: Option[WorkId] = None ) extends CatalogMessage

  case class UnknownRoute( timeSeries: TimeSeries, correlationId: Option[WorkId] ) extends CatalogMessage
  object UnknownRoute {
    def apply( route: Route ): UnknownRoute = UnknownRoute( route.timeSeries, route.correlationId )
  }

  case class GetPlansForTopic( topic: Topic ) extends CatalogMessage
  case class CatalogedPlans( plans: Set[AnalysisPlan.Summary], request: GetPlansForTopic ) extends CatalogMessage
}

object PlanCatalog extends ClassLogging {
  def props(
    configuration: Config,
    maxInFlightCpuFactor: Double = 8.0,
    applicationDetectionBudget: Option[Duration] = None,
    applicationPlans: Set[AnalysisPlan] = Set.empty[AnalysisPlan]
  )(
    implicit
    boundedContext: BoundedContext
  ): Props = {
    Props( new Default( boundedContext, configuration, applicationPlans, maxInFlightCpuFactor, applicationDetectionBudget ) )
    // .withDispatcher( "spotlight.planCatalog.stash-dispatcher" )
  }

  val name: String = "PlanCatalog"

  def flow(
    catalogRef: ActorRef,
    parallelism: Int
  )(
    implicit
    system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ): Future[Flow[TimeSeries, Outliers, NotUsed]] = {
    import spotlight.analysis.{ PlanCatalogProtocol ⇒ P }
    implicit val ec = system.dispatcher

    for {
      _ ← ( catalogRef ? P.WaitForStart )
      cf ← ( catalogRef ? P.MakeFlow( parallelism, system, timeout, materializer ) ).mapTo[P.CatalogFlow]
    } yield { cf.flow }
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
    log.debug( Map( "@msg" → "PlanCatalog initialized", "specified-plans" → specifiedPlans.map( _.name ).mkString( "[", ", ", "]" ) ) )
  }

  case object NoRegisteredPlansError extends IllegalStateException( "Cannot create detection model without registered plans" )
}

abstract class PlanCatalog( boundedContext: BoundedContext )
    extends Actor with Stash with EnvelopingActor with InstrumentedActor with ActorLogging {
  outer: PlanCatalog.ExecutionProvider with PlanCatalog.PlanProvider ⇒

  import spotlight.analysis.{ PlanCatalogProtocol ⇒ P, AnalysisPlanProtocol ⇒ AP }

  override lazy val metricBaseName: MetricName = MetricName( classOf[PlanCatalog] )

  case object Initialize extends P.CatalogMessage
  case object InitializeCompleted extends P.CatalogMessage

  override def preStart(): Unit = self ! Initialize

  val fallbackIndex: Agent[Map[String, AnalysisPlan.Summary]] = {
    Agent( Map.empty[String, AnalysisPlan.Summary] )( scala.concurrent.ExecutionContext.global )
  }

  var knownPlans: Set[AnalysisPlan.Summary] = Set.empty[AnalysisPlan.Summary]
  private def renderKnownPlans(): String = {
    knownPlans.map { p ⇒ Map( "name" → p.name, "id" → p.id.id.toString ) }.mkString( "[", ", ", "]" )
  }

  def updateKnownPlan( pid: AnalysisPlan#TID )( fn: AnalysisPlan.Summary ⇒ AnalysisPlan.Summary ): Unit = {
    knownPlans
      .find { _.id == pid }
      .map { fn }
      .foreach { knownPlans += _ }
  }

  def loadCurrentKnownPlans(): Future[Long] = {
    implicit val ec = context.dispatcher
    implicit val materializer = ActorMaterializer( ActorMaterializerSettings( context.system ) )

    log.info( Map( "@msg" → "query currently known plans...", "known" → renderKnownPlans() ) )

    val lastSequenceNr = {
      AnalysisPlanModule
        .queryJournal( context.system )
        .currentEventsByTag( AnalysisPlanModule.module.rootType.name, NoOffset )
        .via( filterKnownPlansFlow )
        .toMat( knownPlansSink )( Keep.right )
        .run()
    }

    lastSequenceNr foreach { snr ⇒
      log.info( Map( "@msg" → "... finished query currently known plans", "snr" → snr, "known" → renderKnownPlans() ) )
    }
    lastSequenceNr
  }

  //todo need to spend more time verifying this operationally updates knownPlans as they're created in the system after start
  def startPlanProjectionFrom( sequenceId: Long ): Unit = {
    implicit val ec = context.dispatcher
    implicit val materializer = ActorMaterializer( ActorMaterializerSettings( context.system ) )

    log.info( "starting active plan projection source..." )

    AnalysisPlanModule
      .queryJournal( context.system )
      .eventsByTag( AnalysisPlanModule.module.rootType.name, Offset.sequence( sequenceId ) )
      .map { e ⇒ log.error( Map( "@msg" → "#TEST CATALOG NOTIFIED OF NEW PLAN EVENT", "event" → e.toString ) ); e }
      .via( filterKnownPlansFlow )
      .to( knownPlansSink )
      .run()
  }

  def knownPlansSink( implicit ec: ExecutionContext ): Sink[( P.PlanDirective, Long ), Future[Long]] = {
    Sink.fold( 0L ) {
      case ( lastSnr, ( d, snr ) ) ⇒
        log.warn( Map( "@msg" → "#TEST sending catalog message to self", "self" → self.path.name, "directive" → d.toString ) )
        self ! d
        math.max( lastSnr, snr )
    }
  }

  val planDirectiveForEvent: PartialFunction[AP.Event, P.PlanDirective] = {
    case AP.Added( _, Some( p: AnalysisPlan ) ) ⇒ {
      log.warn( Map( "@msg" → "#TEST directive from AP.Added", "plan" → Map( "name" → p.name, "id" → p.id.id.toString ) ) )
      P.AddPlan( p )
    }
    case AP.Disabled( planId, _ ) ⇒ {
      log.warn( Map( "@msg" → "#TEST READ Plan Disabled", "planId" → planId ) )
      P.DisablePlan( planId )
    }
    case AP.Enabled( planId, _ ) ⇒ {
      log.warn( Map( "@msg" → "#TEST READ Plan Enabled", "planId" → planId ) )
      P.EnablePlan( planId )
    }
    case AP.Renamed( planId, _, newName ) ⇒ {
      log.warn( Map( "@msg" → "#TEST READ Plan Renamed", "planId" → planId, "newName" → newName ) )
      P.RenamePlan( planId, newName )
    }
  }

  val EventType = classTag[AP.Event]

  val filterKnownPlansFlow: Flow[EventEnvelope2, ( P.PlanDirective, Long ), NotUsed] = {
    Flow[EventEnvelope2]
      .map { e ⇒ log.warn( Map( "@msg" → "#TEST loaded tagged event", "event" → e.toString ) ); e }
      .collect {
        case EventEnvelope2( offset, pid, snr, EventType( event ) ) if planDirectiveForEvent isDefinedAt event ⇒ {
          val directive = planDirectiveForEvent( event )
          log.warn( Map( "@msg" → "#TEST directive for event", "offset" → offset.toString, "pid" → pid, "sequence-nr" → snr, "event" → event.toString, "directive" → directive.toString ) )
          ( directive, snr )
        }
      }
  }

  def indexedPlans: Future[Set[AnalysisPlan.Summary]] = Future successful { knownPlans }

  type PlanIndex = DomainModel.AggregateIndex[String, AnalysisPlanModule.module.TID, AnalysisPlan.Summary]

  def collectPlans()( implicit ec: ExecutionContext ): Future[Set[AnalysisPlan.Summary]] = {
    for {
      fromIndex ← indexedPlans
      _ = log.debug( Map( "@msg" → "PlanCatalog fromIndex", "index" → fromIndex.toString ) )
      fromFallback ← fallbackIndex.map { _.values.toSet }.future()
      _ = log.debug( Map( "@msg" → "PlanCatalog fromFallback", "fallback" → fromFallback.toString ) )
    } yield fromIndex ++ fromFallback
  }

  def applicablePlanExists( ts: TimeSeries )( implicit ec: ExecutionContext ): Boolean = {
    val current = unsafeApplicablePlanExists( ts )
    if ( current ) current
    else {
      val secondary = {
        for {
          futureCheck ← futureApplicablePlanExists( ts )
          fallback ← fallbackIndex.future()
        } yield {
          if ( futureCheck ) {
            log.info(
              Map( "@msg" → "delayed appearance of topic in plan index - removing from fallback", "topic" → ts.topic.toString )
            )
            fallbackIndex send { _ - ts.topic.toString }
            futureCheck
          } else {
            val fallbackResult = fallback contains ts.topic.toString
            if ( fallbackResult ) {
              log.warn(
                Map( "@msg" → "fallback plan found for topic is not yet reflected in plan index", "topic" → ts.topic.toString )
              )
            }

            fallbackResult
          }
        }
      }

      scala.concurrent.Await.result( secondary, 30.seconds )
    }
  }

  private def doesPlanApply( o: Any )( p: AnalysisPlan.Summary ): Boolean = {
    p.appliesTo map { _.apply( o ) } getOrElse true
  }

  def unsafeApplicablePlanExists( ts: TimeSeries ): Boolean = {
    implicit val ec = context.dispatcher

    log.debug(
      Map(
        "@msg" → "unsafe look at plans for one that applies to topic",
        "index-size" → Await.result( indexedPlans.map( _.size ), 3.seconds ).toString,
        "topic" → ts.topic.toString,
        "indexed-plans" → Await.result( indexedPlans.map( _.mkString( "[", ", ", "]" ) ), 3.seconds )
      )
    )
    val applyTest = doesPlanApply( ts )( _: AnalysisPlan.Summary )
    Await.result( indexedPlans.map( _.exists( applyTest ) ), 3.seconds )
  }

  def futureApplicablePlanExists( ts: TimeSeries )( implicit ec: ExecutionContext ): Future[Boolean] = {
    indexedPlans map { entries ⇒
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

  val planDirectives: Receive = {
    case P.AddPlan( p ) ⇒ {
      log.warn( Map( "@msg" → "#TEST adding known plan", "plan" → Map( "id" → p.id.id.toString, "name" → p.name ), "known" → renderKnownPlans() ) )
      knownPlans += p.toSummary
    }

    case P.EnablePlan( pid ) ⇒ {
      log.warn( Map( "@msg" → "#TEST enabling known plan", "pid" → pid.id.toString, "known" → renderKnownPlans() ) )
      updateKnownPlan( pid ) { _.copy( isActive = true ) }
    }

    case P.DisablePlan( pid ) ⇒ {
      log.warn( Map( "@msg" → "#TEST disabling known plan", "pid" → pid.id.toString, "known" → renderKnownPlans() ) )
      updateKnownPlan( pid ) { _.copy( isActive = false ) }
    }

    case P.RenamePlan( pid, newName ) ⇒ {
      log.warn( Map( "@msg" → "#TEST renaming known plan", "pid" → pid.id.toString, "known" → renderKnownPlans() ) )
      updateKnownPlan( pid ) { _.copy( name = newName ) }
    }
  }

  def quiescent( waiting: Set[ActorRef] = Set.empty[ActorRef] ): Receive = planDirectives orElse {
    case Initialize ⇒ {
      import akka.pattern.pipe
      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool
      implicit val timeout = Timeout( 30.seconds )

      val init = {
        for {
          lastSequenceNr ← loadCurrentKnownPlans()
          _ = log.info( Map( "@msg" → "known current plan loaded", "last-sequence-nr" → lastSequenceNr, "known" → renderKnownPlans() ) )
          _ = startPlanProjectionFrom( lastSequenceNr )
          _ = log.info( Map( "@msg" → "ongoing plan projection started", "known" → renderKnownPlans() ) )
          _ ← initializePlans()
          _ = log.info( Map( "@msg" → "remaining specified plans are initialized", "known" → renderKnownPlans() ) )
        } yield InitializeCompleted
      }

      init pipeTo self
    }

    case Status.Failure( ex ) ⇒ {
      log.error( Map( "@msg" → "failed to initialize plans", "waiting" → waiting.map( _.path.name ).mkString( "[", ", ", "]" ) ), ex )
      throw ex
    }

    case InitializeCompleted ⇒ {
      log.info( Map( "@msg" → "initialization completed - plan catalog activating", "known" → renderKnownPlans() ) )
      waiting foreach { _ ! P.Started }
      context become LoggingReceive { around( planDirectives orElse active orElse admin ) }
    }

    case P.WaitForStart ⇒ {
      log.debug( Map( "@msg" → "received WaitForStart request - adding to waiting queue", "sender" → sender.path.name ) )
      context become LoggingReceive { around( quiescent( waiting + sender() ) ) }
    }
  }

  val active: Receive = {
    case P.MakeFlow( parallelism, system, timeout, materializer ) ⇒ {
      implicit val ec = context.dispatcher
      val requester = sender()
      makeFlow( parallelism )( system, timeout, materializer ) map { P.CatalogFlow.apply } pipeTo requester
    }

    //todo remove
    case route @ P.Route( ts: TimeSeries, _ ) if applicablePlanExists( ts )( context.dispatcher ) ⇒ {
      log.debug(
        Map(
          "@msg" → "PlanCatalog:ACTIVE dispatching stream message for detection",
          "work-id" → workId.toString,
          "route" → route.toString
        )
      )
      dispatch( route, sender() )( context.dispatcher )
    }

    //todo remove
    case ts: TimeSeries if applicablePlanExists( ts )( context.dispatcher ) ⇒ {
      log.debug(
        Map(
          "@msg" → "PlanCatalog:ACTIVE dispatching time series to sender",
          "work-id" → workId,
          "sender" → sender().path.name,
          "topic" → ts.topic.toString
        )
      )
      dispatch( P.Route( ts, Some( correlationId ) ), sender() )( context.dispatcher )
    }

    //todo remove
    case r: P.Route ⇒ {
      //todo route unrecogonized ts
      log.warn(
        Map( "@msg" → "PlanCatalog:ACTIVE: no plan on record that applies to topic", "topic" → r.timeSeries.topic.toString )
      )
      sender() !+ P.UnknownRoute( r )
    }

    //todo remove
    case ts: TimeSeries ⇒ {
      //todo route unrecogonized ts
      log.warn(
        Map( "@msg" → "PlanCatalog:ACTIVE: no plan on record that applies to topic", "topic" → ts.topic.toString )
      )
      sender() !+ P.UnknownRoute( ts, Some( correlationId ) )
    }

    //todo remove
    case result @ DetectionResult( outliers, workIds ) ⇒ {
      if ( 1 < workIds.size ) {
        log.warn(
          Map(
            "@msg" → "received DetectionResult with multiple workIds",
            "nr-work-id" → workIds.size,
            "work-ids" → workIds.mkString( "[", ", ", "]" )
          )
        )
      }

      workIds find { outstandingWork.contains } foreach { cid ⇒
        val subscriber = outstandingWork( cid )
        log.debug( Map( "@msg" → "FLOW2: sending result to subscriber", "subscriber" → subscriber.path.name, "result" → result.toString ) )
        subscriber !+ result
      }

      outstandingWork --= workIds
    }
  }

  val admin: Receive = {
    case req @ P.GetPlansForTopic( topic ) ⇒ {
      import omnibus.akka.envelope.pattern.pipe

      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool

      indexedPlans
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

  def makeFlow(
    parallelism: Int
  )(
    implicit
    system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ): Future[DetectFlow] = {
    import omnibus.akka.stream.StreamMonitor._

    implicit val ec = context.dispatcher
    def collectPlanFlows( model: DomainModel, plans: Set[AnalysisPlan.Summary] ): Future[Set[DetectFlow]] = {
      Future sequence {
        plans map { p ⇒
          val ref = model( AnalysisPlanModule.module.rootType, p.id )

          ( ref ?+ AP.MakeFlow( p.id, parallelism, system, timeout, materializer ) )
            .mapTo[AP.AnalysisFlow]
            .map { af ⇒
              log.debug(
                Map( "@msg" → "PlanCatalog: created analysis flow for", "plan" → Map( "id" → p.id.id.toString, "name" → p.name ) )
              )
              af.flow
            }
        }
      }
    }

    def detectFrom( planFlows: Set[DetectFlow] ): Future[DetectFlow] = {
      val nrFlows = planFlows.size
      log.debug( Map( "@msg" → "making PlanCatalog graph for plans", "nr-plans" → nrFlows ) )

      val graph = Future {
        GraphDSL.create() { implicit b ⇒
          import GraphDSL.Implicits._

          //          val intake = b.add( Flow[TimeSeries].map { identity }
          //          val outlet = b.add( Flow[Outliers].map { identity }

          if ( planFlows.isEmpty ) throw PlanCatalog.NoRegisteredPlansError
          else {
            val broadcast = b.add( Broadcast[TimeSeries]( nrFlows ) )
            val merge = b.add( Merge[Outliers]( nrFlows ) )

            //            intake ~> broadcast.in
            planFlows.zipWithIndex foreach {
              case ( pf, i ) ⇒
                log.debug( Map( "@msg" → "adding to catalog flow, order undefined analysis flow", "index" → i ) )
                val flow = b.add( pf )
                broadcast.out( i ) ~> flow ~> merge.in( i )
            }
            //            merge.out ~> outlet

            FlowShape( broadcast.in, merge.out )
          }
        }
      }

      graph map { g ⇒ Flow.fromGraph( g ).named( "PlanCatalogFlow" ).watchFlow( WatchPoints.Catalog ) }
    }

    for {
      model ← boundedContext.futureModel
      plans ← collectPlans()
      _ = log.debug( Map( "@msg" → "collect plans", "plans" → plans.toSeq.map { p ⇒ ( p.name, p.id ) }.mkString( "[", ", ", "]" ) ) )
      planFlows ← collectPlanFlows( model, plans )
      f ← detectFrom( planFlows )
    } yield f.named( s"PlanCatalog" )
  }

  private def dispatch( route: P.Route, interestedRef: ActorRef )( implicit ec: ExecutionContext ): Unit = {
    val cid = route.correlationId getOrElse outer.correlationId
    outstandingWork += ( cid → interestedRef ) // keep this out of future closure or use an Agent to protect against race conditions

    for {
      model ← boundedContext.futureModel
      entries ← indexedPlans
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
      //      entries <- planIndex.futureEntries
      entries ← indexedPlans //todo dmr
      entryNames = entries map { _.name }
      ( registered, missing ) = outer.specifiedPlans partition { entryNames contains _.name }
      _ = log.info( Map( "@msg" → "registered plans", "plans" → registered.map( _.name ).mkString( "[", ", ", "]" ) ) )
      _ = log.info( Map( "@msg" → "missing plans", "missing-plans" → missing.map( _.name ).mkString( "[", ", ", "]" ) ) )
      created ← makeMissingSpecifiedPlans( missing )
      recorded = created.keySet intersect entryNames
      remaining = created -- recorded
      _ ← fallbackIndex alter { plans ⇒ plans ++ remaining }
    } yield {
      log.info(
        Map(
          "@msg" → "created additional plans",
          "nr-created" → created.size,
          "created" → created.map { case ( n, c ) ⇒ ( n, c.toString ) }
        )
      )

      log.info(
        Map(
          "@msg" → "recorded new plans in index",
          "recorded" → recorded.mkString( "[", ", ", "]" ),
          "remaining" → remaining.map( _._2.name ).mkString( "[", ", ", "]" )
        )
      )

      log.info( Map( "@msg" → "index updated with additional plan(s)", "plans" → created.map { case ( k, p ) ⇒ ( k, p.toString ) } ) )

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
    def loadSpecifiedPlans( model: DomainModel ): Seq[Future[( String, AnalysisPlan.Summary )]] = {
      missing.toSeq.map { p ⇒
        log.info( Map( "@msg" → "making plan entity", "entry" → p.name ) )
        val planRef = model( AnalysisPlanModule.module.rootType, p.id )

        for {
          Envelope( added: AP.Added, _ ) ← ( planRef ?+ AP.Add( p.id, Some( p ) ) ).mapTo[Envelope]
          _ = log.debug( Map( "@msg" → "confirmed that plan is added", "added" → added.info.toString ) )
          loaded ← loadPlan( p.id )
          _ = log.debug( Map( "@msg" → "loaded plan", "plan" → loaded._2.name ) )
        } yield {
          if ( planDirectiveForEvent isDefinedAt added ) self ! planDirectiveForEvent( added )
          loaded
        }
      }
    }

    val made = {
      for {
        m ← boundedContext.futureModel
        loaded ← Future.sequence( loadSpecifiedPlans( m ) )
      } yield Map( loaded: _* )
    }

    made.transform(
      identity,
      ex ⇒ {
        log.error( Map( "@msg" → "failed to make missing plans", "missing" → missing.map( _.name ).mkString( "[", ", ", "]" ) ), ex )
        ex
      }
    )
  }

  def loadPlan( planId: AnalysisPlan#TID )( implicit ec: ExecutionContext, to: Timeout ): Future[( String, AnalysisPlan.Summary )] = {
    fetchPlanInfo( planId ) map { summary ⇒ ( summary.info.name, summary.info.toSummary ) }
  }

  def fetchPlanInfo( pid: AnalysisPlanModule.module.TID )( implicit ec: ExecutionContext, to: Timeout ): Future[AP.PlanInfo] = {
    def toInfo( message: Any ): Future[AP.PlanInfo] = {
      message match {
        case Envelope( info: AP.PlanInfo, _ ) ⇒ {
          log.info( Map( "@msg" → "fetched plan entity", "plan-id" → info.sourceId.toString ) )
          Future successful info
        }

        case info: AP.PlanInfo ⇒ Future successful info

        case m ⇒ {
          val ex = new IllegalStateException( s"unknown response to plan inquiry for id:[${pid}]: ${m}" )
          log.error( Map( "@msg" → "failed to fetch plan for id", "id" → pid.toString ), ex )
          Future failed ex
        }
      }
    }

    for {
      model ← boundedContext.futureModel
      ref = model( AnalysisPlanModule.module.rootType, pid )
      msg ← ( ref ?+ AP.GetPlan( pid ) ).mapTo[Envelope]
      info ← toInfo( msg )
    } yield info
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
