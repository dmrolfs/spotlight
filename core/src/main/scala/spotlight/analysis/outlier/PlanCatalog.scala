package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.matching.Regex
import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.stream.{FlowShape, Materializer}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import akka.util.Timeout

import scalaz._
import Scalaz._
import com.typesafe.config.{Config, ConfigObject}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.MetricName
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import peds.akka.metrics.InstrumentedActor
import peds.commons.log.Trace
import peds.akka.stream.CommonActorPublisher
import demesne.{BoundedContext, DomainModel}
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.model.outlier.{IsQuorum, OutlierPlan, Outliers, ReduceOutliers}
import spotlight.model.timeseries.{TimeSeries, Topic}


/**
  * Created by rolfsd on 5/20/16.
  */
object PlanCatalogProtocol {
  sealed trait CatalogMessage

  case object WaitForStart extends CatalogMessage
  case object Started extends CatalogMessage

  case class Route( timeSeries: TimeSeries, subscriber: ActorRef, correlationId: Option[WorkId] = None ) extends CatalogMessage
  case class UnknownRoute( timeSeries: TimeSeries, subscriber: ActorRef, correlationId: Option[WorkId] ) extends CatalogMessage
  object UnknownRoute {
    def apply( route: Route ): UnknownRoute = UnknownRoute( route.timeSeries, route.subscriber, route.correlationId )
  }

  case class GetPlansForTopic( topic: Topic ) extends CatalogMessage
  case class CatalogedPlans( plans: Set[OutlierPlan.Summary], request: GetPlansForTopic ) extends CatalogMessage
}


object PlanCatalog extends LazyLogging {
  def props(
    configuration: Config,
    maxInFlightCpuFactor: Double = 8.0,
    applicationDetectionBudget: Option[FiniteDuration] = None,
    applicationPlans: Set[OutlierPlan] = Set.empty[OutlierPlan]
  )(
    implicit boundedContext: BoundedContext
  ): Props = {
    Props(
      Default(
        boundedContext,
        configuration,
        maxInFlightCpuFactor,
        applicationDetectionBudget,
        applicationPlans
      )
    )
    // .withDispatcher( "spotlight.planCatalog.stash-dispatcher" )
  }

  val name: String = "PlanCatalog"

  def flow(
    catalogProxyProps: Props
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[TimeSeries, Outliers, NotUsed] = {
    import PlanCatalogProtocol.Route
    import peds.akka.stream.StreamMonitor._
    import WatchPoints.{Intake, Collector, Outlet}

    val outletProps = CommonActorPublisher.props[DetectionResult]()

    val g = GraphDSL.create() { implicit b =>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val (outletRef, outlet) = {
        Source
        .actorPublisher[DetectionResult]( outletProps ).named( "PlanCatalogFlowOutlet" )
        .toMat( Sink.asPublisher(true) )( Keep.both )
        .run()
      }

      PlanCatalogProxy.subscribers send { _ + outletRef }

      val zipWithSubscriber = b.add( Flow[TimeSeries].map{ ts => Route( ts, outletRef ) }.watchFlow(Intake) )
      val planRouter = b.add( Sink.actorSubscriber[Route]( catalogProxyProps ).named( "PlanCatalogProxy" ) )
      zipWithSubscriber ~> planRouter

      val receiveOutliers = b.add( Source.fromPublisher[DetectionResult]( outlet ).named( "ResultCollector" ) )

      val collect = Flow[DetectionResult].map{ identity }.watchFlow( Collector )

      val toOutliers = b.add(
        Flow[DetectionResult]
        .map { m => logger.debug( "received result from collector: [{}]", m ); m }
        .map { _.outliers }
        .watchFlow( Outlet )
      )

      receiveOutliers ~> collect ~> toOutliers

      FlowShape( zipWithSubscriber.in, toOutliers.out )
    }

    Flow.fromGraph( g ).named( "PlanCatalogFlow" ).watchFlow( Symbol("CatalogDetection") )
  }


  object WatchPoints {
    val Intake = Symbol( "catalog.intake" )
    val Collector = Symbol( "catalog.collector" )
    val Outlet = Symbol( "catalog.outlet" )
  }


  private[outlier] case class PlanRequest( subscriber: ActorRef, startNanos: Long = System.nanoTime() )


  trait PlanProvider {
    def specifiedPlans: Set[OutlierPlan]
  }

  trait ExecutionProvider {
    def detectionBudget: FiniteDuration
    def maxInFlight: Int
    def correlationId: WorkId
  }

  trait DefaultExecutionProvider extends ExecutionProvider { outer: EnvelopingActor with ActorLogging =>
    def configuration: Config
    def applicationDetectionBudget: Option[FiniteDuration]
    def maxInFlightCpuFactor: Double

    override val maxInFlight: Int = ( Runtime.getRuntime.availableProcessors() * maxInFlightCpuFactor ).toInt

    override lazy val detectionBudget: FiniteDuration = {
      applicationDetectionBudget getOrElse { loadDetectionBudget( configuration ) getOrElse 10.seconds }
    }

    private def loadDetectionBudget( specification: Config ): Option[FiniteDuration] = {
      import Default.DetectionBudgetPath
      if ( specification hasPath DetectionBudgetPath ) {
        Some( FiniteDuration( specification.getDuration( DetectionBudgetPath, NANOSECONDS ), NANOSECONDS ) )
      } else {
        None
      }
    }

    def correlationId: WorkId = {
      if ( workId != WorkId.unknown ) workId
      else {
        workId = WorkId()
        log.warning( "value for message workId / correlationId is UNKNOWN. setting to {}", workId )
        workId
      }
    }
  }


  object Default {
    val DetectionBudgetPath = "spotlight.workflow.detect.timeout"
    val OutlierPlansPath = "spotlight.detection-plans"
  }

  final case class Default private[PlanCatalog](
    boundedContext: BoundedContext,
    configuration: Config,
    maxInFlightCpuFactor: Double = 8.0,
    applicationDetectionBudget: Option[FiniteDuration] = None,
    applicationPlans: Set[OutlierPlan] = Set.empty[OutlierPlan]
  ) extends PlanCatalog( boundedContext ) with DefaultExecutionProvider with PlanProvider {
    private val trace = Trace[Default]

    //todo also load from plan index?
    override lazy val specifiedPlans: Set[OutlierPlan] = applicationPlans ++ loadPlans( configuration )

    private def loadPlans( specification: Config ): Set[OutlierPlan] = {
      import Default.OutlierPlansPath

      if ( specification hasPath OutlierPlansPath ) {
        val planSpecs = specification getConfig OutlierPlansPath
        log.debug( "specification = [{}]", planSpecs.root().render() )
        import scala.collection.JavaConversions._
        // since config is java API needed for collect

        planSpecs.root
        .collect { case (n, s: ConfigObject) => (n, s.toConfig) }
        .toSet[(String, Config)]
        .map { case (name, spec) => makePlan( name, spec )( detectionBudget ) }
        .flatten
      } else {
        log.warning( "PlanCatalog[{}]: no plan specifications found at configuration path:[{}]", self.path, OutlierPlansPath )
        Set.empty[OutlierPlan]
      }
    }

    private def makePlan( name: String, planSpecification: Config )( budget: FiniteDuration ): Option[OutlierPlan] = {
      logger.info( "PlanCatalog making plan from specification: [{}]", planSpecification )

      //todo: bring initialization of plans into module init and base config on init config?
      val utilization = 0.9
      val utilized = budget * utilization
      val timeout = if ( utilized.isFinite ) utilized.asInstanceOf[FiniteDuration] else budget

      val grouping: Option[OutlierPlan.Grouping] = {
        val GROUP_LIMIT = "group.limit"
        val GROUP_WITHIN = "group.within"
        val limit = if ( planSpecification hasPath GROUP_LIMIT ) planSpecification getInt GROUP_LIMIT else 10000
        val window = if ( planSpecification hasPath GROUP_WITHIN ) {
          Some( FiniteDuration( planSpecification.getDuration( GROUP_WITHIN ).toNanos, NANOSECONDS ) )
        } else {
          None
        }

        window map { w => OutlierPlan.Grouping( limit, w ) }
      }

      //todo: add configuration for at-least and majority
      val algorithms = planAlgorithms( planSpecification )

      val IS_DEFAULT = "is-default"
      val TOPICS = "topics"
      val REGEX = "regex"

      if ( planSpecification.hasPath(IS_DEFAULT) && planSpecification.getBoolean(IS_DEFAULT) ) {
        logger.info(
          "PlanCatalog: topic[{}] default-type plan specification origin:[{}] line:[{}]",
          name,
          planSpecification.origin,
          planSpecification.origin.lineNumber.toString
        )

        Some(
          OutlierPlan.default(
            name = name,
            timeout = timeout,
            isQuorum = makeIsQuorum( planSpecification, algorithms.size ),
            reduce = makeOutlierReducer( planSpecification),
            algorithms = algorithms,
            grouping = grouping,
            planSpecification = planSpecification
          )
        )
      } else if ( planSpecification hasPath TOPICS ) {
        import scala.collection.JavaConverters._
        logger.info(
          "PlanCatalog: topic:[{}] topic-based plan specification origin:[{}] line:[{}]",
          name,
          planSpecification.origin,
          planSpecification.origin.lineNumber.toString
        )

        Some(
          OutlierPlan.forTopics(
            name = name,
            timeout = timeout,
            isQuorum = makeIsQuorum( planSpecification, algorithms.size ),
            reduce = makeOutlierReducer( planSpecification ),
            algorithms = algorithms,
            grouping = grouping,
            planSpecification = planSpecification,
            extractTopic = OutlierDetection.extractOutlierDetectionTopic,
            topics = planSpecification.getStringList( TOPICS ).asScala.map{ Topic(_) }.toSet
          )
        )
      } else if ( planSpecification hasPath REGEX ) {
        logger.info(
          "PlanCatalog: topic:[{}] regex-based plan specification origin:[{}] line:[{}]",
          name,
          planSpecification.origin,
          planSpecification.origin.lineNumber.toString
        )

        Some(
          OutlierPlan.forRegex(
            name = name,
            timeout = timeout,
            isQuorum = makeIsQuorum( planSpecification, algorithms.size ),
            reduce = makeOutlierReducer( planSpecification ),
            algorithms = algorithms,
            grouping = grouping,
            planSpecification = planSpecification,
            extractTopic = OutlierDetection.extractOutlierDetectionTopic,
            regex = new Regex( planSpecification getString REGEX )
          )
        )
      } else {
        None
      }
    }

    private def planAlgorithms( spec: Config ): Set[Symbol] = {
      import scala.collection.JavaConversions._
      val AlgorithmsPath = "algorithms"
      if ( spec hasPath AlgorithmsPath ) spec.getStringList( AlgorithmsPath ).toSet.map{ a: String => Symbol( a ) }
      else Set.empty[Symbol]
    }

    private def makeIsQuorum( spec: Config, algorithmSize: Int ): IsQuorum = {
      val MAJORITY = "majority"
      val AT_LEAST = "at-least"
      if ( spec hasPath AT_LEAST ) {
        val trigger = spec getInt AT_LEAST
        IsQuorum.AtLeastQuorumSpecification( totalIssued = algorithmSize, triggerPoint = trigger )
      } else {
        val trigger = if ( spec hasPath MAJORITY ) spec.getDouble( MAJORITY ) else 50D
        IsQuorum.MajorityQuorumSpecification( totalIssued = algorithmSize, triggerPoint = ( trigger / 100D) )
      }
    }

    private def makeOutlierReducer( spec: Config ): ReduceOutliers = {
      val AT_LEAST = "at-least"
      if ( spec hasPath AT_LEAST ) {
        val threshold = spec getInt AT_LEAST
        ReduceOutliers.byCorroborationCount( threshold )
      } else {
        val MAJORITY = "majority"
        val threshold = if ( spec hasPath MAJORITY ) spec.getDouble( MAJORITY ) else 50D
        ReduceOutliers.byCorroborationPercentage( threshold )
      }
    }
  }
}

class PlanCatalog( boundedContext: BoundedContext )
extends Actor with Stash with EnvelopingActor with InstrumentedActor with ActorLogging {
  outer: PlanCatalog.ExecutionProvider with PlanCatalog.PlanProvider =>

  import spotlight.analysis.outlier.{ PlanCatalogProtocol => P }
  import spotlight.analysis.outlier.{ AnalysisPlanProtocol => AP }

  private val trace = Trace[PlanCatalog]

//  var boundedContext: BoundedContext = _boundedContext

  override lazy val metricBaseName: MetricName = MetricName( classOf[PlanCatalog] )

  val fallbackIndex: Agent[Map[String, OutlierPlan.Summary]] = {
    Agent( Map.empty[String, OutlierPlan.Summary] )( scala.concurrent.ExecutionContext.global )
  }


  type PlanIndex = DomainModel.AggregateIndex[String, AnalysisPlanModule.module.TID, OutlierPlan.Summary]
  lazy val planIndex: PlanIndex = {
    implicit val dispatcher = context.dispatcher
    val index = boundedContext.futureModel map { model =>
      model.aggregateIndexFor[String, AnalysisPlanModule.module.TID, OutlierPlan.Summary](
        AnalysisPlanModule.module.rootType,
        AnalysisPlanModule.namedPlanIndex
      ) match {
        case \/-( pi ) => pi
        case -\/( ex ) => {
          log.error( ex, "PlanCatalog[{}]: failed to initialize PlanCatalog's plan index", self.path )
          throw ex
        }
      }
    }

    scala.concurrent.Await.result( index, 30.seconds )
  }

  def applicablePlanExists( ts: TimeSeries )( implicit ec: ExecutionContext ): Boolean = trace.briefBlock( s"applicablePlanExists( ${ts.topic} )" ) {
    val current = unsafeApplicablePlanExists( ts )
    if ( current ) current
    else {
      val secondary = {
        for {
          futureCheck <- futureApplicablePlanExists( ts )
          fallback <- fallbackIndex.future()
        } yield {
          if ( futureCheck ) {
            log.info(
              "PlanCatalog[{}]: delayed appearance of topic:[{}] in plan index - removing from fallback",
              self.path.name, ts.topic
            )
            fallbackIndex send { _ - ts.topic.toString }
            futureCheck
          } else {
            val fallbackResult = fallback contains ts.topic.toString
            if ( fallbackResult ) {
              log.warning(
                "PlanCatalog[{}]: fallback plan not reflected yet in plan index found for topic:[{}]",
                self.path.name, ts.topic
              )
            }

            fallbackResult
          }
        }
      }

      scala.concurrent.Await.result( secondary, 30.seconds )
    }
  }

  def unsafeApplicablePlanExists( ts: TimeSeries ): Boolean = trace.briefBlock( s"unsafeApplicablePlanExists( ${ts.topic} )" ) {
    log.debug(
      "PlanCatalog: unsafe look at {} plans for one that applies to topic:[{}] -- [{}]",
      planIndex.entries.size, ts.topic, planIndex.entries.values.mkString(", ")
    )

    planIndex.entries.values exists { _ appliesTo ts }
  }

  def futureApplicablePlanExists( ts: TimeSeries )( implicit ec: ExecutionContext ): Future[Boolean] = trace.briefBlock( s"futureApplicablePlanExists( ${ts.topic} )" ) {
    planIndex.futureEntries map { entries =>
      log.debug(
        "PlanCatalog: safe look at {} plans for one that applies to topic:[{}] -- [{}]",
        entries.size, ts.topic, entries.values.mkString(", ")
      )

      entries.values exists { _ appliesTo ts }
    }
  }


  case object Initialize extends P.CatalogMessage
  override def preStart(): Unit = self ! Initialize

  var outstandingWork: Map[WorkId, ActorRef] = Map.empty[WorkId, ActorRef]

  override def receive: Receive = LoggingReceive { around( quiescent ) }

  val quiescent: Receive = {
    case Initialize => {
      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool
      implicit val timeout = Timeout( 30.seconds )

      val task = initializePlans() map { _ => log.info( "PlanCatalog[{}] initialization completed", self.path.name ) }
      scala.concurrent.Await.ready( task, timeout.duration )
      context become LoggingReceive { around( /*stream orElse*/ active orElse admin ) }
    }
  }

  val active: Receive = {
    case route @ P.Route( ts: TimeSeries, subscriber, cid ) if applicablePlanExists( ts )( context.dispatcher ) => {
      log.debug(
        "PlanCatalog:ACTIVE[{}] dispatching stream message for detection and result will go to subscriber[{}]: [{}]",
        workId,
        subscriber.path.name,
        route
      )
      dispatch( route, sender() )( context.dispatcher )
    }

    case ts: TimeSeries if applicablePlanExists( ts )( context.dispatcher ) => {
      log.debug( "PlanCatalog:ACTIVE[{}] dispatching time series to sender[{}]: [{}]", workId, sender().path.name, ts )
      val subscriber = sender()
      dispatch( P.Route(ts, subscriber, Some(correlationId)), subscriber )( context.dispatcher )
    }

    case r: P.Route => {
      //todo route unrecogonized ts
      log.warning( "PlanCatalog:ACTIVE[{}]: no plan on record that applies to topic:[{}]", self.path, r.timeSeries.topic )
      sender() !+ P.UnknownRoute( r )
    }

    case ts: TimeSeries => {
      //todo route unrecogonized ts
      log.warning( "PlanCatalog:ACTIVE[{}]: no plan on record that applies to topic:[{}]", self.path, ts.topic )
      val ref = sender()
      ref !+ P.UnknownRoute( ts, ref, Some(correlationId) )
    }

    case result @ DetectionResult( outliers, workIds ) => {
      workIds find { outstandingWork.contains } foreach { cid => outstandingWork( cid ) !+ result }
      outstandingWork --= workIds
    }
  }

  val admin: Receive = {
    case req @ P.GetPlansForTopic( topic ) => {
      import peds.akka.envelope.pattern.pipe

      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool

      planIndex.futureEntries
      .map { entries =>
        val ps = entries collect { case (_, p) if p appliesTo topic => p }
        log.debug( "PlanCatalog[{}]: for topic:[{}] returning plans:[{}]", self.path.name, topic, ps.mkString(", ") )
        P.CatalogedPlans( plans = ps.toSet, request = req )
      }
      .pipeEnvelopeTo( sender() )
    }

    case P.WaitForStart => sender() ! P.Started
  }


  private def dispatch( route: P.Route, interestedRef: ActorRef )( implicit ec: ExecutionContext ): Unit = {
    val cid = route.correlationId getOrElse outer.correlationId
    outstandingWork += ( cid -> interestedRef ) // keep this out of future closure or use an Agent to protect against race conditions

    for {
      model <- boundedContext.futureModel
      entries <- planIndex.futureEntries
    } {
      entries.values withFilter { _ appliesTo route.timeSeries } foreach { plan =>
        val planRef = model( AnalysisPlanModule.module.rootType, plan.id )
        log.debug(
          "PlanCatalog:DISPATCH[{}]: sending topic[{}] to plan-module[{}] with sender:[{}]",
          s"${self.path.name}:${workId}",
          route.timeSeries.topic,
          planRef.path.name,
          interestedRef.path.name
        )
        planRef !+ AP.AcceptTimeSeries( plan.id, Set(cid), route.timeSeries )
      }
    }
  }

  private def initializePlans()( implicit ec: ExecutionContext, timeout: Timeout ): Future[Done] = {
    for {
      entries <- planIndex.futureEntries
      ( registered, missing ) = outer.specifiedPlans partition { entries contains _.name }
      _ = log.info( "PlanCatalog[{}] registered plans=[{}]", self.path.name, registered.mkString(",") )
      _ = log.info( "PlanCatalog[{}] missing plans=[{}]", self.path.name, missing.mkString(",") )
      created <- makeMissingSpecifiedPlans( missing )
      recorded = created.keySet intersect entries.keySet
      remaining = created -- recorded
      _ <- fallbackIndex alter { plans => plans ++ remaining }
    } yield {
      log.info(
        "PlanCatalog[{}] created additional {} plan(s): [{}]",
        self.path.name,
        created.size,
        created.map{ case (n, c) => s"${n}: ${c}" }.mkString( "\n", "\n", "\n" )
      )

      log.info(
        "PlanCatalog[{}]: recorded new plans in index:[{}] remaining:[{}]",
        self.path.name, recorded.mkString(", "), remaining.mkString(", ")
      )

      log.info(
        "PlanCatalog[{}] index updated with additional plan(s): [{}]",
        self.path.name,
        created.map{ case (k, p) => s"${k}: ${p}" }.mkString( "\n", "\n", "\n" )
      )

      if ( remaining.nonEmpty ) {
        log.warning(
          "PlanCatalog[{}]: not all newly created plans have been recorded in index yet: [{}]",
          self.path.name,
          remaining.mkString( "\n", "\n", "\n" )
        )
      }

      Done
    }
  }

  def makeMissingSpecifiedPlans(
     missing: Set[OutlierPlan]
  )(
    implicit ec: ExecutionContext,
    to: Timeout
  ): Future[Map[String, OutlierPlan.Summary]] = {
    import demesne.module.entity.{ messages => EntityMessages }

    def loadSpecifiedPlans( model: DomainModel ): Seq[Future[(String, OutlierPlan.Summary)]] = {
      missing.toSeq.map { p =>
        log.info( "PlanCatalog[{}]: making plan entity for: [{}]", self.path, p )
        val planRef =  model( AnalysisPlanModule.module.rootType, p.id )

        for {
          added <- ( planRef ?+ EntityMessages.Add( p.id, Some(p) ) )
          _ = log.debug( "PlanCatalog: notified that plan is added:[{}]", added )
          loaded <- loadPlan( p.id )
          _ = log.debug( "PlanCatalog: loaded plan:[{}]", loaded )
        } yield loaded
      }
    }

    for {
      m <- boundedContext.futureModel
      loaded <- Future.sequence( loadSpecifiedPlans(m) )
    } yield Map( loaded:_* )
  }

  def loadPlan( planId: OutlierPlan#TID )( implicit ec: ExecutionContext, to: Timeout ): Future[(String, OutlierPlan.Summary)] = {
    fetchPlanInfo( planId ) map { summary => ( summary.info.name, summary.info.toSummary  ) }
  }

  def fetchPlanInfo( pid: AnalysisPlanModule.module.TID )( implicit ec: ExecutionContext, to: Timeout ): Future[AP.PlanInfo] = {
    def toInfo( message: Any ): Future[AP.PlanInfo] = {
      message match {
        case Envelope( info: AP.PlanInfo, _ ) => {
          log.info( "PlanCatalog[{}]: fetched plan entity: [{}]", self.path, info.sourceId )
          Future successful info
        }

        case info: AP.PlanInfo => Future successful info

        case m => {
          val ex = new IllegalStateException( s"unknown response to plan inquiry for id:[${pid}]: ${m}" )
          log.error( ex, "failed to fetch plan for id: {}", pid )
          Future failed ex
        }
      }
    }

    for {
      model <- boundedContext.futureModel
      ref = model( AnalysisPlanModule.module.rootType, pid )
      msg <- ( ref ?+ AP.GetPlan(pid) ).mapTo[Envelope]
      info <- toInfo( msg )
    } yield info
  }
}
