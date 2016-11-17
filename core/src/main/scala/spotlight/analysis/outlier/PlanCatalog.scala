package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.matching.Regex
import akka.{Done, NotUsed}
import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.{FlowShape, Materializer}
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, MaxInFlightRequestStrategy, RequestStrategy}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import akka.util.Timeout
import com.codahale.metrics.{Metric, MetricFilter}

import scalaz._
import Scalaz._
import com.typesafe.config.{Config, ConfigObject}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{MetricName, Timer}
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.StreamMonitor._
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
  case object WaitingForStart extends CatalogMessage
  case object Started extends CatalogMessage
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

  def flow(
    catalogProps: Props
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[TimeSeries, Outliers, NotUsed] = {
    val outletProps = CommonActorPublisher.props[DetectionResult]()

    val g = GraphDSL.create() { implicit b =>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val (outletRef, outlet) = {
        Source
        .actorPublisher[DetectionResult]( outletProps ).named( "PlanCatalogFlowOutlet" )
        .toMat( Sink.asPublisher(true) )( Keep.both )
        .run()
      }

      subscribers send { _ + outletRef }

      def flowLog[T]( label: String ): FlowShape[T, T] = {
        b.add( Flow[T] map { m => logger.debug( "PlanCatalog:FLOW: {}: [{}]", label, m.toString); m } )
      }

      val zipWithSubscriber = b.add( Flow[TimeSeries] map { ts => StreamMessage( ts, outletRef ) } )
      val planRouter = b.add( Sink.actorSubscriber[StreamMessage[TimeSeries]]( catalogProps ).named( "PlanCatalog" ) )
      zipWithSubscriber ~> flowLog[StreamMessage[TimeSeries]]("before-plan-router-") ~> planRouter

      val receiveOutliers = b.add(
        Source
        .fromPublisher[DetectionResult]( outlet ).named( "ResultCollector" )
        .map { m => logger.debug("TEST: view detection result:[{}]", m); m }
      )

      val getOutliers = b.add(
        Flow[DetectionResult]
        .map { m => logger.debug( "received result from collector: [{}]", m ); m }
        .map { _.outliers }
      )

      receiveOutliers ~> flowLog[DetectionResult]("collected-outliers") ~> getOutliers

      FlowShape( zipWithSubscriber.in, getOutliers.out )
    }

    Flow.fromGraph( g ).named( "PlanCatalogFlow" ).watchFlow( Symbol("CatalogDetection") )
  }

  val subscribers: Agent[Set[ActorRef]] = Agent( Set.empty[ActorRef] )( scala.concurrent.ExecutionContext.global )

  private[outlier] case class StreamMessage[P]( payload: P, subscriber: ActorRef )

  private[outlier] case class PlanRequest( subscriber: ActorRef, startNanos: Long = System.nanoTime() )


  trait Provider {
    def detectionBudget: FiniteDuration
    def specifiedPlans: Set[OutlierPlan]
    def maxInFlight: Int
  }


  object Default {
    val DetectionBudgetPath = "spotlight.workflow.detect.timeout"
    val OutlierPlansPath = "spotlight.detection-plans"
  }

  final case class Default private[PlanCatalog](
    _boundedContext: BoundedContext,
    configuration: Config,
    maxInFlightCpuFactor: Double = 8.0,
    applicationDetectionBudget: Option[FiniteDuration] = None,
    applicationPlans: Set[OutlierPlan] = Set.empty[OutlierPlan]
  ) extends PlanCatalog( _boundedContext ) with Provider {
    private val trace = Trace[Default]

    override val maxInFlight: Int = ( Runtime.getRuntime.availableProcessors() * maxInFlightCpuFactor ).toInt

    override lazy val detectionBudget: FiniteDuration = {
      applicationDetectionBudget getOrElse { loadDetectionBudget( configuration ) getOrElse 10.seconds }
    }

    private def loadDetectionBudget( specification: Config ): Option[FiniteDuration] = trace.block( "specifyDetectionBudget" ) {
      import Default.DetectionBudgetPath
      if ( specification hasPath DetectionBudgetPath ) {
        Some( FiniteDuration( specification.getDuration( DetectionBudgetPath, NANOSECONDS ), NANOSECONDS ) )
      } else {
        None
      }
    }

    //todo also load from plan index?
    override lazy val specifiedPlans: Set[OutlierPlan] = trace.block( "specifiedPlans" ) { applicationPlans ++ loadPlans( configuration ) }

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
      logger.info( "PlanCatalog plan speclet: [{}]", planSpecification )

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

class PlanCatalog( _boundedContext: BoundedContext )
extends ActorSubscriber with Stash with EnvelopingActor with InstrumentedActor with ActorLogging { outer: PlanCatalog.Provider =>
  import PlanCatalog.PlanRequest
  import spotlight.analysis.outlier.{ PlanCatalogProtocol => P }
  import spotlight.analysis.outlier.{ AnalysisPlanProtocol => AP }

  private val trace = Trace[PlanCatalog]

  var boundedContext: BoundedContext = _boundedContext

  override lazy val metricBaseName: MetricName = MetricName( classOf[PlanCatalog] )
  val catalogTimer: Timer = metrics timer "catalog"

  val outstandingMetricName: String = "outstanding"

  def initializeMetrics(): Unit = {
    stripLingeringMetrics()
    metrics.gauge( outstandingMetricName ) { outstandingRequests }
  }

  def stripLingeringMetrics(): Unit = {
    metrics.registry.removeMatching(
      new MetricFilter {
        override def matches( name: String, metric: Metric ): Boolean = {
          name.contains( classOf[PlanCatalog].getName ) && name.contains( outstandingMetricName )
        }
      }
    )
  }


  type PlanIndex = DomainModel.AggregateIndex[String, AnalysisPlanModule.module.TID, OutlierPlan.Summary]
  val planIndex: PlanIndex = {
    boundedContext
    .unsafeModel
    .aggregateIndexFor[String, AnalysisPlanModule.module.TID, OutlierPlan.Summary](
      AnalysisPlanModule.module.rootType,
      AnalysisPlanModule.namedPlanIndex
    ) match {
      case \/-( pindex ) => pindex
      case -\/( ex ) => {
        log.error( ex, "PlanCatalog[{}]: failed to initialize PlanCatalog's plan index", self.path )
        throw ex
      }
    }
  }

  def applicablePlanExists( ts: TimeSeries ): Boolean = trace.briefBlock( s"applicablePlanExists( ${ts.topic} )" ) {
    planIndex.entries.values exists { _ appliesTo ts }
  }

  //todo: associate timestamp with workId in order to reap tombstones
  var _workRequests: Map[WorkId, PlanRequest] = Map.empty[WorkId, PlanRequest]
  def outstandingRequests: Int = _workRequests.size
  def addWorkRequest( correlationId: WorkId, subscriber: ActorRef ): Unit = {
    _workRequests += ( correlationId -> PlanRequest(subscriber) )
  }

  def removeWorkRequests( correlationIds: Set[WorkId] ): Unit = {
    for {
      cid <- correlationIds
      knownRequest <- _workRequests.get( cid ).toSet
    } {
      catalogTimer.update( System.nanoTime() - knownRequest.startNanos, scala.concurrent.duration.NANOSECONDS )
    }

    _workRequests --= correlationIds
  }

  def knownWork: Set[WorkId] = _workRequests.keySet
  val isKnownWork: WorkId => Boolean = _workRequests.contains( _: WorkId )

  def hasWorkInProgress( workIds: Set[WorkId] ): Boolean = findOutstandingCorrelationIds( workIds ).nonEmpty

  def findOutstandingWorkRequests( correlationIds: Set[WorkId] ): Set[(WorkId, PlanRequest)] = {
    correlationIds.collect{ case cid => _workRequests.get( cid ) map { req => ( cid, req ) } }.flatten
  }

  def findOutstandingCorrelationIds( workIds: Set[WorkId] ): Set[WorkId] = trace.briefBlock( "findOutstandingCorrelationIds" ) {
    log.debug( "outstanding work: [{}]", knownWork )
    log.debug( "returning work: [{}]", workIds )
    log.debug( "known intersect: [{}]", knownWork intersect workIds )
    knownWork intersect workIds
  }

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy( outer.maxInFlight ) {
    override def inFlightInternally: Int = outstandingRequests
  }


  case object Initialize extends P.CatalogMessage
  override def preStart(): Unit = self ! Initialize

  var isWaitingToComplete: Boolean = false
  private def stopIfFullyComplete(): Unit = {
    if ( isWaitingToComplete ) {
      log.info(
        "PlanCatalog[{}] waiting to complete on [{}] outstanding work: [{}]",
        self.path.name,
        outstandingRequests,
        knownWork.mkString( ", " )
      )

      if ( outstandingRequests == 0 ) {
        log.info(
          "PlanCatalog[{}] is closing upon work completion...will notify {} subscribers",
          self.path.name,
          PlanCatalog.subscribers.get().size
        )

        import scala.concurrent.ExecutionContext.Implicits.global
        PlanCatalog.subscribers.future().foreach { subs =>
          subs foreach { s =>
            log.debug( "propagating OnComplete to subscriber:[{}]", s.path.name )
            s ! ActorSubscriberMessage.OnComplete
          }
        }

        context stop self

        log.error( "PlanCatalog[{}] SIMULATED closing upon completion", self.path.name )
      }
    }
  }


  override def receive: Receive = LoggingReceive { around( quiescent ) }

  val quiescent: Receive = {
    case Initialize => {
      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool

      initializeMetrics()

      postStartInitialization() foreach { _ =>
        log.info( "PlanCatalog[{}] initialization completed", self.path )
        context become LoggingReceive { around( stream orElse active orElse admin ) }
      }
    }
  }

  import PlanCatalog.StreamMessage

  val active: Receive = {
    case m @ StreamMessage( ts: TimeSeries, subscriber ) if applicablePlanExists( ts ) => {
      log.debug(
        "PlanCatalog:ACTIVE[{}] dispatching stream message for detection and result will go to subscriber[{}]: [{}]",
        workId,
        subscriber.path.name,
        m
      )
      dispatch( ts, subscriber )( context.dispatcher )
      stopIfFullyComplete()
    }

    case StreamMessage( ts: TimeSeries, _ ) => {
      //todo route unrecogonized ts
      log.warning( "PlanCatalog:ACTIVE[{}]: no plan on record that applies to time-series:[{}]", self.path, ts.topic )
      stopIfFullyComplete()
    }

    case ts: TimeSeries if applicablePlanExists( ts ) => {
      log.debug( "PlanCatalog:ACTIVE[{}] dispatching time series to sender[{}]: [{}]", workId, sender().path.name, ts )
      dispatch( ts, sender() )( context.dispatcher )
      stopIfFullyComplete()
    }

    case ts: TimeSeries => {
      //todo route unrecogonized ts
      log.warning( "PlanCatalog:ACTIVE[{}]: no plan on record that applies to time-series:[{}]", self.path, ts.topic )
      stopIfFullyComplete()
    }

    case result: DetectionResult if !hasWorkInProgress( result.correlationIds ) => {
      log.error(
        "PlanCatalog:ACTIVE[{}]: stashing received result for UNKNOWN workId:[{}] all-ids:[{}]: [{}]",
        self.path.name,
        workId,
        knownWork.mkString(", "),
        result
      )

      stash()
      stopIfFullyComplete()
    }

    case result @ DetectionResult( outliers, workIds ) => {
      log.debug(
        "PlanCatalog:ACTIVE[{}]: received outlier result[{}] for workId:[{}] subscriber:[{}]",
        self.path,
        outliers.hasAnomalies,
        workId,
        _workRequests.get( workId ).map{ _.subscriber.path.name }
      )
      log.debug( "PlanCatalog:ACTIVE: received (workId:[{}] -> subscriber:[{}])  all:[{}]", workId, _workRequests.get(workId), _workRequests.mkString(", ") )

      val (known, unknown) = result.correlationIds partition isKnownWork
      if ( unknown.nonEmpty ) {
        log.warning(
          "PlanCatalog:ACTIVE received topic:[{}] algorithms:[{}] results for unrecognized workIds:[{}]",
          result.outliers.topic,
          result.outliers.algorithms.map{ _.name }.mkString( ", " ),
          unknown.mkString(", ")
        )
      }

      val requests = findOutstandingWorkRequests( known )
      removeWorkRequests( result.correlationIds )
      log.debug(
        "PlanCatalog:ACTIVE sending result to {} subscriber{}",
        requests.size,
        if ( requests.size == 1 ) "" else "s"
      )
      requests foreach { case (_, r) => r.subscriber ! result }
      log.debug( "PlanCatalog:ACTIVE unstashing" )
      unstashAll()
      stopIfFullyComplete()
    }
  }

  val stream: Receive = {
    case OnNext( message ) if active isDefinedAt message => {
      log.debug( "PlanCatalog:STREAM: evaluating OnNext( {} )", message )
      active( message )
    }

    case OnComplete => {
      log.info( "PlanCatalog:STREAM[{}] closing on completed stream", self.path.name )
      isWaitingToComplete = true
      stopIfFullyComplete()
    }

    case OnError( ex ) => {
      log.error( ex, "PlanCatalog:STREAM closing on errored stream" )
      context stop self
    }
  }

  val admin: Receive = {
    case req @ P.GetPlansForTopic( topic ) => {
      import peds.akka.envelope.pattern.pipe

      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool

      planIndex.futureEntries
      .map { entries =>
        val ps = entries collect { case (_, p) if p appliesTo topic => p }
        log.debug( "PlanCatalog[{}]: for topic:[{}] returning plans:[{}]", self.path, topic, ps.mkString(", ") )
        log.debug( "TEST: IN GET-PLANS specified-plans:[{}]", specifiedPlans.mkString(", ") )
        P.CatalogedPlans( plans = ps.toSet, request = req )
      }
      .pipeEnvelopeTo( sender() )
    }

    case P.WaitingForStart => sender() ! P.Started
  }


  private def dispatch( ts: TimeSeries, subscriber: ActorRef )( implicit ec: ExecutionContext ): Unit = {
    // pulled up future dependencies so I'm not closing over this
    val correlationId = workId
    val entries = planIndex.entries
    addWorkRequest( correlationId, subscriber )

    for {
      model <- boundedContext.futureModel
    } {
      entries.values withFilter { _ appliesTo ts } foreach { plan =>
        val planRef = model( AnalysisPlanModule.module.rootType, plan.id )
        log.debug(
          "PlanCatalog:DISPATCH[{}]: sending topic[{}] to plan-module[{}] with sender:[{}]",
          s"${self.path.name}:${correlationId}",
          ts.topic,
          planRef.path,
          subscriber.path
        )
        planRef !+ AP.AcceptTimeSeries( plan.id, Set(correlationId), ts )
      }
    }
  }



  private def postStartInitialization(): Future[Done] = {
    implicit val ec = context.system.dispatcher
    implicit val timeout = Timeout( 30.seconds )

    for {
      entries <- planIndex.futureEntries
      ( registered, missing ) = outer.specifiedPlans partition { entries contains _.name }
      _ = log.info( "PlanCatalog[{}] registered plans=[{}]", self.path.name, registered.mkString(",") )
      _ = log.info( "PlanCatalog[{}] missing plans=[{}]", self.path.name, missing.mkString(",") )
      created <- makeMissingSpecifiedPlans( missing )
    } yield {
      log.info(
        "PlanCatalog[{}] created additional {} plans: [{}]",
        self.path,
        created.size,
        created.map{ case (n, c) => s"${n}: ${c}" }.mkString( "\n", "\n", "\n" )
      )

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
    } yield {
      log.info( "TEST: PlanCatalog loaded plans: [{}]", loaded.map{ case (n,p) => s"$n->$p" }.mkString(",") )
      Map( loaded:_* )
    }
  }

  def loadPlan( planId: OutlierPlan#TID )( implicit ec: ExecutionContext, to: Timeout ): Future[(String, OutlierPlan.Summary)] = {
    fetchPlanInfo( planId ) map { summary => ( summary.info.name, summary.info.toSummary  ) }
  }

  def fetchPlanInfo( pid: AnalysisPlanModule.module.TID )( implicit ec: ExecutionContext, to: Timeout ): Future[AP.PlanInfo] = {
    def toInfo( message: Any ): Future[AP.PlanInfo] = {
      message match {
        case Envelope( info: AP.PlanInfo, _ ) => {
          log.info( "PlanCataloge[{}]: fetched plan entity: [{}]", self.path, info.sourceId )
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
