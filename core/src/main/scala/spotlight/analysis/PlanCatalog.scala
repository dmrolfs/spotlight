package spotlight.analysis

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.matching.Regex
import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash, Status}
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.pattern.{ask, pipe}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.scaladsl.{EventsByTagQuery, EventsByTagQuery2, ReadJournal}
import akka.persistence.query.{EventEnvelope2, NoOffset, PersistenceQuery}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, FlowShape, Materializer}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Merge, Sink, Source}
import akka.util.Timeout

import scalaz._
import Scalaz._
import com.typesafe.config.{Config, ConfigObject}
import com.persist.logging._
import nl.grons.metrics.scala.MetricName
import peds.akka.envelope._
import peds.akka.envelope.pattern.{ask => envAsk}
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.CommonActorPublisher
import demesne.{BoundedContext, DomainModel}
import spotlight.Settings
import spotlight.analysis.OutlierDetection.DetectionResult
import spotlight.analysis.PlanCatalog.WatchPoints
import spotlight.model.outlier.{AnalysisPlan, IsQuorum, Outliers, ReduceOutliers}
import spotlight.model.timeseries.{TimeSeries, Topic}
import sun.security.pkcs11.Secmod.Module


/**
  * Created by rolfsd on 5/20/16.
  */
object PlanCatalogProtocol {
  sealed trait CatalogMessage

  case object WaitForStart extends CatalogMessage
  case object Started extends CatalogMessage

  case class MakeFlow(
    parallelism: Int,
    system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ) extends CatalogMessage
  case class CatalogFlow( flow: DetectFlow ) extends CatalogMessage

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
    implicit boundedContext: BoundedContext
  ): Props = {
    Props( new Default(boundedContext, configuration, applicationPlans, maxInFlightCpuFactor, applicationDetectionBudget) )
    // .withDispatcher( "spotlight.planCatalog.stash-dispatcher" )
  }

  val name: String = "PlanCatalog"

  def flow(
    catalogRef: ActorRef,
    parallelism: Int
  )(
    implicit system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ): Future[Flow[TimeSeries, Outliers, NotUsed]] = {
    import spotlight.analysis.{PlanCatalogProtocol => P}
    implicit val ec = system.dispatcher

    for {
      _ <- ( catalogRef ? P.WaitForStart )
      cf <- ( catalogRef ? P.MakeFlow( parallelism, system, timeout, materializer ) ).mapTo[P.CatalogFlow]
    } yield { cf.flow }
  }


  object WatchPoints {
    val Catalog = 'catalog
    val Intake = Symbol( "catalog.intake" )
    val Collector = Symbol( "catalog.collector" )
    val Outlet = Symbol( "catalog.outlet" )
  }


  private[analysis] case class PlanRequest( subscriber: ActorRef, startNanos: Long = System.nanoTime() )


  trait PlanProvider {
    def specifiedPlans: Set[AnalysisPlan]
  }

  trait ExecutionProvider {
    def detectionBudget: Duration
    def maxInFlight: Int
    def correlationId: WorkId
  }

  trait DefaultExecutionProvider extends ExecutionProvider { outer: EnvelopingActor with ActorLogging =>
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
        log.warn( Map("@msg" -> "value for message workId / correlationId is UNKNOWN", "set-work-id" -> workId.toString()) )
        workId
      }
    }
  }


  final class Default private[PlanCatalog](
    boundedContext: BoundedContext,
    override val configuration: Config,
    override val specifiedPlans: Set[AnalysisPlan],
    override val maxInFlightCpuFactor: Double = 8.0,
    override val applicationDetectionBudget: Option[Duration] = None
  ) extends PlanCatalog( boundedContext ) with DefaultExecutionProvider with PlanProvider {
    log.debug(Map( "@msg" -> "#TEST PlanCatalog init", "specified-plans" -> specifiedPlans.map(_.name).mkString("[", ", ", "]")))
    //todo also load from plan index?
//    override lazy val specifiedPlans: Set[AnalysisPlan] = applicationPlans // ++ loadPlans( configuration ) //todo: loadPlans is duplicative since Settings provides to props()

//    private def loadPlans( specification: Config ): Set[AnalysisPlan] = {
//      val planSpecs = Settings detectionPlansConfigFrom specification
//      if ( planSpecs.nonEmpty ) {
//        log.debug( "specification = [{}]", planSpecs.root().render() )
//        import scala.collection.JavaConversions._
//        // since config is java API needed for collect
//
//        planSpecs.root
//        .collect { case (n, s: ConfigObject) => (n, s.toConfig) }
//        .toSet[(String, Config)]
//        .map { case (name, spec) => makePlan( name, spec )( detectionBudget ) }
//        .flatten
//      } else {
//        log.warning( "[{}]: no plan specifications found in settings", self.path )
//        Set.empty[AnalysisPlan]
//      }
//    }

//    private def makePlan( name: String, planSpecification: Config )( budget: Duration ): Option[AnalysisPlan] = {
//      logger.info(
//        "PlanCatalog[{}] making plan from specification[origin:{} @ {}]: [{}]",
//        self.path.name, planSpecification.origin(), planSpecification.origin().lineNumber().toString, planSpecification
//      )
//
//      //todo: bring initialization of plans into module init and base config on init config?
//      val utilization = 0.9
//      val timeout = budget * utilization
//
//      val grouping: Option[AnalysisPlan.Grouping] = {
//        val GROUP_LIMIT = "group.limit"
//        val GROUP_WITHIN = "group.within"
//        val limit = if ( planSpecification hasPath GROUP_LIMIT ) planSpecification getInt GROUP_LIMIT else 10000
//        val window = if ( planSpecification hasPath GROUP_WITHIN ) {
//          Some( FiniteDuration( planSpecification.getDuration( GROUP_WITHIN ).toNanos, NANOSECONDS ) )
//        } else {
//          None
//        }
//
//        window map { w => AnalysisPlan.Grouping( limit, w ) }
//      }
//
//      //todo: add configuration for at-least and majority
//      val algorithms = planAlgorithms( planSpecification )
//
//      val IS_DEFAULT = "is-default"
//      val TOPICS = "topics"
//      val REGEX = "regex"
//
//      if ( planSpecification.hasPath(IS_DEFAULT) && planSpecification.getBoolean(IS_DEFAULT) ) {
//        logger.info(
//          "PlanCatalog: topic[{}] default-type plan specification origin:[{}] line:[{}]",
//          name,
//          planSpecification.origin,
//          planSpecification.origin.lineNumber.toString
//        )
//
//        Some(
//          AnalysisPlan.default(
//            name = name,
//            timeout = timeout,
//            isQuorum = makeIsQuorum( planSpecification, algorithms.size ),
//            reduce = makeOutlierReducer( planSpecification),
//            algorithms = algorithms,
//            grouping = grouping,
//            planSpecification = planSpecification
//          )
//        )
//      } else if ( planSpecification hasPath TOPICS ) {
//        import scala.collection.JavaConverters._
//        logger.info(
//          "PlanCatalog: topic:[{}] topic-based plan specification origin:[{}] line:[{}]",
//          name,
//          planSpecification.origin,
//          planSpecification.origin.lineNumber.toString
//        )
//
//        Some(
//          AnalysisPlan.forTopics(
//            name = name,
//            timeout = timeout,
//            isQuorum = makeIsQuorum( planSpecification, algorithms.size ),
//            reduce = makeOutlierReducer( planSpecification ),
//            algorithms = algorithms,
//            grouping = grouping,
//            planSpecification = planSpecification,
//            extractTopic = OutlierDetection.extractOutlierDetectionTopic,
//            topics = planSpecification.getStringList( TOPICS ).asScala.map{ Topic(_) }.toSet
//          )
//        )
//      } else if ( planSpecification hasPath REGEX ) {
//        logger.info(
//          "PlanCatalog: topic:[{}] regex-based plan specification origin:[{}] line:[{}]",
//          name,
//          planSpecification.origin,
//          planSpecification.origin.lineNumber.toString
//        )
//
//        Some(
//          AnalysisPlan.forRegex(
//            name = name,
//            timeout = timeout,
//            isQuorum = makeIsQuorum( planSpecification, algorithms.size ),
//            reduce = makeOutlierReducer( planSpecification ),
//            algorithms = algorithms,
//            grouping = grouping,
//            planSpecification = planSpecification,
//            extractTopic = OutlierDetection.extractOutlierDetectionTopic,
//            regex = new Regex( planSpecification getString REGEX )
//          )
//        )
//      } else {
//        None
//      }
//    }

//    private def planAlgorithms( spec: Config ): Set[Symbol] = {
//      import scala.collection.JavaConversions._
//      val AlgorithmsPath = "algorithms"
//      if ( spec hasPath AlgorithmsPath ) spec.getStringList( AlgorithmsPath ).toSet.map{ a: String => Symbol( a ) }
//      else Set.empty[Symbol]
//    }
//
//    private def makeIsQuorum( spec: Config, algorithmSize: Int ): IsQuorum = {
//      val MAJORITY = "majority"
//      val AT_LEAST = "at-least"
//      if ( spec hasPath AT_LEAST ) {
//        val trigger = spec getInt AT_LEAST
//        IsQuorum.AtLeastQuorumSpecification( totalIssued = algorithmSize, triggerPoint = trigger )
//      } else {
//        val trigger = if ( spec hasPath MAJORITY ) spec.getDouble( MAJORITY ) else 50D
//        IsQuorum.MajorityQuorumSpecification( totalIssued = algorithmSize, triggerPoint = ( trigger / 100D) )
//      }
//    }
//
//    private def makeOutlierReducer( spec: Config ): ReduceOutliers = {
//      val AT_LEAST = "at-least"
//      if ( spec hasPath AT_LEAST ) {
//        val threshold = spec getInt AT_LEAST
//        ReduceOutliers.byCorroborationCount( threshold )
//      } else {
//        val MAJORITY = "majority"
//        val threshold = if ( spec hasPath MAJORITY ) spec.getDouble( MAJORITY ) else 50D
//        ReduceOutliers.byCorroborationPercentage( threshold )
//      }
//    }
  }


  case object NoRegisteredPlansError extends IllegalStateException( "Cannot create detection model without registered plans" )
}

abstract class PlanCatalog( boundedContext: BoundedContext )
extends Actor with Stash with EnvelopingActor with InstrumentedActor with ActorLogging {
  outer: PlanCatalog.ExecutionProvider with PlanCatalog.PlanProvider =>

  import spotlight.analysis.{ PlanCatalogProtocol => P, AnalysisPlanProtocol => AP }

  override lazy val metricBaseName: MetricName = MetricName( classOf[PlanCatalog] )

  case object Initialize extends P.CatalogMessage
  case object InitializeCompleted extends P.CatalogMessage
  override def preStart(): Unit = self ! Initialize


  val fallbackIndex: Agent[Map[String, AnalysisPlan.Summary]] = {
    Agent( Map.empty[String, AnalysisPlan.Summary] )( scala.concurrent.ExecutionContext.global )
  }


  def indexedPlans: Future[Set[AnalysisPlan.Summary]] = {
    //    planIndex.futureEntries map { _.values.toSet }

    import spotlight.analysis.{ AnalysisPlanProtocol => AP }

    implicit val ec = context.dispatcher
    implicit val materializer = ActorMaterializer( ActorMaterializerSettings(context.system) )

    //    planIndex.futureEntries map { _.values.toSet }
    AnalysisPlanModule
    .queryJournal( context.system )
    .currentEventsByTag( AnalysisPlanModule.module.rootType.name, NoOffset )
    .map { e => log.warn(Map("@msg" -> "#TEST tagged event", "event" -> e.toString)); e }
    .collect {
      case EventEnvelope2( offset, pid, snr, event: AP.Added ) => {
        log.warn(Map("@msg"->"#TEST READ Plan Added", "offset"->offset.toString, "pid"->pid, "sequence-nr"->snr))
        event
      }
      case EventEnvelope2( offset, pid, snr, event: AP.Disabled ) => {
        log.warn(Map("@msg"->"#TEST READ Plan Disabled", "offset"->offset.toString, "pid"->pid, "sequence-nr"->snr))
        event
      }
      case EventEnvelope2( offset, pid, snr, event: AP.Enabled ) => {
        log.warn(Map("@msg"->"#TEST READ Plan Enabled", "offset"->offset.toString, "pid"->pid, "sequence-nr"->snr))
        event
      }
      case EventEnvelope2( offset, pid, snr, event: AP.Renamed ) => {
        log.warn(Map("@msg"->"#TEST READ Plan Renamed", "offset"->offset.toString, "pid"->pid, "sequence-nr"->snr))
        event
      }
    }
    .fold( Set.empty[AnalysisPlan.Summary] ) {
      case (ps, AP.Added(_, Some(p: AnalysisPlan))) => {
        val r = ps + p.toSummary
        log.warn(Map("@msg"->"#TEST folding Added into set", "plan-name"->p.name, "plan-id"->p.id.toString))
        r
      }
      case (ps, e: AP.Disabled) => {
        val r = ps.find { _.id == e.sourceId } map { ps + _.copy( isActive = false ) } getOrElse { ps }
        log.warn(Map("@msg"->"#TEST folding Disabled into set", "plan-id"->e.sourceId.toString))
        r
      }
      case (ps, e: AP.Enabled) => {
        val r = ps.find { _.id == e.sourceId } map { ps + _.copy( isActive = true ) } getOrElse { ps }
        log.warn(Map("@msg"->"#TEST folding Enabled into set", "plan-id"->e.sourceId.toString))
        r
      }
      case (ps, e: AP.Renamed) => {
        val r = ps.find { _.id == e.sourceId } map { ps + _.copy( name = e.newName ) } getOrElse { ps }
        log.warn(Map("@msg"->"#TEST folding Renamed into set", "plan-id"->e.sourceId.toString))
        r
      }
    }
    .map { _ filter { p => log.warn(Map("@msg"->"#TEST filtering for active plans", "plan"->Map("name"->p.name, "id"->p.id.id.toString, "is-active"->p.isActive))); p.isActive } }
    .runFold( Set.empty[AnalysisPlan.Summary] ){ case (as, ps) =>
      log.warn(Map("@msg"->"#TEST final sink", "plans"->ps.mkString("[", ", ", "]"), "result"->as.mkString("[", ", ", "]")))
      as ++ ps
    }
  }

//todo WHY ARENT PLAN BEING LOADED???
  type PlanIndex = DomainModel.AggregateIndex[String, AnalysisPlanModule.module.TID, AnalysisPlan.Summary]
//  lazy val planIndex: PlanIndex = {
//    implicit val dispatcher = context.dispatcher
//    val index = boundedContext.futureModel map { model =>
//      model.aggregateIndexFor[String, AnalysisPlanModule.module.TID, AnalysisPlan.Summary](
//        AnalysisPlanModule.module.rootType,
//        AnalysisPlanModule.namedPlanIndex
//      ) match {
//        case \/-( pi ) => pi
//        case -\/( ex ) => {
//          log.error( "failed to initialize catalog's plan index", ex )
//          throw ex
//        }
//      }
//    }
//
//    scala.concurrent.Await.result( index, 30.seconds )
//  }

  def collectPlans()( implicit ec: ExecutionContext ): Future[Set[AnalysisPlan.Summary]] = {
    for {
      fromIndex <- indexedPlans
      _ = log.debug(Map("@msg" -> "PlanCatalog fromIndex", "index" -> fromIndex.toString))
      fromFallback <- fallbackIndex.map{ _.values.toSet }.future()
      _ = log.debug(Map("@msg" -> "PlanCatalog fromFallback", "fallback" -> fromFallback.toString))
    } yield fromIndex ++ fromFallback
  }

  def applicablePlanExists( ts: TimeSeries )( implicit ec: ExecutionContext ): Boolean = {
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
              Map( "@msg" -> "delayed appearance of topic in plan index - removing from fallback", "topic" -> ts.topic.toString )
            )
            fallbackIndex send { _ - ts.topic.toString }
            futureCheck
          } else {
            val fallbackResult = fallback contains ts.topic.toString
            if ( fallbackResult ) {
              log.warn(
                Map( "@msg" -> "fallback plan found for topic is not yet reflected in plan index", "topic" -> ts.topic.toString )
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
    p.appliesTo map { _.apply(o) } getOrElse true
  }

  def unsafeApplicablePlanExists( ts: TimeSeries ): Boolean = {
    implicit val ec = context.dispatcher

    log.debug(
      Map(
        "@msg" -> "unsafe look at plans for one that applies to topic",
        "index-size" -> Await.result(indexedPlans.map(_.size), 3.seconds).toString,
        "topic" -> ts.topic.toString,
        "indexed-plans" -> Await.result(indexedPlans.map(_.mkString("[", ", ", "]")), 3.seconds)
      )
    )
    val applyTest = doesPlanApply( ts )( _: AnalysisPlan.Summary )
    Await.result( indexedPlans.map( _.exists( applyTest ) ), 3.seconds )
  }

  def futureApplicablePlanExists( ts: TimeSeries )( implicit ec: ExecutionContext ): Future[Boolean] = {
    indexedPlans map { entries =>
      log.debug(
        Map(
          "@msg" -> "safe look at plans for one that applies to topic",
          "index-size" -> entries.size,
          "topic" -> ts.topic.toString,
           "indexed-plans" -> entries.mkString("[", ", ", "]")
        )
      )

      val applyTest = doesPlanApply( ts )( _: AnalysisPlan.Summary )
      entries exists { applyTest }
    }
  }


  var outstandingWork: Map[WorkId, ActorRef] = Map.empty[WorkId, ActorRef]

  override def receive: Receive = LoggingReceive { around( quiescent() ) }

  def quiescent( waiting: Set[ActorRef] = Set.empty[ActorRef] ): Receive = {
    case Initialize => {
      import akka.pattern.pipe
      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool
      implicit val timeout = Timeout( 30.seconds )
      initializePlans() map { _ => InitializeCompleted } pipeTo self
    }

    case Status.Failure( ex ) => {
      log.error( Map("@msg" -> "failed to initialize plans", "waiting" -> waiting.map(_.path.name).mkString("[", ", ", "]")), ex )
      throw ex
    }

    case InitializeCompleted => {
      log.info( "initialization completed" )
      waiting foreach { _ ! P.Started }
      context become LoggingReceive { around( active orElse admin ) }
    }

    case P.WaitForStart => {
      log.debug( Map( "@msg" -> "received WaitForStart request - adding to waiting queue", "sender" -> sender.path.name ) )
      context become LoggingReceive { around( quiescent( waiting + sender() ) ) }
    }
  }

  val active: Receive = {
    case P.MakeFlow( parallelism, system, timeout, materializer ) => {
      implicit val ec = context.dispatcher
      val requester = sender()
      makeFlow( parallelism )( system, timeout, materializer ) map { P.CatalogFlow.apply } pipeTo requester
    }


    case route @ P.Route( ts: TimeSeries, _ ) if applicablePlanExists( ts )( context.dispatcher ) => {
      log.debug( 
        Map(
          "@msg" -> "PlanCatalog:ACTIVE dispatching stream message for detection", 
          "work-id" -> workId.toString, 
          "route" -> route.toString 
        )
      )
      dispatch( route, sender() )( context.dispatcher )
    }

    case ts: TimeSeries if applicablePlanExists( ts )( context.dispatcher ) => {
      log.debug( 
        Map(
          "@msg" -> "PlanCatalog:ACTIVE dispatching time series to sender", 
          "work-id" -> workId, 
          "sender" -> sender().path.name, 
          "topic" -> ts.topic.toString 
        )
      )
      dispatch( P.Route(ts, Some(correlationId)), sender() )( context.dispatcher )
    }

    case r: P.Route => {
      //todo route unrecogonized ts
      log.warn(
        Map( "@msg" -> "PlanCatalog:ACTIVE: no plan on record that applies to topic", "topic" -> r.timeSeries.topic.toString )
      )
      sender() !+ P.UnknownRoute( r )
    }

    case ts: TimeSeries => {
      //todo route unrecogonized ts
      log.warn(
        Map( "@msg" -> "PlanCatalog:ACTIVE: no plan on record that applies to topic", "topic" -> ts.topic.toString )
      )
      sender() !+ P.UnknownRoute( ts, Some(correlationId) )
    }

    case result @ DetectionResult( outliers, workIds ) => {
      if ( 1 < workIds.size ) {
        log.warn(
          Map(
            "@msg" -> "received DetectionResult with multiple workIds", 
            "nr-work-id" -> workIds.size, 
            "work-ids" -> workIds.mkString("[", ", ", "]")
          )
        )
      }

      workIds find { outstandingWork.contains } foreach { cid =>
        val subscriber = outstandingWork( cid )
        log.debug( Map("@msg" -> "FLOW2: sending result to subscriber", "subscriber" -> subscriber.path.name, "result" -> result.toString ) )
        subscriber !+ result
      }

      outstandingWork --= workIds
    }
  }

  val admin: Receive = {
    case req @ P.GetPlansForTopic( topic ) => {
      import peds.akka.envelope.pattern.pipe

      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool

      indexedPlans
      .map { entries =>
        val ps = entries filter { doesPlanApply(topic) }
        log.debug(
          Map(
            "@msg" -> "PlanCatalog: for topic returning plans", 
            "topic" -> topic.toString, 
            "plans" -> ps.map(_.name).mkString("[", ", ", "]")
          )
        )
        P.CatalogedPlans( plans = ps.toSet, request = req )
      }
      .pipeEnvelopeTo( sender() )
    }

    case P.WaitForStart => sender() ! P.Started
  }

  def makeFlow(
    parallelism: Int
  )(
    implicit system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ): Future[DetectFlow] = {
    import peds.akka.stream.StreamMonitor._

    implicit val ec = context.dispatcher
    def collectPlanFlows( model: DomainModel, plans: Set[AnalysisPlan.Summary] ): Future[Set[DetectFlow]] = {
      Future sequence {
        plans map { p =>
          val ref = model( AnalysisPlanModule.module.rootType, p.id )

          ( ref ?+ AP.MakeFlow( p.id, parallelism, system, timeout, materializer ) )
          .mapTo[AP.AnalysisFlow]
          .map { af =>
            log.debug(
              Map( "@msg" -> "PlanCatalog: created analysis flow for", "plan" -> Map("id" -> p.id.id.toString, "name" -> p.name) )
            )
            af.flow
          }
        }
      }
    }

    def detectFrom( planFlows: Set[DetectFlow] ): Future[DetectFlow] = {
      val nrFlows = planFlows.size
      log.debug( Map("@msg" -> "making PlanCatalog graph with [plans", "nr-plans" -> nrFlows) )

      val graph = Future {
        GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._

          val intake = b.add( Flow[TimeSeries].map{ identity }/*.watchFlow( WatchPoints.Intake )*/ )
          val outlet = b.add( Flow[Outliers].map{ identity }/*.watchFlow( WatchPoints.Outlet )*/ )

          if ( planFlows.nonEmpty ) {
            val broadcast = b.add( Broadcast[TimeSeries]( nrFlows ) )
            val merge = b.add( Merge[Outliers]( nrFlows ) )

            intake ~> broadcast.in
            planFlows.zipWithIndex foreach { case (pf, i) =>
              log.debug( Map("@msg" -> "adding to catalog flow, order undefined analysis flow", "index"->i) )
              val flow = b.add( pf )
              broadcast.out( i ) ~> flow ~> merge.in( i )
            }
            merge.out ~> outlet
          } else {
            throw PlanCatalog.NoRegisteredPlansError
          }

          FlowShape( intake.in, outlet.out )
        }
      }

      graph map { g => Flow.fromGraph( g ).named( "PlanCatalogFlow" ).watchFlow( WatchPoints.Catalog ) }
    }

    for {
      model <- boundedContext.futureModel
      plans <- collectPlans()
      _ = log.debug( Map("@msg"-> "collect plans", "plans" -> plans.toSeq.map{ p => (p.name, p.id) }.mkString("[", ", ", "]")) )
      planFlows <- collectPlanFlows( model, plans )
      f <- detectFrom( planFlows )
    } yield f
  }



  private def dispatch( route: P.Route, interestedRef: ActorRef )( implicit ec: ExecutionContext ): Unit = {
    val cid = route.correlationId getOrElse outer.correlationId
    outstandingWork += ( cid -> interestedRef ) // keep this out of future closure or use an Agent to protect against race conditions

    for {
      model <- boundedContext.futureModel
      entries <- indexedPlans
    } {
      val applyTest = doesPlanApply(route.timeSeries)( _: AnalysisPlan.Summary )
      entries withFilter { applyTest } foreach { plan =>
        val planRef = model( AnalysisPlanModule.module.rootType, plan.id )
        log.debug(
          Map(
            "@msg" -> "DISPATCH: sending topic to plan-module with sender",
            "topic" -> route.timeSeries.topic.toString,
            "plan-module" -> planRef.path.name,
            "sender" -> interestedRef.path.name
          )
        )
        planRef !+ AP.AcceptTimeSeries( plan.id, Set(cid), route.timeSeries )
      }
    }
  }

  private def initializePlans()( implicit ec: ExecutionContext, timeout: Timeout ): Future[Done] = {
    for {
//      entries <- planIndex.futureEntries
      entries <- indexedPlans //todo dmr
      entryNames = entries map { _.name }
      ( registered, missing ) = outer.specifiedPlans partition { entryNames contains _.name }
      _ = log.info(Map("@msg" -> "registered plans", "plans" -> registered.map(_.name).mkString("[", ", ", "]")))
      _ = log.info(Map("@msg" -> "missing plans", "missing-plans" -> missing.map(_.name).mkString("[", ", ", "]")))
      created <- makeMissingSpecifiedPlans( missing )
      recorded = created.keySet intersect entryNames
      remaining = created -- recorded
      _ <- fallbackIndex alter { plans => plans ++ remaining }
    } yield {
      log.info(
        Map(
          "@msg" -> "created additional plans",
          "nr-created" -> created.size,
          "created" -> created.map{ case (n, c) => s"${n}: ${c}" }.mkString("[", ", ", "]")
        )
      )

      log.info(
        Map(
          "@msg" -> "recorded new plans in index",
          "recorded" -> recorded.mkString("[", ", ", "]"),
          "remaining" -> remaining.map(_._2.name).mkString("[", ", ", "]")
        )
      )

      log.info(
        Map(
          "@msg" -> "index updated with additional plan(s)",
          "plans" -> created.map{ case (k, p) => s"${k}: ${p}" }.mkString("[", ", ", "]")
        )
      )

      if ( remaining.nonEmpty ) {
        log.warn(
          Map( 
            "@msg" -> "not all newly created plans have been recorded in index yet",
            "remaining" -> remaining.map(_._2.name).mkString("[", ", ", "]")
          )
        )
      }

      Done
    }
  }

  def makeMissingSpecifiedPlans(
     missing: Set[AnalysisPlan]
  )(
    implicit ec: ExecutionContext,
    to: Timeout
  ): Future[Map[String, AnalysisPlan.Summary]] = {
    def loadSpecifiedPlans( model: DomainModel ): Seq[Future[(String, AnalysisPlan.Summary)]] = {
      missing.toSeq.map { p =>
        log.info( Map("@msg" -> "making plan entity", "entry" -> p.name) )
        val planRef =  model( AnalysisPlanModule.module.rootType, p.id )

        for {
          Envelope( added: AP.Added, _ ) <- ( planRef ?+ AP.Add( p.id, Some(p) ) ).mapTo[Envelope]
          _ = log.debug(Map("@msg" -> "notified that plan is added", "added" -> added.info.toString))
          loaded <- loadPlan( p.id )
          _ = log.debug(Map("@msg" -> "loaded plan", "plan" -> loaded._2.name))
        } yield loaded
      }
    }

    val made = {
      for {
        m <- boundedContext.futureModel
        loaded <- Future.sequence( loadSpecifiedPlans(m) )
      } yield Map( loaded:_* )
    }

    made.transform(
      identity,
      ex => {
        log.error( Map("@msg" -> "failed to make missing plans", "missing" -> missing.map(_.name).mkString("[", ", ", "]")), ex )
        ex
      }
    )
  }

  def loadPlan( planId: AnalysisPlan#TID )( implicit ec: ExecutionContext, to: Timeout ): Future[(String, AnalysisPlan.Summary)] = {
    fetchPlanInfo( planId ) map { summary => ( summary.info.name, summary.info.toSummary  ) }
  }

  def fetchPlanInfo( pid: AnalysisPlanModule.module.TID )( implicit ec: ExecutionContext, to: Timeout ): Future[AP.PlanInfo] = {
    def toInfo( message: Any ): Future[AP.PlanInfo] = {
      message match {
        case Envelope( info: AP.PlanInfo, _ ) => {
          log.info(Map("@msg" -> "fetched plan entity", "plan-id" -> info.sourceId.toString))
          Future successful info
        }

        case info: AP.PlanInfo => Future successful info

        case m => {
          val ex = new IllegalStateException( s"unknown response to plan inquiry for id:[${pid}]: ${m}" )
          log.error(Map("@msg" -> "failed to fetch plan for id","id" -> pid.toString), ex )
          Future failed ex
        }
      }
    }

    for {
      model <- boundedContext.futureModel
_ = log.debug(Map("@msg" -> "#TEST fetching info for plan", "plan-id" -> pid.toString) )
      ref = model( AnalysisPlanModule.module.rootType, pid )
      msg <- ( ref ?+ AP.GetPlan(pid) ).mapTo[Envelope]
_ = log.debug(Map("@msg" -> "#TEST fetched info from plan", "plan-id" -> pid.toString, "info" -> msg.payload.toString))
      info <- toInfo( msg )
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
//  import peds.akka.stream.StreamMonitor._
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
//  import peds.akka.stream.StreamMonitor._
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
