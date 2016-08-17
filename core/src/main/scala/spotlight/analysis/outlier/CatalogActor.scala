package spotlight.analysis.outlier

import java.util.ServiceConfigurationError
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.matching.Regex
import akka.Done
import akka.actor.{Actor, ActorLogging, Props}
import akka.event.LoggingReceive
import akka.util.Timeout
import scalaz._
import Scalaz._
import com.typesafe.config.{Config, ConfigObject}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.MetricName
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import peds.akka.metrics.InstrumentedActor
import peds.commons.Valid
import peds.commons.log.Trace
import demesne.DomainModel
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.model.timeseries.{TimeSeries, Topic}


/**
  * Created by rolfsd on 5/20/16.
  */
object CatalogProtocol {
  sealed trait CatalogMessage
  case object WaitingForStart extends CatalogMessage
  case object Started extends CatalogMessage
  case class GetPlansForTopic( topic: Topic ) extends CatalogMessage
  case class CatalogedPlans( plans: Set[OutlierPlan.Summary], request: GetPlansForTopic ) extends CatalogMessage
}


object CatalogActor extends LazyLogging {
  def props(model: DomainModel, configuration: Config): Props = {
    Default.checkConfiguration( configuration ).disjunction match {
      case \/-( c ) => Props( new Default( model, c ) )
      case -\/( exs ) => {
        exs foreach { ex => logger.error( "CatalogActor failed to make actor Props", ex ) }
        throw exs.head
      }
    }
  }


  trait Provider {
    def detectionBudget: FiniteDuration
    def specifiedPlans: Set[OutlierPlan]
  }


  object Default {
    val detectionBudgetPath = "spotlight.workflow.detect.timeout"
    val outlierPlansPath = "spotlight.detection-plans"

    def checkConfiguration(configuration: Config): Valid[Config] = {
      if ( configuration hasPath outlierPlansPath ) configuration.successNel
      else Validation.failureNel( new ServiceConfigurationError( s"configuration does not have path: ${outlierPlansPath}" ) )
    }
  }

  final class Default private[CatalogActor]( model: DomainModel, configuration: Config )
  extends CatalogActor( model ) with Provider {
    private val trace = Trace[Default]

    override lazy val detectionBudget: FiniteDuration = specifyDetectionBudget( configuration ) getOrElse 10.seconds

    private def specifyDetectionBudget(specification: Config): Option[FiniteDuration] = trace.block( "specifyDetectionBudget" ) {
      import Default.{ detectionBudgetPath => budgetPath}
      if ( specification hasPath budgetPath ) {
        Some( FiniteDuration( specification.getDuration( Default.detectionBudgetPath, NANOSECONDS ), NANOSECONDS ) )
      } else {
        None
      }
    }

    override lazy val specifiedPlans: Set[OutlierPlan] = specifyPlans( configuration )

    private def specifyPlans( specification: Config ): Set[OutlierPlan] = trace.block( "specifyPlans" ) {
      import Default.{ outlierPlansPath => planPath }
      if ( specification hasPath planPath ) {
        val planSpecs = specification getConfig planPath
        log.debug( "specification = [{}]", planSpecs.root().render() )
        import scala.collection.JavaConversions._
        // since config is java API needed for collect

        planSpecs.root
        .collect { case (n, s: ConfigObject) => (n, s.toConfig) }
        .toSet[(String, Config)]
        .map { case (name, spec) => makePlan( name, spec )( detectionBudget ) }
        .flatten
      } else {
        log.warning( "CatalogActor[{}]: no plan specifications found", self.path)
        Set.empty[OutlierPlan]
      }
    }


    private def makePlan( name: String, planSpecification: Config )( budget: FiniteDuration ): Option[OutlierPlan] = {
      logger.info( "CatalogModule plan speclet: [{}]", planSpecification )

      //todo: bring initialization of plans into module init and base config on init config?
      val utilization = 0.8
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
          "CatalogModule: topic[{}] default-type plan specification origin:[{}] line:[{}]",
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
          "CatalogModule: topic:[{}] topic-based plan specification origin:[{}] line:[{}]",
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
          "CatalogModule: topic:[{}] regex-based plan specification origin:[{}] line:[{}]",
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

class CatalogActor( model: DomainModel ) extends Actor with InstrumentedActor with ActorLogging { outer: CatalogActor.Provider =>
  import spotlight.analysis.outlier.{ CatalogProtocol => P }
  import spotlight.analysis.outlier.{ AnalysisPlanProtocol => AP }

  override lazy val metricBaseName: MetricName = MetricName( classOf[CatalogActor] )

  type PlanIndex = DomainModel.AggregateIndex[String, AnalysisPlanModule.module.TID, OutlierPlan.Summary]
  lazy val planIndex: PlanIndex = {
    model
    .aggregateIndexFor[String, AnalysisPlanModule.module.TID, OutlierPlan.Summary](
      AnalysisPlanModule.module.rootType,
      AnalysisPlanModule.namedPlanIndex
    ) match {
      case \/-( pindex ) => pindex
      case -\/( ex ) => {
        log.error( ex, "CatalogActor[{}]: failed to initialize CatalogActor's plan index", self.path )
        throw ex
      }
    }
  }

  case object Initialize extends P.CatalogMessage

  override def preStart(): Unit = self ! Initialize

  override def receive: Receive = quiescent


  val quiescent: Receive = {
    case Initialize => {
      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool
      postStartInitialization() foreach { _ =>
        log.info( "CatalogActor[{}] initialization completed" )
        context become LoggingReceive { around( active ) }
      }
    }
  }

  val active: Receive = {
    case ts: TimeSeries if planIndex.entries.values exists { _ appliesTo ts } => {
      // forwarding to retain publisher sender
      for {
        p <- planIndex.entries.values if p appliesTo ts
        pref = model.aggregateOf( AnalysisPlanModule.module.rootType, p.id )
      } {
        log.debug( "CatalogActor[{}]: forwarding data[{}] to plan-module[{}]", self.path, ts.topic, pref.path )
        pref forwardEnvelope AP.AcceptTimeSeries( p.id, ts )
      }
    }

    case ts: TimeSeries => {
      log.warning( "CatalogActor[{}]: no plan on record that applies to time-series:[{}]", self.path, ts.topic )
    }

    case req @ P.GetPlansForTopic( topic ) => {
      import peds.akka.envelope.pattern.pipe

      implicit val ec = context.system.dispatcher //todo: consider moving off actor threadpool

      planIndex.futureEntries
      .map { entries =>
        val ps = entries collect { case (_, p) if p appliesTo topic => p }
        log.debug( "CatalogActor[{}]: for topic:[{}] returning plans:[{}]", self.path, topic, ps.mkString(", ") )
        log.debug( "TEST: IN GET-PLANS specified-plans:[{}]", specifiedPlans.mkString(", ") )
        P.CatalogedPlans( plans = ps.toSet, request = req )
      }
      .pipeEnvelopeTo( sender() )
    }

    case P.WaitingForStart => sender() ! P.Started
  }

  private def postStartInitialization(): Future[Done] = {
    implicit val ec = context.system.dispatcher
    implicit val timeout = Timeout( 30.seconds )

    for {
      entries <- planIndex.futureEntries
      ( registered, missing ) = outer.specifiedPlans partition { entries contains _.name }
      _ = log.info( "TEST: CatalogActor[{}] registered plans=[{}]", self.path, registered.mkString(",") )
      _ = log.info( "TEST: CatalogActor[{}] missing plans=[{}]", self.path, missing.mkString(",") )
      created <- makeMissingSpecifiedPlans( missing )
    } yield {
      log.info(
        "CatalogActor[{}] created additional {} plans: [{}]",
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

    val queries = missing.toSeq.map { p =>
      log.info( "CatalogActor[{}]: making plan entity for: [{}]", self.path, p )
      val planRef =  model.aggregateOf( AnalysisPlanModule.module.rootType, p.id )

      for {
        added <- ( planRef ?+ EntityMessages.Add( p.id, Some(p) ) )
        _ = log.debug( "CatalogModule: notified that plan is added:[{}]", added )
        loaded <- loadPlan( p.id )
        _ = log.debug( "CatalogModule: loaded plan:[{}]", loaded )
      } yield loaded
    }


    Future.sequence( queries ) map { qs =>
      log.info( "TEST: CatalogModule loaded plans: [{}]", qs.map{ case (n,p) => s"$n->$p" }.mkString(",") )
      Map( qs:_* )
    }
  }

  def loadPlan( planId: OutlierPlan#TID )( implicit ec: ExecutionContext, to: Timeout ): Future[(String, OutlierPlan.Summary)] = {
    fetchPlanInfo( planId ) map { summary => ( summary.info.name, summary.info.toSummary  ) }
  }

  def fetchPlanInfo(
    pid: AnalysisPlanModule.module.TID
  )(
    implicit ec: ExecutionContext,
    to: Timeout
  ): Future[AnalysisPlanProtocol.PlanInfo] = {
    val planRef = model.aggregateOf( AnalysisPlanModule.module.rootType, pid )
    ( planRef ?+ AP.GetPlan( pid ) ).collect { case Envelope( info: AP.PlanInfo, _ ) =>
      log.info( "CatalogActore[{}]: fetched plan entity: [{}]", self.path, info.sourceId )
      info
    }
  }

}





//sealed trait Catalog extends Entity {
//  override type ID = ShortUUID
//  override val evID: ClassTag[ID] = classTag[ShortUUID]
//  override val evTID: ClassTag[TID] = classTag[TaggedID[ShortUUID]]
//
//  def isActive: Boolean
//  def analysisPlans: Map[String, OutlierPlan#TID]
//}
//
//object Catalog {
//  implicit val identifying: EntityIdentifying[Catalog] = {
//    new EntityIdentifying[Catalog] with ShortUUID.ShortUuidIdentifying[Catalog] {
//      override val evEntity: ClassTag[Catalog] = classTag[Catalog]
//    }
//  }
//
//  private[outlier] def apply(
//    id: Catalog#TID,
//    name: String,
//    slug: String,
//    analysisPlans: Map[String, OutlierPlan#TID] = Map.empty[String, OutlierPlan#TID],
//    isActive: Boolean = true
//  ): Catalog = {
//    CatalogState( id, name, slug, analysisPlans, isActive )
//  }
//}


//object CatalogModule extends CatalogModule


//final case class CatalogState private[outlier](
//  override val id: Catalog#TID,
//  override val name: String,
//  override val slug: String,
//  override val analysisPlans: Map[String, OutlierPlan#TID] = Map.empty[String, OutlierPlan#TID],
//  override val isActive: Boolean = true
//) extends Catalog

//class CatalogActor extends Actor with InstrumentedActor with ActorLogging { module: CatalogActor.Provider =>
//
//
//
////////////////////////////////////////////////////////
///////////////////////////////////////////////////////
//
//
//  override def initializer(
//    rootType: AggregateRootType,
//    model: DomainModel,
//    props: Map[Symbol, Any]
//  )(
//    implicit ec: ExecutionContext
//  ): Valid[Future[Done]] = trace.block( "initializer" ) {
//    import shapeless.syntax.typeable._
//
//    logger.debug(
//      "Module context demesne.configuration = [\n{}\n]",
//      props(demesne.ConfigurationKey).asInstanceOf[Config].root().render(
//        com.typesafe.config.ConfigRenderOptions.defaults().setFormatted(true)
//      )
//    )
//
//    val init: Option[Valid[Future[Done]]] = {
//      val spotlightPath = "spotlight"
//      for {
//        conf <- props get demesne.ConfigurationKey
//        base <- conf.cast[Config]
//        spotlight <- if ( base hasPath spotlightPath ) Some(base getConfig spotlightPath ) else None
//        budget <- specifyDetectionBudget( spotlight getConfig "workflow" )
//        plans <- Option( specifyPlans(spotlight getConfig "detection-plans") )
//      } yield {
//        detectionBudget = budget
//        logger.debug( "CatalogModule: specified detection budget = [{}]", detectionBudget.toCoarsest )
//
//        specifiedPlans = plans
//        logger.debug( "CatalogModule: specified plans = [{}]", specifiedPlans.mkString(", ") )
//
//        super.initializer( rootType, model, props )
//      }
//    }
//
//    init getOrElse {
//      throw new IllegalStateException( "unable to initialize CatalogModule due to insufficient context configuration" )
//    }
//  }
//
//  var detectionBudget: FiniteDuration = 10.seconds
//
//  protected var specifiedPlans: Set[OutlierPlan] = Set.empty[OutlierPlan]
//
//
//  override def idLens: Lens[Catalog, TID] = new Lens[Catalog, Catalog#TID] {
//    override def get( c: Catalog ): Catalog#TID = c.id
//    override def set( c: Catalog )( id: Catalog#TID ): Catalog = {
//      CatalogState( id = id, name = c.name, slug = c.slug, analysisPlans = c.analysisPlans, isActive = c.isActive )
//    }
//  }
//
//  override val nameLens: Lens[Catalog, String] = new Lens[Catalog, String] {
//    override def get( c: Catalog ): String = c.name
//    override def set( c: Catalog )( name: String ): Catalog = {
//      CatalogState( id = c.id, name = name, slug = c.slug, analysisPlans = c.analysisPlans, isActive = c.isActive )
//    }
//  }
//
//  override val slugLens: Option[Lens[Catalog, String]] = {
//    Some(
//      new Lens[Catalog, String] {
//        override def get( c: Catalog ): String = c.slug
//        override def set( c: Catalog )( slug: String ): Catalog = {
//          CatalogState( id = c.id, name = c.name, slug = slug, analysisPlans = c.analysisPlans, isActive = c.isActive )
//        }
//      }
//    )
//  }
//
//  override val isActiveLens: Option[Lens[Catalog, Boolean]] = {
//    Some(
//      new Lens[Catalog, Boolean] {
//        override def get( c: Catalog ): Boolean = c.isActive
//        override def set( c: Catalog )( isActive: Boolean ): Catalog = {
//          CatalogState( id = c.id, name = c.name, slug = c.slug, analysisPlans = c.analysisPlans, isActive = isActive )
//        }
//      }
//    )
//  }
//
//  val analysisPlansLens: Lens[Catalog, Map[String, OutlierPlan#TID]] = new Lens[Catalog, Map[String, OutlierPlan#TID]] {
//    override def get( c: Catalog ): Map[String, OutlierPlan#TID] = c.analysisPlans
//    override def set( c: Catalog )( ps: Map[String, OutlierPlan#TID] ): Catalog = {
//      CatalogState( id = c.id, name = c.name, slug = c.slug, analysisPlans = ps, isActive = c.isActive )
//    }
//  }
//
//  override def aggregateRootPropsOp: AggregateRootProps = {
//    (model: DomainModel, rootType: AggregateRootType) => CatalogActor.props( model, rootType )
//  }
//
//
//  object CatalogActor {
//    def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )
//
//    private class Default( model: DomainModel, rootType: AggregateRootType )
//    extends CatalogActor( model, rootType ) with Provider with StackableStreamPublisher with StackableRegisterBusPublisher
//
//
//    trait Provider {
//      def detectionBudget: FiniteDuration = module.detectionBudget
//      def specifiedPlans: Set[OutlierPlan] = module.specifiedPlans
//    }
//  }
//
//  class CatalogActor( override val model: DomainModel, override val rootType: AggregateRootType )
//  extends module.EntityAggregateActor { actor: CatalogActor.Provider with EventPublisher =>
//    import demesne.module.entity.{ messages => EntityMessage }
//    import spotlight.analysis.outlier.{ CatalogProtocol => P }
//    import spotlight.analysis.outlier.{ AnalysisPlanProtocol => AP }
//
////    override var state: Catalog = _
//
////    initialize upon first data message? to avoid order dependency?
////    catalog doesn't need to be persistent if impl in terms of plan index
//
//
////    var plansCache: Map[String, P.PlanSummary] = Map.empty[String, P.PlanSummary]
//
////    val actorDispatcher = context.system.dispatcher
////    val defaultTimeout = akka.util.Timeout( 30.seconds )
//
//
////    override def preStart(): Unit = {
////      super.preStart( )
////      context.system.eventStream.subscribe( self, classOf[EntityMessage] )
////      context.system.eventStream.subscribe( self, classOf[AnalysisPlanProtocol.Message] )
////    }
//
//
////    def loadExistingCacheElements(
////      plans: Set[OutlierPlan]
////    )(
////      implicit ec: ExecutionContext,
////      to: Timeout
////    ): Future[Map[String, P.PlanSummary]] = {
////      val queries = plans.toSeq.map { p => loadPlan( p.id ) }
////      Future.sequence( queries ) map { qs => Map( qs:_* ) }
////    }
//
//
////    WORK HERE NEED TO RECORD PLAN-ADDED FOR ESTABLISHED PLANS...  MAYBE PERSIST PLAN-ADDED EVENTS FOR SPECIFIED PLANS?
////    WORK HERE HOW NOT TO CONFLICT WITH RECEOVERY SCENARIO?
//
//    //todo upon add watch info actor for changes and incorporate them in to cache
//    //todo the cache is what is sent out in reply to GetPlansForXYZ
//
//
////    override val acceptance: Acceptance = entityAcceptance orElse {
////      case (P.PlanAdded(_, pid, name), s) => analysisPlansLens.modify( s ){ _ + (name -> pid) }
////      case (P.PlanRemoved(_, _, name), s) => analysisPlansLens.modify( s ){ _ - name }
////    }
//
//
////    override def active: Receive = workflow orElse catalog orElse plans orElse super.active
//
//
//    val catalog: Receive = {
////      case P.AddPlan( _, summary ) => persistAddedPlan( summary )
//
////      case P.RemovePlan( _, pid, name ) if state.analysisPlans.contains( name ) => persistRemovedPlan( name )
//
//    }
//
//
//
//  }
//
//
//}
