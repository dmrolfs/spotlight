package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect._
import scala.util.matching.Regex
import akka.Done
import akka.actor.Props
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigObject}
import shapeless._
import demesne.module.AggregateRootProps
import demesne.module.entity.EntityAggregateModule
import demesne.module.entity.messages.{EntityMessage, EntityProtocol}
import demesne.register.StackableRegisterBusPublisher
import demesne.{AggregateRootType, DomainModel}
import peds.akka.envelope._
import peds.akka.envelope.pattern.{ask, pipe}
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityIdentifying}
import peds.commons.Valid
import peds.commons.identifier.{ShortUUID, TaggedID}
import peds.commons.log.Trace
import spotlight.analysis.outlier.AnalysisPlanProtocol.PlanInfo
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.model.timeseries.{TimeSeries, Topic}

import scalaz.{-\/, \/-}


/**
  * Created by rolfsd on 5/20/16.
  */
object CatalogProtocol extends EntityProtocol[Catalog] {
  sealed trait CatalogProtocol
  case class GetPlansForTopic( targetId: Catalog#TID, topic: Topic ) extends Command with CatalogProtocol
  case class CatalogedPlans(
    sourceId: Catalog#TID,
    plans: Set[PlanSummary],
    request: GetPlansForTopic
  ) extends Event with CatalogProtocol

  case class AddPlan( override val targetId: Catalog#TID, summary: PlanSummary ) extends Command with CatalogProtocol

  case class RemovePlan(
    override val targetId: Catalog#TID,
    planId: OutlierPlan#TID,
    planName: String
  ) extends Command with CatalogProtocol

  case class PlanAdded(
    override val sourceId: Catalog#TID,
    planId: OutlierPlan#TID,
    planName: String
  ) extends Event with CatalogProtocol

  case class PlanRemoved(
    override val sourceId: Catalog#TID,
    planId: OutlierPlan#TID,
    planName: String
  ) extends Event with CatalogProtocol


  case class PlanSummary( id: OutlierPlan#TID, name: String, appliesTo: OutlierPlan.AppliesTo )
  object PlanSummary {
    def apply( info: OutlierPlan ): PlanSummary = PlanSummary( id = info.id, name = info.name, appliesTo = info.appliesTo )

    def apply( summary: AnalysisPlanProtocol.PlanInfo ): PlanSummary = {
      PlanSummary( id = summary.sourceId, name = summary.info.name, appliesTo = summary.info.appliesTo )
    }
  }
}


sealed trait Catalog extends Entity {
  override type ID = ShortUUID
  override val evID: ClassTag[ID] = classTag[ShortUUID]
  override val evTID: ClassTag[TID] = classTag[TaggedID[ShortUUID]]

  def isActive: Boolean
  def analysisPlans: Map[String, OutlierPlan#TID]
}

object Catalog {
  implicit val identifying: EntityIdentifying[Catalog] = {
    new EntityIdentifying[Catalog] with ShortUUID.ShortUuidIdentifying[Catalog] {
      override val evEntity: ClassTag[Catalog] = classTag[Catalog]
    }
  }

  private[outlier] def apply(
    id: Catalog#TID,
    name: String,
    slug: String,
    analysisPlans: Map[String, OutlierPlan#TID] = Map.empty[String, OutlierPlan#TID],
    isActive: Boolean = true
  ): Catalog = {
    CatalogState( id, name, slug, analysisPlans, isActive )
  }
}


object CatalogModule extends CatalogModule


final case class CatalogState private[outlier](
  override val id: Catalog#TID,
  override val name: String,
  override val slug: String,
  override val analysisPlans: Map[String, OutlierPlan#TID] = Map.empty[String, OutlierPlan#TID],
  override val isActive: Boolean = true
) extends Catalog


private[outlier] abstract class CatalogModule extends EntityAggregateModule[Catalog] { module =>
  override val trace: Trace[_] = Trace[CatalogModule.type]

  override def initializer(
    rootType: AggregateRootType,
    model: DomainModel,
    props: Map[Symbol, Any]
  )(
    implicit ec: ExecutionContext
  ): Valid[Future[Done]] = trace.block( "initializer" ) {
    import shapeless.syntax.typeable._

    logger.debug(
      "Module context demesne.configuration = [\n{}\n]",
      props(demesne.ConfigurationKey).asInstanceOf[Config].root().render(
        com.typesafe.config.ConfigRenderOptions.defaults().setFormatted(true)
      )
    )

    val init: Option[Valid[Future[Done]]] = {
      val spotlightPath = "spotlight"
      for {
        conf <- props get demesne.ConfigurationKey
        base <- conf.cast[Config]
        spotlight <- if ( base hasPath spotlightPath ) Some(base getConfig spotlightPath ) else None
        budget <- specifyDetectionBudget( spotlight getConfig "workflow" )
        plans <- Option( specifyPlans(spotlight getConfig "detection-plans") )
      } yield {
        detectionBudget = budget
        logger.debug( "CatalogModule: specified detection budget = [{}]", detectionBudget.toCoarsest )

        specifiedPlans = plans
        logger.debug( "CatalogModule: specified plans = [{}]", specifiedPlans.mkString(", ") )

        super.initializer( rootType, model, props )
      }
    }

    init getOrElse {
      throw new IllegalStateException( "unable to initialize CatalogModule due to insufficient context configuration" )
    }
  }

  protected var detectionBudget: FiniteDuration = 10.seconds
  private def specifyDetectionBudget( specification: Config ): Option[FiniteDuration] = trace.block("specifyDetectionBudget") {
    val budgetPath = "detect.timeout"
    if ( specification hasPath budgetPath ) {
      Some( FiniteDuration( specification.getDuration(budgetPath, NANOSECONDS), NANOSECONDS ) )
    } else {
      None
    }
  }

  protected var specifiedPlans: Set[OutlierPlan] = Set.empty[OutlierPlan]
  private def specifyPlans( specification: Config ): Set[OutlierPlan] = {
    import scala.collection.JavaConversions._ // since config is java API needed for collect

    specification.root
    .collect { case (n, s: ConfigObject) => ( n, s.toConfig ) }
    .toSet[(String, Config)]
    .map { case (name, spec) => module.makePlan( name, spec )( module.detectionBudget ) }
    .flatten
  }


  override def idLens: Lens[Catalog, TID] = new Lens[Catalog, Catalog#TID] {
    override def get( c: Catalog ): Catalog#TID = c.id
    override def set( c: Catalog )( id: Catalog#TID ): Catalog = {
      CatalogState( id = id, name = c.name, slug = c.slug, analysisPlans = c.analysisPlans, isActive = c.isActive )
    }
  }

  override val nameLens: Lens[Catalog, String] = new Lens[Catalog, String] {
    override def get( c: Catalog ): String = c.name
    override def set( c: Catalog )( name: String ): Catalog = {
      CatalogState( id = c.id, name = name, slug = c.slug, analysisPlans = c.analysisPlans, isActive = c.isActive )
    }
  }

  override val slugLens: Option[Lens[Catalog, String]] = {
    Some(
      new Lens[Catalog, String] {
        override def get( c: Catalog ): String = c.slug
        override def set( c: Catalog )( slug: String ): Catalog = {
          CatalogState( id = c.id, name = c.name, slug = slug, analysisPlans = c.analysisPlans, isActive = c.isActive )
        }
      }
    )
  }

  override val isActiveLens: Option[Lens[Catalog, Boolean]] = {
    Some(
      new Lens[Catalog, Boolean] {
        override def get( c: Catalog ): Boolean = c.isActive
        override def set( c: Catalog )( isActive: Boolean ): Catalog = {
          CatalogState( id = c.id, name = c.name, slug = c.slug, analysisPlans = c.analysisPlans, isActive = isActive )
        }
      }
    )
  }

  val analysisPlansLens: Lens[Catalog, Map[String, OutlierPlan#TID]] = new Lens[Catalog, Map[String, OutlierPlan#TID]] {
    override def get( c: Catalog ): Map[String, OutlierPlan#TID] = c.analysisPlans
    override def set( c: Catalog )( ps: Map[String, OutlierPlan#TID] ): Catalog = {
      CatalogState( id = c.id, name = c.name, slug = c.slug, analysisPlans = ps, isActive = c.isActive )
    }
  }

  override def aggregateRootPropsOp: AggregateRootProps = {
    (model: DomainModel, rootType: AggregateRootType) => CatalogActor.props( model, rootType )
  }


  object CatalogActor {
    def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

    private class Default( model: DomainModel, rootType: AggregateRootType )
    extends CatalogActor( model, rootType ) with Provider with StackableStreamPublisher with StackableRegisterBusPublisher


    trait Provider {
      def detectionBudget: FiniteDuration = module.detectionBudget
      def specifiedPlans: Set[OutlierPlan] = module.specifiedPlans
    }
  }

  class CatalogActor( override val model: DomainModel, override val rootType: AggregateRootType )
  extends module.EntityAggregateActor { actor: CatalogActor.Provider with EventPublisher =>
    import demesne.module.entity.{ messages => EntityMessage }
    import spotlight.analysis.outlier.{ CatalogProtocol => P }

    override var state: Catalog = _

    initialize upon first data message? to avoid order dependency?
    catalog doesn't need to be persistent if impl in terms of plan index

    val planIndex = {
      model.aggregateRegisterFor[String, OutlierPlan](
        AnalysisPlanModule.module.rootType,
        AnalysisPlanModule.namedPlanIndex
      ) match {
        case \/-( pindex ) => pindex
        case -\/( ex ) => {
          log.error( ex, "CatalogModule[{}]: failed to initialize CatalogActor's plan index", self.path )
          throw ex
        }
      }
    }

    var plansCache: Map[String, P.PlanSummary] = Map.empty[String, P.PlanSummary]

    val actorDispatcher = context.system.dispatcher
    val defaultTimeout = akka.util.Timeout( 30.seconds )


    override def preStart(): Unit = {
      super.preStart( )
      context.system.eventStream.subscribe( self, classOf[EntityMessage] )
      context.system.eventStream.subscribe( self, classOf[AnalysisPlanProtocol.Message] )
    }

    override def receiveRecover: Receive = {
      case akka.persistence.RecoveryCompleted => {

        val (oldPlans, newPlans) = actor.specifiedPlans partition { p =>
          Option(state)
          .map { _.analysisPlans contains p.name }
          .getOrElse { false }
        }
        log.info( "TEST: CatalogModule[{}] oldPlans=[{}]", self.path, oldPlans.mkString(",") )
        log.info( "TEST: CatalogModule[{}] newPlans=[{}]", self.path, newPlans.mkString(",") )

        implicit val ec = actorDispatcher
        implicit val to = defaultTimeout

        val plans = for {
          existing <- loadExistingCacheElements( oldPlans )
          created <- makeNewCacheElements( newPlans )
        } yield existing ++ created

        actor.plansCache = scala.concurrent.Await.result( plans , 5.seconds )
        log.debug( "CatalogModule[{}]: plans-cache=[{}]", self.path, actor.plansCache.mkString(", ") )
      }
    }

    def loadExistingCacheElements(
      plans: Set[OutlierPlan]
    )(
      implicit ec: ExecutionContext,
      to: Timeout
    ): Future[Map[String, P.PlanSummary]] = {
      val queries = plans.toSeq.map { p => loadPlan( p.id ) }
      Future.sequence( queries ) map { qs => Map( qs:_* ) }
    }

    def makeNewCacheElements(
      plans: Set[OutlierPlan]
    )(
      implicit ec: ExecutionContext,
      to: Timeout
    ): Future[Map[String, P.PlanSummary]] = {
      val queries = plans.toSeq.map { p =>
        log.info( "CatalogModule: making plan entity for: [{}]", p )
        val planRef =  model.aggregateOf( AnalysisPlanModule.module.rootType, p.id )

        for {
          added <- ( planRef ?+ EntityMessage.Add( p.id, Some(p) ) )
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

    def loadPlan( planId: OutlierPlan#TID )( implicit ec: ExecutionContext, to: Timeout ): Future[(String, P.PlanSummary)] = {
      fetchPlanInfo( planId ) map { summary => ( summary.info.name, P.PlanSummary(summary) ) }
    }

    WORK HERE NEED TO RECORD PLAN-ADDED FOR ESTABLISHED PLANS...  MAYBE PERSIST PLAN-ADDED EVENTS FOR SPECIFIED PLANS?
    WORK HERE HOW NOT TO CONFLICT WITH RECEOVERY SCENARIO?

    //todo upon add watch info actor for changes and incorporate them in to cache
    //todo the cache is what is sent out in reply to GetPlansForXYZ

    import spotlight.analysis.outlier.{ AnalysisPlanProtocol => PlanProtocol }

    override val acceptance: Acceptance = entityAcceptance orElse {
      case (P.PlanAdded(_, pid, name), s) => analysisPlansLens.modify( s ){ _ + (name -> pid) }
      case (P.PlanRemoved(_, _, name), s) => analysisPlansLens.modify( s ){ _ - name }
    }


    override def active: Receive = workflow orElse catalog orElse plans orElse super.active

    val workflow: Receive = {
      case ts: TimeSeries if plansCache.values.exists { _ appliesTo ts } => {
        // forwarding to retain publisher sender
        for {
          p <- plansCache.values if p appliesTo ts
          pref = model.aggregateOf( AnalysisPlanModule.module.rootType, p.id )
        } {
          log.debug( "CatalogModule[{}]: forwarding data[{}] to plan-module[{}]", self.path, ts.topic, pref.path )
          pref forwardEnvelope AnalysisPlanProtocol.AcceptTimeSeries( p.id, ts )
        }
      }

      case ts: TimeSeries => log.warning( "CatalogModule[{}]: no plan found for time-series:[{}]", self.path, ts.topic )
    }

    val catalog: Receive = {
      case P.AddPlan( _, summary ) => persistAddedPlan( summary )

      case P.RemovePlan( _, pid, name ) if state.analysisPlans.contains( name ) => persistRemovedPlan( name )

      case req @ P.GetPlansForTopic( _, topic ) => {
        val ps = plansCache collect { case (_, p) if p appliesTo topic => p }
        log.debug( "CatalogModule[{}]: for topic:[{}] returning plans:[{}]", topic, ps.mkString(", ") )
        log.debug( "TEST: IN GET-PLANS specified-plans:[{}]", specifiedPlans.mkString(", ") )
        sender() !+ P.CatalogedPlans( sourceId = state.id, plans = ps.toSet, request = req )
      }
    }

    val planTidType = TypeCase[OutlierPlan#TID]
    val planType = TypeCase[OutlierPlan]

    val plans: Receive = {
      case e @ EntityMessage.Added( _, Some(planType(info)) ) => persistAddedPlan( P.PlanSummary(info) )

      case e @ EntityMessage.Enabled( planTidType(id), _ ) => {
        implicit val ec = actorDispatcher
        implicit val to = defaultTimeout

        fetchPlanInfo( id )
        .map { i => P.AddPlan( targetId = state.id, summary = P.PlanSummary(i.info) ) }
        .pipeEnvelopeTo( self )
      }

      case e @ EntityMessage.Disabled( planTidType(id), slug ) => persistRemovedPlan( slug )

      case e @ EntityMessage.Renamed( planTidType(id), oldName, newName ) => {
        implicit val ec = actorDispatcher
        implicit val to = defaultTimeout

        persistRemovedPlan( oldName )

        fetchPlanInfo( id )
        .map { i => P.AddPlan( targetId = state.id, summary = P.PlanSummary(i.info) ) }
        .pipeEnvelopeTo( self )
      }
    }


    def fetchPlanInfo(
      id: module.TID
    )(
      implicit ec: ExecutionContext,
      to: Timeout
    ): Future[AnalysisPlanProtocol.PlanInfo] = {
      val planRef = model.aggregateOf( AnalysisPlanModule.module.rootType, id )
      ( planRef ?+ AnalysisPlanProtocol.GetPlan( id ) ).collect { case Envelope( info: PlanInfo, _ ) =>
        log.info( "CatalogModule: fetched plan entity: [{}]", info.sourceId )
        info
      }
    }

    def persistAddedPlan( summary: P.PlanSummary ): Unit = {
      persist( P.PlanAdded(state.id, summary.id, summary.name) ) { event =>
        plansCache += ( summary.name -> summary )
        acceptAndPublish( event )
      }
    }

    def persistRemovedPlan( name: String ): Unit = {
      state.analysisPlans.get( name ) foreach { pid =>
        persist( P.PlanRemoved(state.id, pid, name) ) { event =>
          acceptAndPublish( event )
          plansCache -= name
        }
      }
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
