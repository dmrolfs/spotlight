package spotlight.analysis.outlier

import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration._
import scala.util.matching.Regex
import akka.actor.Props
import akka.pattern.{ask, pipe}
import akka.persistence.RecoveryCompleted
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import shapeless._
import demesne.{AggregateRootType, DomainModel}
import demesne.module.EntityAggregateModule
import demesne.register.StackableRegisterBusPublisher
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityCompanion}
import peds.commons.identifier.{ShortUUID, TaggedID}
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.model.timeseries.{TimeSeries, Topic}
import spotlight.analysis.outlier.AnalysisPlanModule.AggregateRoot.{Protocol => PlanProtocol, module => PlanModule}


/**
  * Created by rolfsd on 5/20/16.
  */
trait Catalog extends Entity {
  override type ID = ShortUUID
  override def idClass: Class[_] = classOf[ShortUUID]
  def isActive: Boolean
  def analysisPlans: Map[String, OutlierPlan#TID]
  def configuration: Config
}

object Catalog extends EntityCompanion[Catalog] {
  def apply( name: String, slug: String, config: Config ): Catalog = {
    CatalogState(
      id = nextId(),
      name = name,
      slug = slug,
      configuration = config,
      isActive = true
    )
  }

  override def nextId(): Catalog#TID = ShortUUID()
  override val idTag: Symbol = 'catalog
  override implicit def tag( id: Catalog#ID ): Catalog#TID = TaggedID( idTag, id )

  object ConfigPaths {
    val DETECTION_PLANS = "spotlight.detection-plans"
    val DETECTION_BUDGET = "spotlight.workflow.detect.timeout"
  }


  override val idLens: Lens[Catalog, Catalog#TID] = new Lens[Catalog, Catalog#TID] {
    override def get( c: Catalog ): Catalog#TID = c.id
    override def set( c: Catalog )( id: Catalog#TID ): Catalog = {
      CatalogState(
        id = id,
        name = c.name,
        slug = c.slug,
        configuration = c.configuration,
        analysisPlans = c.analysisPlans,
        isActive = c.isActive
      )
    }
  }

  override val nameLens: Lens[Catalog, String] = new Lens[Catalog, String] {
    override def get( c: Catalog ): String = c.name
    override def set( c: Catalog )( name: String ): Catalog = {
      CatalogState(
        id = c.id,
        name = name,
        slug = c.slug,
        configuration = c.configuration,
        analysisPlans = c.analysisPlans,
        isActive = c.isActive
      )
    }
  }

  val slugLens: Lens[Catalog, String] = new Lens[Catalog, String] {
    override def get( c: Catalog ): String = c.slug
    override def set( c: Catalog )( slug: String ): Catalog = {
      CatalogState(
        id = c.id,
        name = c.name,
        slug = slug,
        configuration = c.configuration,
        analysisPlans = c.analysisPlans,
        isActive = c.isActive
      )
    }
  }

  val isActiveLens: Lens[Catalog, Boolean] = new Lens[Catalog, Boolean] {
    override def get( c: Catalog ): Boolean = c.isActive
    override def set( c: Catalog )( isActive: Boolean ): Catalog = {
      CatalogState(
        id = c.id,
        name = c.name,
        slug = c.slug,
        configuration = c.configuration,
        analysisPlans = c.analysisPlans,
        isActive = isActive
      )
    }
  }

  val analysisPlansLens: Lens[Catalog, Map[String, OutlierPlan#TID]] = new Lens[Catalog, Map[String, OutlierPlan#TID]] {
    override def get( c: Catalog ): Map[String, OutlierPlan#TID] = c.analysisPlans
    override def set( c: Catalog )( ps: Map[String, OutlierPlan#TID] ): Catalog = {
      CatalogState(
        id = c.id,
        name = c.name,
        slug = c.slug,
        configuration = c.configuration,
        analysisPlans = ps,
        isActive = c.isActive
      )
    }
  }

  val configurationLens: Lens[Catalog, Config] = new Lens[Catalog, Config] {
    override def get( c: Catalog ): Config = c.configuration
    override def set( c: Catalog )( config: Config ): Catalog = {
      CatalogState(
        id = c.id,
        name = c.name,
        slug = c.slug,
        configuration = config,
        analysisPlans = c.analysisPlans,
        isActive = c.isActive
      )
    }
  }


  case class PlanSummary( id: OutlierPlan#TID, name: String, appliesTo: OutlierPlan.AppliesTo )
  object PlanSummary {
    def apply( info: OutlierPlan ): PlanSummary = {
      PlanSummary( id = info.id, name = info.name, appliesTo = info.appliesTo )
    }

    def apply( summary: PlanProtocol.PlanInfo ): PlanSummary = {
      PlanSummary( id = summary.sourceId, name = summary.info.name, appliesTo = summary.info.appliesTo )
    }
  }


  final case class CatalogState private[Catalog](
    override val id: Catalog#TID,
    override val name: String,
    override val slug: String,
    override val configuration: Config,
    override val analysisPlans: Map[String, OutlierPlan#TID] = Map.empty[String, OutlierPlan#TID],
    override val isActive: Boolean = true
  ) extends Catalog



  object AggregateRoot {
    val module: EntityAggregateModule[Catalog] = {
      val b = EntityAggregateModule.builderFor[Catalog].make
      import b.P.{ Tag => BTag, Props => BProps, _ }

      b
      .builder
      .set( BTag, Catalog.idTag )
      .set( BProps, CatalogActor.props(_, _) )
      .set( IdLens, idLens )
      .set( NameLens, nameLens )
      .set( SlugLens, Some(slugLens) )
      .set( IsActiveLens, Some(isActiveLens) )
      .build()
    }

    object Protocol extends module.Protocol {
      sealed trait CatalogProtocol
      case class GetPlansForTopic( targetId: Catalog#TID, topic: Topic ) extends CatalogProtocol
      case class CatalogedPlans( sourceId: Catalog#TID, plans: Set[PlanSummary] ) extends CatalogProtocol

      case class AddPlan( override val targetId: AddPlan#TID, summary: PlanSummary ) extends Command with CatalogProtocol

      case class RemovePlan(
        override val targetId: RemovePlan#TID,
        planId: OutlierPlan#TID,
        planName: String
      ) extends Command with CatalogProtocol

      case class PlanAdded(
        override val sourceId: PlanAdded#TID,
        planId: OutlierPlan#TID,
        planName: String
      ) extends Event with CatalogProtocol

      case class PlanRemoved(
        override val sourceId: PlanRemoved#TID,
        planId: OutlierPlan#TID,
        planName: String
      ) extends Event with CatalogProtocol
    }


    object CatalogActor {
      def props( model: DomainModel, meta: AggregateRootType ): Props = {
        Props( new CatalogActor( model, meta ) with StackableStreamPublisher with StackableRegisterBusPublisher )
      }
    }

    class CatalogActor( override val model: DomainModel, override val meta: AggregateRootType )
    extends module.EntityAggregateActor { publisher: EventPublisher =>
      override var state: Catalog = _

      var plansCache: Map[String, PlanSummary] = Map.empty[String, PlanSummary]

      val actorEc = context.system.dispatcher
      val defaultTimeout = Timeout( 30.seconds )


      override def preStart(): Unit = {
        super.preStart( )
        context.system.eventStream.subscribe( self, classOf[PlanProtocol.Entity.EntityMessage] )
        context.system.eventStream.subscribe( self, classOf[PlanProtocol.PlanProtocol] )
      }

      override def receiveRecover: Receive = {
        case RecoveryCompleted => {
          val config = ConfigFactory.load()
          val configPlans: Set[OutlierPlan] = makePlans( config.getConfig(ConfigPaths.DETECTION_PLANS), detectionBudget(config) )
          val (oldPlans, newPlans) = configPlans partition { state.analysisPlans contains _.name }

          implicit val ec = actorEc
          implicit val to = defaultTimeout

          val plans = for {
            existing <- loadExistingCacheElements( oldPlans )
            created <- makeNewCacheElements( newPlans )
          } yield existing ++ created

          val bootstrapTimeout = akka.pattern.after( 5.seconds, context.system.scheduler ) {
            Future.failed( new TimeoutException( "failed to bootstrap Outlier Plan Catalog" ) )
          }

          plansCache = Await.result( Future.firstCompletedOf( plans :: bootstrapTimeout :: Nil ), 5.seconds )
        }
      }

      def loadExistingCacheElements(
        plans: Set[OutlierPlan]
      )(
        implicit ec: ExecutionContext,
        to: Timeout
      ): Future[Map[String, PlanSummary]] = {
        val queries = plans.toSeq.map { p => loadPlan( p.id ) }
        Future.sequence( queries ) map { qs => Map( qs:_* ) }
      }

      def makeNewCacheElements(
        plans: Set[OutlierPlan]
      )(
        implicit ec: ExecutionContext,
        to: Timeout
      ): Future[Map[String, PlanSummary]] = {
        val queries = plans.toSeq.map { p =>
          val planRef = model.aggregateOf( PlanModule.aggregateRootType, p.id )
          planRef ! PlanProtocol.Entity.Add( p )
          loadPlan( p.id )
        }
        Future.sequence( queries ) map { qs => Map( qs:_* ) }
      }

      def loadPlan( planId: OutlierPlan#TID )( implicit ec: ExecutionContext, to: Timeout ): Future[(String, PlanSummary)] = {
        val planRef = model.aggregateOf( PlanModule.aggregateRootType, planId )
        ( planRef ? PlanProtocol.GetInfo )
        .mapTo[PlanProtocol.PlanInfo]
        .map { summary => ( summary.info.name, PlanSummary( summary ) ) }
      }


      //todo upon add watch info actor for changes and incorporate them in to cache
      //todo the cache is what is sent out in reply to GetPlansForXYZ

      import Protocol._

      override val acceptance: Acceptance = entityAcceptance orElse {
        case (PlanAdded(_, pid, name), s) => analysisPlansLens.modify( s ){ _ + (name -> pid) }
        case (PlanRemoved(_, _, name), s) => analysisPlansLens.modify( s ){ _ - name }
      }


      override def active: Receive = workflow orElse catalog orElse plans orElse super.active

      val workflow: Receive = {
        case ts: TimeSeries => {
          for {
            p <- plansCache.values if p appliesTo ts
          } {
            // forwarding to retain publisher sender
            model.aggregateOf( PlanModule.aggregateRootType, p.id ) forward ts
          }
        }
      }

      val catalog: Receive = {
        case AddPlan( _, summary ) => persistAddedPlan( summary )

        case RemovePlan( _, pid, name ) if state.analysisPlans.contains( name ) => persistRemovedPlan( name )

        case GetPlansForTopic( _, topic ) => {
          val ps = plansCache collect { case (_, p) if p appliesTo topic => p }
          sender() ! CatalogedPlans( sourceId = state.id, plans = ps.toSet )
        }
      }

      val plans: Receive = {
        case e: PlanProtocol.Entity.Added => persistAddedPlan( PlanSummary(e.info) )

        case e: PlanProtocol.Entity.Enabled => {
          implicit val ec = actorEc
          implicit val to = defaultTimeout

          fetchPlanInfo( e.sourceId )
          .map { i => AddPlan( targetId = state.id, summary = PlanSummary(i.info) ) }
          .pipeTo( self )
        }

        case e: PlanProtocol.Entity.Disabled => persistRemovedPlan( e.slug )

        case e: PlanProtocol.Entity.Renamed => {
          implicit val ec = actorEc
          implicit val to = defaultTimeout

          persistRemovedPlan( e.oldName )

          fetchPlanInfo( e.sourceId )
          .map { i => AddPlan( targetId = state.id, summary = PlanSummary(i.info) ) }
          .pipeTo( self )
        }
      }


      def fetchPlanInfo( id: PlanModule.TID )( implicit ec: ExecutionContext, to: Timeout ): Future[PlanProtocol.PlanInfo] = {
        val planRef = model.aggregateOf( PlanModule.aggregateRootType, id )
        ( planRef ? PlanProtocol.GetInfo )
        .mapTo[PlanProtocol.PlanInfo]
      }


      def persistAddedPlan( summary: PlanSummary ): Unit = {
          persist( PlanAdded(state.id, summary.id, summary.name) ) { event =>
            plansCache += ( summary.name -> summary )
            acceptAndPublish( event )
          }
        }

      def persistRemovedPlan( name: String ): Unit = {
        state.analysisPlans.get( name ) foreach { pid =>
          persist( PlanRemoved(state.id, pid, name) ) { event =>
            acceptAndPublish( event )
            plansCache -= name
          }
        }
      }


      private def makePlans( planSpecifications: Config, budget: FiniteDuration ): Set[OutlierPlan] = {
        import scala.collection.JavaConversions._

        val utilization = 0.8
        val utilized = budget * utilization
        val timeout = if ( utilized.isFinite ) utilized.asInstanceOf[FiniteDuration] else budget

        val result = planSpecifications.root.collect{ case (n, s: ConfigObject) => (n, s.toConfig) }.toSeq.map {
          case (name, spec) => {
            val IS_DEFAULT = "is-default"
            val TOPICS = "topics"
            val REGEX = "regex"

            val grouping: Option[OutlierPlan.Grouping] = {
              val GROUP_LIMIT = "group.limit"
              val GROUP_WITHIN = "group.within"
              val limit = if ( spec hasPath GROUP_LIMIT ) spec getInt GROUP_LIMIT else 10000
              log.info( "CONFIGURATION spec: [{}]", spec )
              val window = if ( spec hasPath GROUP_WITHIN ) {
                Some( FiniteDuration( spec.getDuration( GROUP_WITHIN ).toNanos, NANOSECONDS ) )
              } else {
                None
              }

              window map { w => OutlierPlan.Grouping( limit, w ) }
            }

            //todo: add configuration for at-least and majority
            val algorithms = planAlgorithms( spec )

            if ( spec.hasPath(IS_DEFAULT) && spec.getBoolean(IS_DEFAULT) ) {
              log.info( "topic[{}] default info origin:[{}] line:[{}]", name, spec.origin, spec.origin.lineNumber )

              Some(
                OutlierPlan.default(
                  name = name,
                  timeout = timeout,
                  isQuorum = makeIsQuorum( spec, algorithms.size ),
                  reduce = makeOutlierReducer( spec ),
                  algorithms = algorithms,
                  grouping = grouping,
                  planSpecification = spec
                )
              )
            } else if ( spec hasPath TOPICS ) {
              import scala.collection.JavaConverters._
              log.info( "topic:[{}] topic info origin:[{}] line:[{}]", name, spec.origin, spec.origin.lineNumber )

              Some(
                OutlierPlan.forTopics(
                  name = name,
                  timeout = timeout,
                  isQuorum = makeIsQuorum( spec, algorithms.size ),
                  reduce = makeOutlierReducer( spec ),
                  algorithms = algorithms,
                  grouping = grouping,
                  planSpecification = spec,
                  extractTopic = OutlierDetection.extractOutlierDetectionTopic,
                  topics = spec.getStringList(TOPICS).asScala.map{ Topic(_) }.toSet
                )
              )
            } else if ( spec hasPath REGEX ) {
              log.info( "topic:[{}] regex info origin:[{}] line:[{}]", name, spec.origin, spec.origin.lineNumber )
              Some(
                OutlierPlan.forRegex(
                  name = name,
                  timeout = timeout,
                  isQuorum = makeIsQuorum( spec, algorithms.size ),
                  reduce = makeOutlierReducer( spec ),
                  algorithms = algorithms,
                  grouping = grouping,
                  planSpecification = spec,
                  extractTopic = OutlierDetection.extractOutlierDetectionTopic,
                  regex = new Regex( spec.getString(REGEX) )
                )
              )
            } else {
              None
            }
          }
        }

        result.flatten.toSet
      }

      private def planAlgorithms( spec: Config ): Set[Symbol] = {
        import scala.collection.JavaConversions._
        val AlgorithmsPath = "algorithms"
        if ( spec hasPath AlgorithmsPath ) spec.getStringList( AlgorithmsPath ).toSet.map{ a: String => Symbol( a ) }
        else Set.empty[Symbol]
      }

      def detectionBudget( config: Config ): FiniteDuration = {
        FiniteDuration( config.getDuration(ConfigPaths.DETECTION_BUDGET, NANOSECONDS), NANOSECONDS )
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
        val MAJORITY = "majority"
        val AT_LEAST = "at-least"
        if ( spec hasPath AT_LEAST ) {
          val threshold = spec getInt AT_LEAST
          ReduceOutliers.byCorroborationCount( threshold )
        } else {
          val threshold = if ( spec hasPath MAJORITY ) spec.getDouble( MAJORITY ) else 50D
          ReduceOutliers.byCorroborationPercentage( threshold )
        }
      }
    }
  }
}