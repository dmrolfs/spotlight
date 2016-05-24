package spotlight.analysis.outlier

import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration._
import akka.actor.{ActorRef, Props}
import akka.persistence.RecoveryCompleted
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import shapeless._
import demesne.{AggregateRootModule, AggregateRootType, DomainModel}
import demesne.module.EntityAggregateModule
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityCompanion}
import peds.commons.identifier.{ShortUUID, TaggedID}
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.model.timeseries.Topic

import scala.util.matching.Regex


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

   case class CatalogState (
    override val id: Catalog#TID,
    override val name: String,
    override val slug: String,
    override val configuration: Config,
    override val analysisPlans: Map[String, OutlierPlan#TID] = Map.empty[String, OutlierPlan#TID],
    override val isActive: Boolean = true
  ) extends Catalog


  type Command = AggregateRootModule.Command[Catalog#ID]
  type Event = AggregateRootModule.Event[Catalog#ID]

  sealed trait CatalogProtocol
  case class GetPlansForTopic( targetId: Catalog#TID, topic: Topic ) extends CatalogProtocol
  case class CatalogedPlans( sourceId: Catalog#TID, plans: Set[OutlierPlan] ) extends CatalogProtocol

  case class AddPlan( override val targetId: Catalog#TID, planId: OutlierPlan#TID ) extends Command with CatalogProtocol
  case class RemovePlan( override val targetId: Catalog#TID, planId: OutlierPlan#TID ) extends Command with CatalogProtocol

  case class PlanAdded( override val sourceId: Catalog#TID, planId: OutlierPlan#TID ) extends Event with CatalogProtocol
  case class PlanRemoved( override val sourceId: Catalog#TID, planId: OutlierPlan#TID ) extends Event with CatalogProtocol


  object CatalogAggregateRoot {
    val builderFactory: EntityAggregateModule.BuilderFactory[CatalogState] = EntityAggregateModule.builderFor[CatalogState]

    val module: EntityAggregateModule[CatalogState] = {
      val b = builderFactory.make
      import b.P.{ Tag => BTag, Props => BProps, _ }

      b
      .builder
      .set( BTag, Catalog.idTag )
      .set( BProps, CatalogActor.props(_, _) )
      .set( IdLens, lens[CatalogState] >> 'id )
      .set( NameLens, lens[CatalogState] >> 'name )
      .set( SlugLens, Some(lens[CatalogState] >> 'slug) )
      .set( IsActiveLens, Some(lens[CatalogState] >> 'isActive) )
      .build()
    }


    object CatalogActor {
      def props( model: DomainModel, meta: AggregateRootType ): Props = {
        Props( new CatalogActor( model, meta ) with StackableStreamPublisher )
      }
    }

    class CatalogActor( override val model: DomainModel, override val meta: AggregateRootType )
    extends module.EntityAggregateActor { publisher: EventPublisher =>
      override var state: CatalogState = _

      var plansCache: Map[String, OutlierPlan] = Map.empty[String, OutlierPlan]


      override def preStart(): Unit = {
        super.preStart( )
        context.system.eventStream.subscribe( self, classOf[OutlierPlan.PlanChanged] )
        context.system.eventStream.subscribe( self, classOf[OutlierPlan.Added] )
        context.system.eventStream.subscribe( self, classOf[OutlierPlan.Disabled] )
        context.system.eventStream.subscribe( self, classOf[OutlierPlan.Enabled] )
      }

      override def receiveRecover: Receive = {
        case RecoveryCompleted => {
          val config = ConfigFactory.load()
          val configPlans: Set[OutlierPlan] = makePlans( config.getConfig(ConfigPaths.DETECTION_PLANS), detectionBudget(config) )
          val (oldPlans, newPlans) = configPlans partition { state.analysisPlans contains _.name }

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


      def loadExistingCacheElements( plans: Set[OutlierPlan] ): Future[Map[String, OutlierPlan]] = {
        val queries = {
          plans.toSeq
          .map { p =>
//                          Future { ( p.name, p) }
            val planRef = model.aggregateOf( OutlierPlan.module.aggregateRootType, p.id )
            val planSummary = ( planRef ? OutlierPlan.GetSummary ).mapTo[OutlierPlan.Summary].plan
            ( p.name, planSummary )
          }
        }

        Future.sequence( queries ) map { qs => Map( qs:_* ) }
      }

      def makeNewCacheElements( plans: Set[OutlierPlan] ): Future[Map[String, OutlierPlan]] = {
        val queries = {
          plans.toSeq
          .map { p =>
            val planRef = model.aggregateOf( OutlierPlan.module.aggregateRootType, p.id )
            val planSummary = ( planRef ? OutlierPlan.module.Add( p ) ).mapTo[OutlierPlan.module.Added].info
            ( p.name, planSummary )
          }
        }
      }

      //todo upon add watch plan actor for changes and incorporate them in to cache
      //todo the cache is what is sent out in reply to GetPlansForXYZ

      override val acceptance: Acceptance = entityAcceptance orElse {
        case (PlanAdded(_, pid), s) => s.copy( analysisPlans = s.analysisPlans + pid )
        case (PlanRemoved(_, pid), s) => s.copy( analysisPlans = s.analysisPlans - pid )
      }

      val catalog: Receive = {
        case AddPlan( id, pid ) => persist( PlanAdded(id, pid) ) { event => acceptAndPublish( event ) }
        case RemovePlan( id, pid ) => persist( PlanRemoved(id, pid) ) { event => acceptAndPublish( event ) }

        case GetPlansForTopic( _, topic ) => {
          val ps = plansCache collect { case (_, p) if p appliesTo topic => p }
          sender() ! CatalogedPlans( sourceId = state.id, plans = ps.toSet )
        }
      }

      override def active: Receive = catalog //super.active orElse catalog

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
              log.info( "topic[{}] default plan origin:[{}] line:[{}]", name, spec.origin, spec.origin.lineNumber )

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
              log.info( "topic:[{}] topic plan origin:[{}] line:[{}]", name, spec.origin, spec.origin.lineNumber )

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
              log.info( "topic:[{}] regex plan origin:[{}] line:[{}]", name, spec.origin, spec.origin.lineNumber )
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