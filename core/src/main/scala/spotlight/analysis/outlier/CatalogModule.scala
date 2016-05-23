package spotlight.analysis.outlier

import akka.actor.Props
import com.typesafe.config.Config
import demesne.{AggregateRootModule, AggregateRootType, DomainModel}
import demesne.module.EntityAggregateModule
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityCompanion}
import peds.commons.identifier.{ShortUUID, TaggedID}
import shapeless._
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.Topic


/**
  * Created by rolfsd on 5/20/16.
  */
trait Catalog extends Entity {
  override type ID = ShortUUID
  override def idClass: Class[_] = classOf[ShortUUID]
  def isActive: Boolean
  def analysisPlans: Set[OutlierPlan#TID]
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
    override val analysisPlans: Set[OutlierPlan#TID] = Set.empty[OutlierPlan#TID],
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

      var plansCache: Set[OutlierPlan] = Set.empty[OutlierPlan]

      //todo upon add watch plan actor for changes and incorporate them in to cache
      //todo the cache is what is sent out in reply to GetPlansForXYZ

      override val acceptance: Acceptance = entityAcceptance orElse {
        case (PlanAdded(_, pid), s) => s.copy( analysisPlans = s.analysisPlans + pid )
        case (PlanRemoved(_, pid), s) => s.copy( analysisPlans = s.analysisPlans - pid )
      }

      override def active: Receive = super.active orElse {
        case AddPlan( id, pid ) => persist( PlanAdded(id, pid) ) { event => acceptAndPublish( event ) }
        case RemovePlan( id, pid ) => persist( PlanRemoved(id, pid) ) { event => acceptAndPublish( event ) }

        case GetPlansForTopic( _, topic ) => {
          sender() ! CatalogedPlans( sourceId = state.id, plans = plansCache collect { case p if p appliesTo topic => p } )
        }
      }
    }
  }
}