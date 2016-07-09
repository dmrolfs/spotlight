package spotlight.analysis.outlier

import scala.reflect._
import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import demesne.module.entity.EntityAggregateModule
import demesne.module.entity.messages.EntityProtocol
import demesne.{AggregateRootType, DomainModel}
import demesne.register.StackableRegisterBusPublisher
import peds.akka.envelope._
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{EntityIdentifying, EntityLensProvider}
import peds.commons.identifier.ShortUUID
import shapeless.Lens
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.model.timeseries.{TimeSeries, Topic}


object AnalysisPlanProtocol extends EntityProtocol[OutlierPlan] {
    //todo add info change commands
    //todo reify algorithm
    //      case class AddAlgorithm( override val targetId: OutlierPlan#TID, algorithm: Symbol ) extends Command with AnalysisPlanMessage
    case class GetInfo( override val targetId: GetInfo#TID ) extends CommandMessage
    case class PlanInfo( override val sourceId: PlanInfo#TID, info: OutlierPlan ) extends EventMessage

    case class ApplyTo( override val targetId: ApplyTo#TID, appliesTo: OutlierPlan.AppliesTo ) extends CommandMessage

    case class UseAlgorithms(
      override val targetId: UseAlgorithms#TID,
      algorithms: Set[Symbol],
      algorithmConfig: Config
    ) extends CommandMessage

    case class ResolveVia(
      override val targetId: ResolveVia#TID,
      isQuorum: IsQuorum,
      reduce: ReduceOutliers
    ) extends CommandMessage


    case class ScopeChanged( override val sourceId: ScopeChanged#TID, appliesTo: OutlierPlan.AppliesTo ) extends EventMessage

    case class AlgorithmsChanged(
      override val sourceId: AlgorithmsChanged#TID,
      algorithms: Set[Symbol],
      algorithmConfig: Config
    ) extends EventMessage

    case class AnalysisResolutionChanged(
      override val sourceId: AnalysisResolutionChanged#TID,
      isQuorum: IsQuorum,
      reduce: ReduceOutliers
    ) extends EventMessage
}


/**
  * Created by rolfsd on 5/26/16.
  */
object AnalysisPlanModule extends EntityLensProvider[OutlierPlan] {
  implicit val identifying: EntityIdentifying[OutlierPlan] = {
    new EntityIdentifying[OutlierPlan] with ShortUUID.ShortUuidIdentifying[OutlierPlan] {
      override val evEntity: ClassTag[OutlierPlan] = classTag[OutlierPlan]
    }
  }


  override def idLens: Lens[OutlierPlan, OutlierPlan#TID] = OutlierPlan.idLens
  override def nameLens: Lens[OutlierPlan, String] = OutlierPlan.nameLens
  override def slugLens: Lens[OutlierPlan, String] = OutlierPlan.slugLens


  object AggregateRoot {
    val module: EntityAggregateModule[OutlierPlan] = {
      val b = EntityAggregateModule.builderFor[OutlierPlan].make
      import b.P.{ Tag => BTag, Props => BProps, _ }

      b
      .builder
      .set( BTag, identifying.idTag )
      .set( BProps, OutlierPlanActor.props(_, _) )
      .set( IdLens, OutlierPlan.idLens )
      .set( NameLens, OutlierPlan.nameLens )
      .set( IsActiveLens, Some(OutlierPlan.isActiveLens) )
      .build()
    }


    object OutlierPlanActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new AggregateRootActor(model, rootType) )

      class AggregateRootActor( model: DomainModel, rootType: AggregateRootType )
      extends OutlierPlanActor( model, rootType )
      with StackableStreamPublisher
      with StackableRegisterBusPublisher {
        override protected def onPersistRejected( cause: Throwable, event: Any, seqNr: Long ): Unit = {
          log.error(
            "Rejected to persist event type [{}] with sequence number [{}] for persistenceId [{}] due to [{}].",
            event.getClass.getName, seqNr, persistenceId, cause
          )
          throw cause
        }
      }
    }

    class OutlierPlanActor( override val model: DomainModel, override val rootType: AggregateRootType )
    extends module.EntityAggregateActor { publisher: EventPublisher =>
      import AnalysisPlanProtocol._

      override var state: OutlierPlan = _

      var scopeProxies: Map[Topic, ActorRef] = Map.empty[Topic, ActorRef]

      def proxyFor( topic: Topic ): ActorRef = {
        scopeProxies
        .get( topic )
        .getOrElse {
          val scope = OutlierPlan.Scope( plan = state, topic )
          val proxy = context.actorOf(
            AnalysisScopeProxy.props(
              scope = scope,
              plan = state,
              model = model,
              highWatermark = 10 * Runtime.getRuntime.availableProcessors(),
              bufferSize = 1000
            ),
            name = "analysis-scope-proxy-"+scope.toString
          )
          scopeProxies += topic -> proxy
          proxy
        }
      }

      ///// TEST
      override def entityAcceptance: Acceptance = {
        case (demesne.module.entity.messages.Added(_, info), s) => {
          log.info( "ACCEPTED ADDED: info=[{}]", info )
          context become LoggingReceive{ around( active ) }
          module.triedToEntity( info ) getOrElse s
        }
        case (demesne.module.entity.messages.Renamed(_, _, newName), s ) => module.nameLens.set( s )( newName )
        case (demesne.module.entity.messages.Reslugged(_, _, newSlug), s ) => module.slugLens map { _.set( s )( newSlug ) } getOrElse s
        case (_: demesne.module.entity.messages.Disabled, s) => {
          context become LoggingReceive { around( disabled ) }
          module.isActiveLens map { _.set( s )( false ) } getOrElse s
        }
        case (_: demesne.module.entity.messages.Enabled, s) => {
          context become LoggingReceive { around( active ) }
          module.isActiveLens map { _.set( s )( true ) } getOrElse s
        }
      }
      /////

      override def acceptance: Acceptance = entityAcceptance orElse {
        case (e: ScopeChanged, s) => {
          //todo: cast for expediency. my ideal is to define a Lens in the OutlierPlan trait; minor solace is this module is in the same package
          s.asInstanceOf[OutlierPlan.SimpleOutlierPlan].copy( appliesTo = e.appliesTo )
        }

        case (e: AlgorithmsChanged, s) => {
          //todo: cast for expediency. my ideal is to define a Lens in the OutlierPlan trait; minor solace is this module is in the same package
          s.asInstanceOf[OutlierPlan.SimpleOutlierPlan].copy(
            algorithms = e.algorithms,
            algorithmConfig = e.algorithmConfig
          )
        }

        case (e: AnalysisResolutionChanged, s) =>{
          //todo: cast for expediency. my ideal is to define a Lens in the OutlierPlan trait; minor solace is this module is in the same package
          s.asInstanceOf[OutlierPlan.SimpleOutlierPlan].copy(
            isQuorum = e.isQuorum,
            reduce = e.reduce
          )
        }
      }

      override def active: Receive = trace.block( "active" ) {
        workflow orElse planEntity orElse super.active
      }

      def workflow: Receive = {
        // forward to retain publisher sender
        case ts: TimeSeries => proxyFor( ts.topic ) forwardEnvelope (ts, OutlierPlan.Scope(plan = state, topic = ts.topic))
      }

      def planEntity: Receive = {
        case _: GetInfo => sender() !+ PlanInfo( state.id, state )

        case ApplyTo( id, appliesTo ) => persist( ScopeChanged(id, appliesTo) ) { e => acceptAndPublish( e ) }

        case UseAlgorithms( id, algorithms, config ) => {
          persist( AlgorithmsChanged(id, algorithms, config) ) { e => acceptAndPublish( e ) }
        }

        case ResolveVia( id, isQuorum, reduce ) => {
          persist( AnalysisResolutionChanged(id, isQuorum, reduce) ) { e => acceptAndPublish( e ) }
        }

//        case x => log.info( "PLAN_ENTITY UNHANDLED: {}", x )
      }

      override def unhandled( message: Any ): Unit = {
        val total = workflow orElse planEntity orElse super.active
        log.error(
          "UNHANDLED: [{}] (workflow,planEntity,super):[{}] total:[{}]",
          message,
          (workflow.isDefinedAt(message), planEntity.isDefinedAt(message), super.active.isDefinedAt(message)),
          total.isDefinedAt(message)
        )
        super.unhandled( message )
      }
    }
  }
}
