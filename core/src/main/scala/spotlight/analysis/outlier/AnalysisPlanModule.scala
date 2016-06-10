package spotlight.analysis.outlier

import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import demesne.{AggregateRootType, DomainModel}
import demesne.module.EntityAggregateModule
import demesne.register.StackableRegisterBusPublisher
import peds.akka.envelope._
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.model.timeseries.{TimeSeries, Topic}


/**
  * Created by rolfsd on 5/26/16.
  */
object AnalysisPlanModule {
  object AggregateRoot {
    val module: EntityAggregateModule[OutlierPlan] = {
      val b = EntityAggregateModule.builderFor[OutlierPlan].make
      import b.P.{ Tag => BTag, Props => BProps, _ }

      b
      .builder
      .set( BTag, OutlierPlan.idTag )
      .set( BProps, OutlierPlanActor.props(_, _) )
      .set( IdLens, OutlierPlan.idLens )
      .set( NameLens, OutlierPlan.nameLens )
      .set( IsActiveLens, Some(OutlierPlan.isActiveLens) )
      .build()
    }

    object Protocol extends module.Protocol {
      sealed trait PlanProtocol
      //todo add info change commands
      //todo reify algorithm
      //      case class AddAlgorithm( override val targetId: OutlierPlan#TID, algorithm: Symbol ) extends Command with PlanProtocol
      case object GetInfo extends PlanProtocol
      case class PlanInfo( sourceId: module.TID, info: OutlierPlan ) extends PlanProtocol

      case class ApplyTo( override val targetId: ApplyTo#TID, appliesTo: OutlierPlan.AppliesTo ) extends Command with PlanProtocol

      case class UseAlgorithms( override val targetId: UseAlgorithms#TID, algorithms: Set[Symbol], algorithmConfig: Config )
        extends Command with PlanProtocol

      case class ResolveVia( override val targetId: ResolveVia#TID, isQuorum: IsQuorum, reduce: ReduceOutliers )
        extends Command with PlanProtocol


      case class ScopeChanged( override val sourceId: ScopeChanged#TID, appliesTo: OutlierPlan.AppliesTo )
        extends Event with PlanProtocol

      case class AlgorithmsChanged(
        override val sourceId: AlgorithmsChanged#TID,
        algorithms: Set[Symbol],
        algorithmConfig: Config
      ) extends Event with PlanProtocol

      case class AnalysisResolutionChanged(
        override val sourceId: AnalysisResolutionChanged#TID,
        isQuorum: IsQuorum,
        reduce: ReduceOutliers
      ) extends Event with PlanProtocol
    }


    object OutlierPlanActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = {
        Props( new OutlierPlanActor( model, rootType ) with StackableStreamPublisher with StackableRegisterBusPublisher )
      }
    }

    class OutlierPlanActor( override val model: DomainModel, override val rootType: AggregateRootType )
    extends module.EntityAggregateActor { publisher: EventPublisher =>
      import Protocol._

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

      override def active: Receive = workflow orElse planEntity orElse super.active

      val workflow: Receive = {
        // forward to retain publisher sender
        case ts: TimeSeries => proxyFor( ts.topic ) forwardEnvelope (ts, OutlierPlan.Scope(plan = state, topic = ts.topic))
      }

      val planEntity: Receive = {
        case GetInfo => sender( ) !+ PlanInfo( state.id, state )

        case ApplyTo( id, appliesTo ) => persist( ScopeChanged(id, appliesTo) ) { e => acceptAndPublish( e ) }

        case UseAlgorithms( id, algorithms, config ) => {
          persist( AlgorithmsChanged(id, algorithms, config) ) { e => acceptAndPublish( e ) }
        }

        case ResolveVia( id, isQuorum, reduce ) => {
          persist( AnalysisResolutionChanged(id, isQuorum, reduce) ) { e => acceptAndPublish( e ) }
        }
      }
    }
  }
}
