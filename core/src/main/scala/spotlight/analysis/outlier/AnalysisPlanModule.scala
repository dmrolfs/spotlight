package spotlight.analysis.outlier

import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive
import akka.persistence.RecoveryCompleted
import demesne.{AggregateRootType, DomainModel}
import demesne.module.EntityAggregateModule
import demesne.register.StackableRegisterBusPublisher
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import spotlight.model.outlier.OutlierPlan


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
      //todo add plan change commands
      //todo reify algorithm
      //      case class AddAlgorithm( override val targetId: OutlierPlan#TID, algorithm: Symbol ) extends Command with PlanProtocol
      case object GetSummary extends PlanProtocol
      case class Summary( sourceId: OutlierPlan#TID, plan: OutlierPlan ) extends PlanProtocol

      case class PlanChanged( override val sourceId: OutlierPlan#TID, plan: OutlierPlan ) extends Event with PlanProtocol
    }


    object OutlierPlanActor {
      def props( model: DomainModel, meta: AggregateRootType ): Props = {
        Props( new OutlierPlanActor( model, meta ) with StackableStreamPublisher with StackableRegisterBusPublisher )
      }
    }

    class OutlierPlanActor( override val model: DomainModel, override val meta: AggregateRootType )
      extends module.EntityAggregateActor { publisher: EventPublisher =>
      import Protocol._

      override var state: OutlierPlan = _

      override def receiveRecover: Receive = {
        case RecoveryCompleted => {

        }
      }

      case class Workers( detector: ActorRef, router: ActorRef, algorithms: Set[ActorRef] )
      def makePlanWorkers(): Workers = {
        def makeRouter(): ActorRef = {
          context.actorOf(
            DetectionAlgorithmRouter.props.withDispatcher( "outlier-detection-dispatcher" ),
            s"router-${state.name}"
          )
        }

        def makeAlgorithms( routerRef: ActorRef ): Set[ActorRef] = {

        }

        def makeDetector( routerRef: ActorRef ): ActorRef = {

        }
      }


      override val quiescent: Receive = {
        case Protocol.Entity.Add( plan ) => {
          persist( Protocol.Entity.Added(plan) ) { e =>
            acceptAndPublish( e )
            context become LoggingReceive { around( active ) }
          }
WORK HERE to makePlanWorkers in right spot (in or out of persist block)
        }
      }
                                                                                                                            .quiescent

      //todo add plan modification
      val plan: Receive = {
        case GetSummary => sender() ! Summary( state.id, state )
      }

      override def active: Receive = plan orElse super.active
    }
  }
}
