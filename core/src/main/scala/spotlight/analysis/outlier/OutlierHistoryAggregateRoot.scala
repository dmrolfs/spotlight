package spotlight.analysis.outlier

import akka.actor.Props
import akka.event.LoggingReceive
import peds.akka.publish.{ StackableStreamPublisher, EventPublisher }
import peds.commons.log.Trace
import demesne.register.StackableRegisterBusPublisher
import demesne.{ AggregateRootType, DomainModel }
import demesne.module.EntityAggregateModule
import spotlight.model.outlier.{ NoOutliers, OutlierAnnotation, OutlierHistory, Outliers }


/**
 * Created by rolfsd on 10/6/15.
 */
object OutlierHistoryAggregateRoot {
  val trace = Trace[OutlierHistoryAggregateRoot.type]

  val module: EntityAggregateModule[OutlierHistory] = trace.block( s"module" ) {
    val b = EntityAggregateModule.builderFor[OutlierHistory].make
    import b.P.{ Props => BProps, _ }
    b.builder
    .set( Tag, OutlierHistory.idTag )
    .set( BProps, OutlierHistoryActor.props(_: DomainModel, _: AggregateRootType) )
    .set( IdLens, OutlierHistory.idLens )
    .set( NameLens, OutlierHistory.nameLens )
    .build
  }


  sealed trait OutlierHistoryMessage

  sealed trait Command extends OutlierHistoryMessage {
    def targetId: OutlierHistory#TID
  }

  sealed trait Event extends OutlierHistoryMessage {
    def sourceId: OutlierHistory#TID
  }

  case class AddCleanPeriod( override val targetId: OutlierHistory#TID, clean: NoOutliers ) extends Command

  case class AddOutliers( override val targetId: OutlierHistory#TID, outliers: Outliers ) extends Command

  case class CleanPeriodAdded( override val sourceId: OutlierHistory#TID, clean: NoOutliers ) extends Event

  case class OutliersAdded( override val sourceId: OutlierHistory#TID, outliers: Outliers ) extends Event //todo: need to figure out what relevant here


  object OutlierHistoryActor {
    def props( model: DomainModel, meta: AggregateRootType ): Props = {
      Props( new OutlierHistoryActor( model, meta ) with StackableStreamPublisher with StackableRegisterBusPublisher )
    }
  }

  class OutlierHistoryActor( override val model: DomainModel, override val meta: AggregateRootType )
  extends module.EntityAggregateActor { publisher: EventPublisher =>
    override val acceptance: Acceptance = historyAcceptance orElse entityAcceptance

    override var state: OutlierHistory = _
    val algorithmName = self.path.name

    def historyAcceptance: Acceptance = {
      case (OutliersAdded(_, outliers), s) => trace.block( s"historyAcceptance.OutliersAdded" ) {
        val lens = OutlierHistory.outlierAnnotationsLens
        val annotations = lens get s
        val outlierAnnotations = ( OutlierAnnotation annotationsFromSeries outliers ) valueOr { exs => throw exs.head } //todo: det preferred handling of errors
        lens.set( s )( annotations ++ outlierAnnotations )
      }

      case (CleanPeriodAdded(_, series), s) => trace.block( s"historyAcceptance.CleanPeriodAdded" ) {
        val lens = OutlierHistory.outlierAnnotationsLens
        val annotations = lens get s
        val clean = OutlierAnnotation.cleanAnnotation( series.source.start, series.source.end ) valueOr { exs =>
          //todo: det preferred handling of errors
          throw exs.head
        }
        lens.set( s )( annotations :+ clean )
      }
    }


    override def active: Receive = history orElse super.active

    val history: Receive = LoggingReceive {
      case AddCleanPeriod(id, clean) => persistAsync( CleanPeriodAdded(id, clean) ) { e => acceptAndPublish( e ) }
      case AddOutliers(id, outliers) => persistAsync( OutliersAdded(id, outliers) ) { e => acceptAndPublish( e ) }
    }
  }
}
