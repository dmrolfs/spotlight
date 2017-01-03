package spotlight.analysis.outlier.algorithm

import scala.reflect._
import akka.actor.Props
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.MetricName
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.archetype.domain.model.core.Entity
import peds.commons.identifier._
import demesne.module.{LocalAggregate, SimpleAggregateModule}
import demesne.{AggregateMessage, AggregateProtocol, AggregateRootType, DomainModel}
import peds.akka.publish.EventPublisher
import spotlight.model.outlier.OutlierPlan



/**
  * Created by rolfsd on 12/21/16.
  */
object AlgorithmShardProtocol extends AggregateProtocol[AlgorithmShardCatalog.ID] {
  sealed trait AlgorithmShardMessage
  sealed abstract class AlgorithmShardCommand extends AlgorithmShardMessage with CommandMessage
  sealed abstract class AlgorithmShardEvent extends AlgorithmShardMessage with EventMessage

  case class RouteMessage( override val targetId: RouteMessage#TID, payload: Any )
  extends AggregateMessage with AlgorithmShardMessage


  case class UseControl(
    override val targetId: UseControl#TID,
    control: AlgorithmShardCatalog.Control
  ) extends AlgorithmShardCommand

  case class ControlSet(
    override val sourceId: ControlSet#TID,
    control: AlgorithmShardCatalog.Control
  ) extends AlgorithmShardEvent
}


object AlgorithmShardCatalog extends Instrumented with LazyLogging {
  type ID = ShardCatalog#ID
  type TID = ShardCatalog#TID

  def idFor( plan: OutlierPlan.Summary ): TID = {
    val Tid = identifying.evTID
    val Tid( pid ) = plan.id
    identifying.tidAs[TID]( identifying.tag( pid ) ).toOption.get
  }

  override lazy val metricBaseName: MetricName = MetricName( getClass )

  sealed trait Control { }

  object Control {
    final case class ControlBySize private[Control]( thresholdMB: Int ) extends Control
  }


  case class ShardCatalog(
    override val id: ShardCatalog#TID,
    override val name: String
  ) extends Entity with Equals {
    override type ID = OutlierPlan#ID
    override val evID: ClassTag[ID] = classTag[ID]
    override val evTID: ClassTag[TID] = classTag[TID]

    override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[ShardCatalog]

    override def hashCode: Int = 41 + id.##

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: ShardCatalog => {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.id == that.id )
          }
        }

        case _ => false
      }
    }

    override def toString(): String = WORK HERE
  }

  implicit val identifying: Identifying[ShardCatalog] = {
    new Identifying[ShardCatalog] with ShortUUID.ShortUuidIdentifying[ShardCatalog] {
      override val idTag: Symbol = Symbol( "shard-catalog" )
      override def idOf( o: ShardCatalog ): TID = tag( o.id )
    }
  }


  val module: SimpleAggregateModule[ShardCatalog] = {
    val b = SimpleAggregateModule.builderFor[ShardCatalog].make
    import b.P.{ Tag => BTag, Props => BProps, _ }

    b
    .builder
    .set( BTag, identifying.idTag )
    .set( Environment, LocalAggregate )
    .set( BProps, AggregateRoot.ShardCatalogActor.props(_, _) )
    .build()
  }


  object AggregateRoot {

    object ShardCatalogActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = ???

      def name( rootType: AggregateRootType ): String = rootType.name + "ShardCatalog"
    }

    class ShardCatalogActor( override val model: DomainModel, override val rootType: AggregateRootType )
    extends demesne.AggregateRoot[ShardCatalog, ShardCatalog#ID]
    with InstrumentedActor
    with demesne.AggregateRoot.Provider {
      outer: EventPublisher =>

      import spotlight.analysis.outlier.algorithm.{ AlgorithmShardProtocol => P }

      override lazy val metricBaseName: MetricName = MetricName( classOf[ShardCatalogActor] )

      override def parseId( idrep: String ): TID = identifying.safeParseTid[TID]( idrep )( classTag[TID] )

      override var state: ShardCatalog = ShardCatalog( id = aggregateId, name = self.path.name )

    }
  }





  def props(model: DomainModel, rootType: AggregateRootType, control: Control): Props = {
    ???
  }

}
