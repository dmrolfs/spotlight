package spotlight.analysis.outlier.algorithm

import akka.actor.Props
import demesne.{AggregateMessage, AggregateProtocol, AggregateRootType, DomainModel}


/**
  * Created by rolfsd on 12/21/16.
  */
object AlgorithmShardProtocol extends AggregateProtocol[AlgorithmShardCatalog#ID] {
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


object AlgorithmShardCatalog {






  sealed trait Control {
  }

  case class ControlBySize( thresholdMB: Int ) extends Control

need to use demesne entity module
  def props( model: DomainModel, rootType: AggregateRootType, control: Control ): Props = {
    ???
  }

  def name( rootType: AggregateRootType ): String = rootType.name + "ShardCatalog"
}

class AlgorithmShardCatalog {

}
