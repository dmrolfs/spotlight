package spotlight.checkpoint

import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import cats.Show
import cats.data.NonEmptyList
import demesne.{ AggregateRootType, CommandLike, EventLike }
import org.joda.{ time ⇒ joda }
import spotlight.SpotlightProtocol
import spotlight.analysis.algorithm.Algorithm
import spotlight.checkpoint.CheckpointProtocol.SavePointError

/** Created by rolfsd on 5/19/17.
  */
sealed trait CheckpointProtocol extends SpotlightProtocol

case class Checkpoint( token: String, sourceRef: ActorRef, start: joda.DateTime = joda.DateTime.now ) extends CheckpointProtocol

sealed trait CheckpointResponse extends CheckpointProtocol {
  def start: joda.DateTime
  def token: String
  def leadTime: FiniteDuration
  def isSuccess: Boolean
  def isFailure: Boolean = !isSuccess
}

case class CheckpointOpened( val targets: Set[Algorithm.TID] ) extends CheckpointProtocol

case class CheckpointAcknowledged(
    override val sourceId: CheckpointAcknowledged#TID
) extends CheckpointProtocol with EventLike {
  override type ID = Algorithm.ID
}

case class CheckpointSuccess(
    override val start: joda.DateTime,
    override val leadTime: FiniteDuration,
    override val token: String
) extends CheckpointResponse {
  override def isSuccess: Boolean = true
}

case class CheckpointFailure(
    override val start: joda.DateTime,
    override val leadTime: FiniteDuration,
    override val token: String,
    failures: NonEmptyList[SavePointError]
) extends CheckpointResponse {
  override def isSuccess: Boolean = false
}

object CheckpointFailure {
  implicit val showCheckpointFailure = new Show[CheckpointFailure] {
    override def show( f: CheckpointFailure ): String = {
      f
        .failures
        .toList
        .map { f0 ⇒ s"${f0.savePoint.aggregate.path.name}: ${f0.error.getClass.getName}:${f0.error.getMessage}" }
        .mkString( "[", ", ", "]" )
    }
  }
}

object CheckpointProtocol {
  case class SavePoint( aggregate: ActorRef, rootType: AggregateRootType )
  case class SavePointError( savePoint: SavePoint, error: Throwable )

  object Internal {
    sealed trait SavePointResponse extends CheckpointProtocol with EventLike {
      override type ID = Algorithm.ID
      def savePoint: SavePoint
      def isSuccess: Boolean
      def isFailure: Boolean = !isSuccess
    }

    case class SavePointSuccess(
        override val sourceId: SavePointSuccess#TID,
        override val savePoint: SavePoint
    ) extends SavePointResponse {
      override def isSuccess: Boolean = true
    }

    case class SavePointFailure(
        override val sourceId: SavePointFailure#TID,
        override val savePoint: SavePoint,
        error: Throwable
    ) extends SavePointResponse {
      override def isSuccess: Boolean = false
    }
  }
}
