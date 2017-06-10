package spotlight.checkpoint

import akka.actor.ActorRef

import scala.concurrent.duration.FiniteDuration
import cats.Show
import cats.data.NonEmptyList
import demesne.{ CommandLike, EventLike }
import omnibus.commons.identifier.ShortUUID
import org.joda.{ time ⇒ joda }
import spotlight.SpotlightProtocol
import spotlight.analysis.algorithm.Algorithm

/** Created by rolfsd on 5/19/17.
  */
sealed trait CheckpointProtocol extends SpotlightProtocol

case class Checkpoint( label: String, start: joda.DateTime = joda.DateTime.now ) extends CheckpointProtocol

case class CheckpointMarked( label: String, start: joda.DateTime, leadTime: FiniteDuration ) extends CheckpointProtocol

case class CheckpointFailure(
  label: String,
  start: joda.DateTime,
  leadTime: FiniteDuration,
  failures: NonEmptyList[( ActorRef, Throwable )]
) extends CheckpointProtocol

object CheckpointFailure {
  implicit val showCheckpointFailure = new Show[CheckpointFailure] {
    override def show( f: CheckpointFailure ): String = {
      f
        .failures
        .toList
        .map { f0 ⇒ s"${f0._1.path.name}: ${f0._2.getClass.getName}:${f0._2.getMessage}" }
        .mkString( "[", ", ", "]" )
    }
  }
}

case class MarkCheckpoint(
    override val targetId: MarkCheckpoint#TID,
    label: String,
    correlationId: ShortUUID
) extends CheckpointProtocol with CommandLike {
  override type ID = Algorithm.ID
}

case class CheckpointAcknowledged(
    override val sourceId: CheckpointAcknowledged#TID,
    correlationId: ShortUUID
) extends CheckpointProtocol with EventLike {
  override type ID = Algorithm.ID
}

//sealed trait CheckpointResponse extends CheckpointProtocol {
//  def start: joda.DateTime
//  def token: String
//  def leadTime: FiniteDuration
//  def isSuccess: Boolean
//  def isFailure: Boolean = !isSuccess
//}
//
//case class CheckpointOpened( val targets: Set[Algorithm.TID] ) extends CheckpointProtocol
//
//case class CheckpointAcknowledged(
//    override val sourceId: CheckpointAcknowledged#TID
//) extends CheckpointProtocol with EventLike {
//  override type ID = Algorithm.ID
//}
//
//case class CheckpointSuccess(
//    override val start: joda.DateTime,
//    override val leadTime: FiniteDuration,
//    override val token: String
//) extends CheckpointResponse {
//  override def isSuccess: Boolean = true
//}
//
//case class CheckpointFailure(
//    override val start: joda.DateTime,
//    override val leadTime: FiniteDuration,
//    override val token: String,
//    failures: NonEmptyList[SavePointError]
//) extends CheckpointResponse {
//  override def isSuccess: Boolean = false
//}
//
//
//object CheckpointProtocol {
//  case class SavePoint( aggregate: ActorRef, rootType: AggregateRootType )
//  case class SavePointError( savePoint: SavePoint, error: Throwable )
//
//  object Internal {
//    sealed trait SavePointResponse extends CheckpointProtocol with EventLike {
//      override type ID = Algorithm.ID
//      def savePoint: SavePoint
//      def isSuccess: Boolean
//      def isFailure: Boolean = !isSuccess
//    }
//
//    case class SavePointSuccess(
//        override val sourceId: SavePointSuccess#TID,
//        override val savePoint: SavePoint
//    ) extends SavePointResponse {
//      override def isSuccess: Boolean = true
//    }
//
//    case class SavePointFailure(
//        override val sourceId: SavePointFailure#TID,
//        override val savePoint: SavePoint,
//        error: Throwable
//    ) extends SavePointResponse {
//      override def isSuccess: Boolean = false
//    }
//  }
//}
