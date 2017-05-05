package spotlight.analysis.algorithm

import akka.actor.{ ActorPath, NotInfluenceReceiveTimeout }
import squants.information.{ Bytes, Information }
import demesne.{ AggregateProtocol, MessageLike }
import spotlight.model.timeseries.{ DataPoint, ThresholdBoundary, Topic }

/** Created by rolfsd on 2/17/17.
  */
object AlgorithmProtocol extends AggregateProtocol[Algorithm.ID] {
  case class RouteMessage( override val targetId: RouteMessage#TID, payload: Any ) extends MessageLike {
    override type ID = Algorithm.ID
  }

  case class EstimateSize( override val targetId: EstimateSize#TID ) extends Message with NotInfluenceReceiveTimeout

  case class EstimatedSize(
      val sourceId: Algorithm.TID,
      nrShapes: Int,
      size: Information
  ) extends ProtocolMessage with NotInfluenceReceiveTimeout {
    def averageSizePerShape: Information = if ( nrShapes != 0 ) ( size / nrShapes ) else Bytes( 0 )
  }

  object EstimatedSize {
    implicit val ordering = new scala.math.Ordering[EstimatedSize] {
      override def compare( lhs: EstimatedSize, rhs: EstimatedSize ): Int = {
        val sizeComparison = lhs.size compare rhs.size
        if ( sizeComparison != 0 ) sizeComparison
        else lhs.nrShapes compare rhs.nrShapes
      }
    }
  }

  case class GetTopicShapeSnapshot(
    override val targetId: GetTopicShapeSnapshot#TID,
    topic: Topic
  ) extends Command with NotInfluenceReceiveTimeout

  case class TopicShapeSnapshot(
    override val sourceId: TopicShapeSnapshot#TID,
    algorithm: String,
    topic: Topic,
    snapshot: Option[Algorithm[_]#Shape]
  ) extends Event

  case class GetStateSnapshot( override val targetId: Algorithm.TID ) extends Command with NotInfluenceReceiveTimeout

  import scala.language.existentials
  case class StateSnapshot(
    override val sourceId: StateSnapshot#TID,
    snapshot: AlgorithmState[_]
  ) extends Event with NotInfluenceReceiveTimeout

  case class Advanced(
    override val sourceId: Advanced#TID,
    topic: Topic,
    point: DataPoint,
    isOutlier: Boolean,
    threshold: ThresholdBoundary
  ) extends Event

  case class AlgorithmUsedBeforeRegistrationError(
    sourceId: TID,
    algorithm: String,
    path: ActorPath
  ) extends IllegalStateException(
    s"actor [${path}] not registered algorithm [${algorithm}] with scope [${sourceId}] before use"
  ) with OutlierAlgorithmError
}
