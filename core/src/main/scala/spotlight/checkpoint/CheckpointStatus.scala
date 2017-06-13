package spotlight.checkpoint

import scala.collection.parallel.immutable
import akka.persistence.fsm.PersistentFSM.FSMState
import enumeratum._


/**
  * Created by rolfsd on 6/12/17.
  */
sealed trait CheckpointStatus extends EnumEntry with FSMState

object CheckpointStatus extends Enum[CheckpointStatus] {
  override def values: immutable.IndexedSeq[CheckpointStatus] = findValues

  case object Quiescent extends CheckpointStatus { override val identifier: String = "quiescent" }
  case object Pending extends CheckpointStatus { override val identifier: String = "pending" }
  case object Open extends CheckpointStatus { override val identifier: String = "open" }
  case object InProgress extends CheckpointStatus { override val identifier: String = "in-progress" }
  case object Marked extends CheckpointStatus { override val identifier: String = "marked" }
  case object Failed  extends CheckpointStatus { override val identifier: String = "failed" }
}
