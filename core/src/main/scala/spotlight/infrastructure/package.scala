package spotlight

import scala.collection.immutable.IndexedSeq
import akka.Done
import demesne.StartTask
import enumeratum._

package object infrastructure {
  sealed abstract class ClusterRole( override val entryName: String ) extends EnumEntry

  object ClusterRole extends Enum[ClusterRole] {
    override val values: IndexedSeq[ClusterRole] = findValues

    case object Control extends ClusterRole( "control" )
    case object Worker extends ClusterRole( "worker" )
  }

  // val kamonStartTask: StartTask = StartTask.withFunction( "start Kamon monitoring" ) { bc ⇒ kamon.Kamon.start(); Done }
}
