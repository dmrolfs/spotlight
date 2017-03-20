package spotlight.infrastructure

import enumeratum._


sealed abstract class ClusterRole( override val entryName: String ) extends EnumEntry

object ClusterRole extends Enum[ClusterRole] {
  override val values: IndexedSeq[ClusterRole] = findValues

  case object Intake extends ClusterRole( "intake" )
  case object Analysis extends ClusterRole( "analysis" )
  case object Seed extends ClusterRole( "seed" )
  case object All extends ClusterRole( "all" )
}
