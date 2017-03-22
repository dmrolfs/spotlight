package spotlight.infrastructure

import scala.collection.immutable
import enumeratum._

sealed abstract class ClusterRole( override val entryName: String, val hostsFlow: Boolean ) extends EnumEntry

object ClusterRole extends Enum[ClusterRole] {
  override val values: immutable.IndexedSeq[ClusterRole] = findValues

  case object Intake extends ClusterRole( entryName = "intake", hostsFlow = true )
  case object Analysis extends ClusterRole( entryName = "analysis", hostsFlow = false )
  case object Seed extends ClusterRole( entryName = "seed", hostsFlow = false )
  case object All extends ClusterRole( entryName = "all", hostsFlow = true )

  implicit val readClusterRole: scopt.Read[ClusterRole] = scopt.Read.reads( ClusterRole withName _ )
}
