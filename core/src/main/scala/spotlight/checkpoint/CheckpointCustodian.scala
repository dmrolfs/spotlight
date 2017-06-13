package spotlight.checkpoint

import akka.actor.ActorRef

import scala.concurrent.duration._
import akka.persistence.fsm.PersistentFSM
import org.joda.{time => joda}
import org.joda.time.DateTime

import scala.reflect.ClassTag


/**
  * Created by rolfsd on 6/11/17.
  */


sealed trait CheckpointJournal {
  def addCheckpoint( client: ActorRef, label: String, start: joda.DateTime ): CheckpointJournal
  def setCheckpointStatus( label: String, status: CheckpointStatus ): CheckpointJournal
}

private[checkpoint] case object EmptyCheckpointJournal extends CheckpointJournal {
  override def addCheckpoint( client: ActorRef, label: String, start: DateTime ): CheckpointJournal = {
    NonEmptyCheckpointJournal().addCheckpoint( client, label, start )
  }

  override def setCheckpointStatus( label: String, status: CheckpointStatus ): CheckpointJournal = this
}

private[checkpoint] case class CheckpointEntry( client: ActorRef, label: String, start: joda.DateTime, status: CheckpointStatus )

private[checkpoint] case class NonEmptyCheckpointJournal(
  entries: Map[String, CheckpointEntry] = Map.empty[String, CheckpointEntry ]
) extends CheckpointJournal {
  override def addCheckpoint( client: ActorRef, label: String, start: DateTime ): CheckpointJournal = {
    this.copy( entries = entries + ( label -> CheckpointEntry(client, label, start, CheckpointStatus.Pending )))
  }

  override def setCheckpointStatus( label: String, status: CheckpointStatus ): CheckpointJournal = {
    entries
    .get( label )
    .map { e => e.copy( status = status ) }
    .map { newEntry => this.copy( entries = entries + ( label -> newEntry ) ) }
    .getOrElse { this }
  }
}

//todo make into cluster singleton
//todo big problem for checkpointing in clustered environment:
//todo the checkpoint custodian is a cluster singleton in order to support a single process that applies to the system. It doesn't
//todo make a lot of sense to checkpoint relative to one cluster, incl halting flow, only to have flow continue on other intake
//todo nodes. Valve, on the other hand are plugged into the stream flow in each intake node.
//todo custodian and valve works okay for a single node, however for a clustered environment, it would help to have a
//todo cluster-singleton valve control that each valve instance would register with. The custodian then shuts valves via the
//todo singleton control point. This is possible, but adds a lot of complexity and suggests a point where migrating to Flink may
//todo the right direction for a large scale capability.
//todo See design for actor collaboration
class CheckpointCustodian extends PersistentFSM[CheckpointStatus, CheckpointJournal, CheckpointProtocol] {
  import CheckpointStatus._

  override implicit val domainEventClassTag: ClassTag[CheckpointProtocol] = ClassTag( classOf[CheckpointProtocol] )

  // need to determine how to scale checkpoint process beyond one intake node;
  // perhaps via clustersingleton valve control & corr intake valve registration (listen to cluster node event)
  override val persistenceId: String = "CheckpointCustodian"


  startWith( Quiescent, EmptyCheckpointJournal )

  when( Quiescent ) {
    case Event( Checkpoint(label, start), state ) => goto( Pending ) applying( CheckpointAdded(label, sender(), start) ) forMax(1.minute)
    case Event( RecoverCheckpoint(label), state ) => stay replying state
  }

  when( Pending ) {
    //todo WORK HERE -- see note above
  }

  when( Open ) { ??? }

  when( InProgress ) { ??? }

  override def applyEvent( event: CheckpointProtocol, journalBeforeEvent: CheckpointJournal ): CheckpointJournal = {
    event match {
      case CheckpointAdded( label, client, start ) => journalBeforeEvent.addCheckpoint( client, label, start )
      case CheckpointStatusChanged( label, status ) => journalBeforeEvent.setCheckpointStatus( label, status )
    }
  }
}


