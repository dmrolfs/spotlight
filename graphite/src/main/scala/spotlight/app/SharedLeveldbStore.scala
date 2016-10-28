package spotlight.app

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.Done
import akka.actor.{ActorIdentity, ActorPath, ActorSystem, Identify, Props}
import akka.pattern.ask
import akka.persistence.journal.leveldb
import akka.util.Timeout
import demesne.BoundedContext


/**
  * Created by rolfsd on 10/25/16.
  */
object SharedLeveldbStore {
  val Name = "store"

  def start( path: ActorPath, startStore: Boolean = false )( implicit system: ActorSystem ): BoundedContext.StartTask = {
    val action = if ( startStore ) "started" else "connected to"

    val task = { bc: BoundedContext =>
      if ( startStore ) system.actorOf( Props[leveldb.SharedLeveldbStore], "store" )

      import system.dispatcher
      implicit val timeout = Timeout( 1.minute )

      ( system.actorSelection( path ) ? Identify( None ) )
      .mapTo[ActorIdentity]
      .onComplete {
        case Success( ActorIdentity(_, Some(ref)) ) => leveldb.SharedLeveldbJournal.setStore( ref, system )
        case Success( id ) => {
          system.log.error( "shared journal not {} at {}", action, path )
          system.terminate()
        }
        case Failure( ex ) => {
          system.log.error( "failure not {} shared journal at {}", action, path )
          system.terminate()
        }
      }

      Done
    }

    BoundedContext.StartTask( task, s"${action} shared LevelDB store" )
  }
}
