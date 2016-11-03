package spotlight.app

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.Done
import akka.actor.{ActorIdentity, ActorPath, ActorSystem, Identify, Props}
import akka.pattern.ask
import akka.persistence.journal.leveldb
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import peds.commons.concurrent._
import demesne.{BoundedContext, StartTask}


/**
  * Created by rolfsd on 10/25/16.
  */
object SharedLeveldbStore extends StrictLogging {
  val Name = "store"

  def start( path: ActorPath, startStore: Boolean = false )( implicit system: ActorSystem ): StartTask = {
    val action = if ( startStore ) "started" else "connected to"
    import system.dispatcher
    implicit val timeout = Timeout( 1.minute )

    StartTask.withBoundTask( s"${action} shared LevelDB ${Name}" ) { bc: BoundedContext =>
      val identifyStore = {
        if ( startStore ) {
          val ref = system.actorOf( Props[leveldb.SharedLeveldbStore], Name )
          logger.info( s"started shared leveldb ${Name}: [{}]", ref )
        }

        ( system.actorSelection( path ) ? Identify( None ) ).mapTo[ActorIdentity]
      }

      identifyStore onComplete {
        case Success( ActorIdentity(_, Some(ref)) ) => {
          logger.info( "setting reference and system with shared leveldb journal {}...", Name )
          leveldb.SharedLeveldbJournal.setStore( ref, system )
        }
        case Success( id ) => {
          system.log.error( "shared journal {} not {} at {}", Name, action, path )
          system.terminate()
        }
        case Failure( ex ) => {
          system.log.error( "failure not {} shared journal {} at {}", action, Name, path )
          system.terminate()
        }
      }

      val result = identifyStore map { _ => Done }
      result.toTask
    }
  }
}
