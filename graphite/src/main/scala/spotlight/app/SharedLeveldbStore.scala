package spotlight.app

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.Done
import akka.actor.{ActorIdentity, ActorPath, Identify, Props}
import akka.pattern.ask
import akka.persistence.journal.leveldb
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import peds.commons.concurrent._
import demesne.{BoundedContext, StartTask}
import spotlight.stream.Configuration


/**
  * Created by rolfsd on 10/25/16.
  */
object SharedLeveldbStore extends StrictLogging {
  val Name = "store"

  def start( startStore: Boolean = false ): StartTask = {
    val action = if ( startStore ) "started" else "connected to"
    StartTask.withBoundTask( s"${action} shared LevelDB ${Name}" ) { bc: BoundedContext =>
      logger.info( "TEST:akka.persistence.journal.plugin: [{}]", bc.configuration.getString("akka.persistence.journal.plugin") )
      val system = bc.system
      implicit val ec = system.dispatcher
      implicit val timeout = Timeout( 1.minute )
      val clusterPort = bc.configuration.asInstanceOf[Configuration].clusterPort
      val path = ActorPath fromString s"akka.tcp://${system.name}@127.0.0.1:${clusterPort}/user/${SharedLeveldbStore.Name}"
      val identifyStore = {
        if ( startStore ) {
          val ref = system.actorOf( Props[leveldb.SharedLeveldbStore], Name )
          logger.info( "started shared leveldb {}: [{}]", Name, ref )
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
