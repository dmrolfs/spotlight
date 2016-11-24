package spotlight.app

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.Done
import akka.actor.{ActorIdentity, ActorPath, ActorRef, Identify, Props}
import akka.persistence.journal.leveldb
import akka.util.Timeout

import scalaz.concurrent.Task
import com.typesafe.scalalogging.{Logger, StrictLogging}
import peds.commons.concurrent._
import demesne.{BoundedContext, StartTask}
import spotlight.stream.Settings



/**
  * Created by rolfsd on 10/25/16.
  */
object SharedLeveldbStore extends StrictLogging {
  val Name = "store"

  val configLogger: Logger = Logger( "Config" )

  def start( createActor: Boolean = false ): StartTask = {
    val actionLabel = if ( createActor ) "create" else "connect"
    StartTask.withBoundTask( s"${actionLabel} shared LevelDB ${Name}" ) { implicit bc: BoundedContext =>
      val clusterPort = {
        val ClusterPortPath = "spotlight.settings.cluster-port"
        if ( bc.configuration hasPath ClusterPortPath ) bc.configuration.getInt( ClusterPortPath ) else 2551
      }

      for {
        _ <- logConfiguration()
        ref <- startStore( SharedLeveldbStore.Name, createActor )
        path = ref map { _.path } getOrElse {
          ActorPath fromString s"akka.tcp://${bc.system.name}@127.0.0.1:${clusterPort}/user/${SharedLeveldbStore.Name}"
        }
        id <- identifyStore( path, actionLabel )
      } yield Done
    }
  }

  private def logConfiguration()( implicit bc: BoundedContext ): Task[Unit] = {
    Task now {
      configLogger.info( bc.configuration.root.render )

      val JournalPluginPath = "akka.persistence.journal.plugin"
      logger.info( "A TEST: looking at {}", JournalPluginPath )
      val journalPlugin = {
        if ( bc.configuration hasPath JournalPluginPath ) Some( bc.configuration getString JournalPluginPath )
        else None
      }
      logger.info( "SharedLeveldbStore: akka.persistence.journal.plugin: [{}]", journalPlugin )

      val JournalDirPath = "akka.persistence.journal.leveldb-shared.store.dir" // JournalPluginPath+".store.dir"
      val journalDir = if ( bc.configuration hasPath JournalDirPath ) Some( bc.configuration.getString(JournalDirPath) ) else None
      logger.info( "SharedLeveldbStore: journal dir: [{}]", journalDir )

      val SnapshotsDirPath = "akka.persistence.snapshot-store.local.dir"
      val snapshotDir = if ( bc.configuration hasPath SnapshotsDirPath ) Some( bc.configuration getString SnapshotsDirPath ) else None
      logger.info( "SharedLeveldbStore: snapshots dir: [{}]", snapshotDir )
    }
  }

  private def startStore( name: String, create: Boolean )( implicit bc: BoundedContext ): Task[Option[ActorRef]] = {
    Task now {
      if ( !create ) None
      else {
        val ref = bc.system.actorOf( Props[leveldb.SharedLeveldbStore], Name )
        logger.info( "started shared leveldb {}: [{}]", Name, ref )
        Option( ref )
      }
    }
  }

  private def identifyStore( path: ActorPath, actionLabel: String )( implicit bc: BoundedContext ): Task[ActorIdentity] = {
    import akka.pattern.ask
    implicit val dispatcher = bc.system.dispatcher
    implicit val timeout = Timeout( 1.minute )
    val id = ( bc.system.actorSelection( path ) ? Identify(None) ).mapTo[ActorIdentity]
    id onComplete {
      case Success( ActorIdentity(_, Some(ref)) ) => {
        logger.info( "setting reference and system with shared leveldb journal {}...", Name )
        leveldb.SharedLeveldbJournal.setStore( ref, bc.system )
      }

      case Success( id ) => {
        bc.system.log.error( "shared journal {} not {} at {}", Name, actionLabel, path )
        bc.system.terminate()
      }

      case Failure( ex ) => {
        bc.system.log.error( "failure not {} shared journal {} at {}", actionLabel, Name, path )
        bc.system.terminate()
      }
    }

    id.toTask
  }
}
