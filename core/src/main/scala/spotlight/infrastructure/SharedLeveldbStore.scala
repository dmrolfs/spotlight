package spotlight.infrastructure

import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import akka.Done
import akka.actor.{ ActorIdentity, ActorPath, ActorRef, Identify, Props }
import akka.persistence.journal.leveldb
import akka.util.Timeout

import monix.eval.Task
import net.ceedubs.ficus.Ficus._
import com.typesafe.scalalogging.{ Logger, StrictLogging }
import omnibus.commons.concurrent._
import demesne.{ BoundedContext, StartTask }

/** Created by rolfsd on 10/25/16.
  */
object SharedLeveldbStore extends StrictLogging {
  val Name = "store"

  val configLogger: Logger = Logger( "Config" )

  def start( createActor: Boolean = false ): StartTask = {
    val actionLabel = if ( createActor ) "create" else "connect"
    StartTask.withBoundTask( s"${actionLabel} shared LevelDB ${Name}" ) { implicit bc: BoundedContext ⇒
      val ClusterPortPath = "spotlight.settings.cluster-port"
      val clusterPort = bc.configuration.as[Option[Int]]( ClusterPortPath ) getOrElse 2551

      for {
        _ ← logConfiguration()
        ref ← startStore( SharedLeveldbStore.Name, createActor )
        path = ref map { _.path } getOrElse {
          ActorPath fromString s"akka.tcp://${bc.system.name}@127.0.0.1:${clusterPort}/user/${SharedLeveldbStore.Name}"
        }
        id ← identifyStore( path, actionLabel )
      } yield Done
    }
  }

  private def logConfiguration()( implicit bc: BoundedContext ): Task[Unit] = {
    Task now {
      configLogger.info( bc.configuration.root.render )
      val AkkaPersistencePath = "akka.persistence"

      val JournalPluginPath = AkkaPersistencePath + ".journal.plugin"
      logger.info( "A TEST: looking at {}", JournalPluginPath )
      val journalPlugin = bc.configuration.as[Option[String]]( JournalPluginPath )
      logger.info( "SharedLeveldbStore: akka.persistence.journal.plugin: [{}]", journalPlugin )

      val JournalDirPath = AkkaPersistencePath + ".journal.leveldb-shared.store.dir" // JournalPluginPath+".store.dir"
      val journalDir = bc.configuration.as[Option[String]]( JournalDirPath )
      logger.info( "SharedLeveldbStore: journal dir: [{}]", journalDir )

      val SnapshotsDirPath = AkkaPersistencePath + ".snapshot-store.local.dir"
      val snapshotDir = bc.configuration.as[Option[String]]( SnapshotsDirPath )
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
    val id = ( bc.system.actorSelection( path ) ? Identify( None ) ).mapTo[ActorIdentity]
    id onComplete {
      case Success( ActorIdentity( _, Some( ref ) ) ) ⇒ {
        logger.info( "setting reference and system with shared leveldb journal {}...", Name )
        leveldb.SharedLeveldbJournal.setStore( ref, bc.system )
      }

      case Success( id ) ⇒ {
        bc.system.log.error( "shared journal {} not {} at {}", Name, actionLabel, path )
        bc.system.terminate()
      }

      case Failure( ex ) ⇒ {
        bc.system.log.error( "failure not {} shared journal {} at {}", actionLabel, Name, path )
        bc.system.terminate()
      }
    }

    id.toTask
  }
}
