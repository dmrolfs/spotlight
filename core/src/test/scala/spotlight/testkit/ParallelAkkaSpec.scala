package spotlight.testkit

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.dispatch.Dispatchers
import akka.event.{ Logging, LoggingAdapter }
import akka.testkit.TestEvent.Mute
import akka.testkit.{ DeadLettersFilter, TestKit }
import com.persist.logging._

import scalaz.{ -\/, \/, \/- }
import com.typesafe.config.{ Config, ConfigFactory }
import com.typesafe.scalalogging.{ LazyLogging, StrictLogging }
import demesne.testkit.concurrent.CountDownFunction
import org.scalatest.{ MustMatchers, Outcome, ParallelTestExecution, Tag, fixture }
import omnibus.commons.log.Trace
import omnibus.commons.util._
import spotlight.infrastructure.ClusterRole
import spotlight.{ Settings, Spotlight }

/** Created by rolfsd on 10/28/15.
  */
object ParallelAkkaSpec extends LazyLogging {
  val testPosition: AtomicInteger = new AtomicInteger()

  def testConf( systemName: String ): Config = {
    ConfigFactory.load.withFallback(
      ConfigFactory.parseString(
        s"""
          |include "logging.conf"
          |
          |akka {
          |  persistence {
          |    journal.plugin = "inmemory-journal"
          |    snapshot-store.plugin = "inmemory-snapshot-store"
          |  }
          |}
          |
          |akka {
          |  loggers = ["akka.testkit.TestEventListener"]
          |  logging-filter = "akka.event.DefaultLoggingFilter"
          |  loglevel = DEBUG
          |  stdout-loglevel = DEBUG
          |  log-dead-letters = on
          |  log-dead-letters-during-shutdown = on
          |
          |  actor {
          |    provider = "akka.cluster.ClusterActorRefProvider"
          |#    default-dispatcher {
          |#      executor = "fork-join-executor"
          |#      fork-join-executor {
          |#        parallelism-min = 8
          |#        parallelism-tolerance = 2.0
          |#        parallelism-max = 8
          |#      }
          |#    }
          |  }
          |
          |  remote {
          |    log-remote-lifecycle-events = off
          |    netty.tcp {
          |      hostname = "127.0.0.1"
          |      port = 0
          |    }
          |  }
          |
          |  cluster {
          |    seed-nodes = [
          |      "akka.tcp://${systemName}@127.0.0.1:2551",
          |      "akka.tcp://${systemName}@127.0.0.1:2552"
          |    ]
          |
          |    auto-down-unreachable-after = 10s
          |  }
          |}
          |
          |spotlight.dispatchers {
          |  outlier-detection-dispatcher {
          |    type = Dispatcher
          |    executor = "fork-join-executor"
          |    #  throughput = 100
          |    fork-join-executor { }
          |  }
          |
          |  publishing-dispatcher {
          |    type = Dispatcher
          |    executor = "thread-pool-executor"
          |    thread-pool-executor {
          |      fixed-pool-size = 8
          |    }
          |  }
          |}
          |
        """.stripMargin
      )
    )
  }

  def getCallerName( clazz: Class[_] ): String = {
    val s = ( Thread.currentThread.getStackTrace map ( _.getClassName ) drop 1 )
      .dropWhile( _ matches "(java.lang.Thread|.*AkkaSpec.?$)" )
    val reduced = s.lastIndexWhere( _ == clazz.getName ) match {
      case -1 ⇒ s
      case z ⇒ s drop ( z + 1 )
    }
    reduced.head.replaceFirst( """.*\.""", "" ).replaceAll( "[^a-zA-Z_0-9]", "_" )
  }

}

trait ParallelAkkaSpec
    extends fixture.WordSpec
    with MustMatchers
    with ParallelTestExecution
    with StrictLogging {
  outer ⇒

  val trace = Trace( getClass.safeSimpleName )

  object WIP extends Tag( "wip" )

  //  def makeSystem( name: String, config: Config ): ActorSystem = ActorSystem( name, config )

  type Fixture <: AkkaFixture
  type FixtureParam = Fixture

  def testSlug( test: OneArgTest ): String = s"Par-${getClass.safeSimpleName}-${ParallelAkkaSpec.testPosition.incrementAndGet()}"

  def testConfiguration( test: OneArgTest, slug: String ): Config = {
    logger.error( "#TEST testConfiguration..." )
    Settings.adaptConfiguration(
      config = ParallelAkkaSpec.testConf( systemName = slug ),
      role = ClusterRole.All,
      systemName = slug
    )
  }

  def testSystem( test: OneArgTest, config: Config, slug: String ): ActorSystem = {
    import net.ceedubs.ficus.Ficus._
    logger.error( "#TEST testSystem...port:[{}] seeds:[{}]", config.as[String]( Settings.AkkaRemotePortPath ), Settings.seedNodesFrom( config ).mkString( ", " ) )
    ActorSystem( name = slug, config )
  }

  def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture

  class AkkaFixture( val config: Config, _system: ActorSystem, val slug: String ) extends TestKit( _system ) {
    var loggingSystem: LoggingSystem = _

    def before( test: OneArgTest ): Unit = {
      loggingSystem = LoggingSystem( _system, s"Test:${getClass.getName}", "1", "localhost" )
      logger.warn( "giving logging system time to initialize..." )
      val countDown = new CountDownFunction[String]
      countDown await 500.millis
      logger.warn( "... done waiting for logging system " )
    }

    def after( test: OneArgTest ): Unit = {}

    val log: LoggingAdapter = Logging( system, outer.getClass )

    def spawn( dispatcherId: String = Dispatchers.DefaultDispatcherId )( body: ⇒ Unit ): Unit = {
      Future( body )( system.dispatchers.lookup( dispatcherId ) )
    }

    def muteDeadLetters( messageClasses: Class[_]* )( sys: ActorSystem = system ): Unit = {
      if ( !sys.log.isDebugEnabled ) {
        def mute( clazz: Class[_] ): Unit = sys.eventStream.publish( Mute( DeadLettersFilter( clazz )( occurrences = Int.MaxValue ) ) )

        if ( messageClasses.isEmpty ) mute( classOf[AnyRef] )
        else messageClasses foreach mute
      }
    }
  }

  override def withFixture( test: OneArgTest ): Outcome = {
    val slug = testSlug( test )
    val config = testConfiguration( test, slug )
    val system = testSystem( test, config, slug )

    val fixture = \/ fromTryCatchNonFatal { createAkkaFixture( test, config, system, slug ) }

    val results = fixture map { f ⇒
      logger.debug( ".......... before test .........." )
      f before test
      logger.debug( "++++++++++ starting test ++++++++++" )
      ( test( f ), f )
    }

    val outcome = results map {
      case ( outcome, f ) ⇒
        logger.debug( "---------- finished test ------------" )
        f after test
        logger.debug( ".......... after test .........." )

        Option( f.system ) foreach { s ⇒
          val terminated = s.terminate()
          Await.ready( terminated, 10.second )
        }

        outcome
    }

    outcome match {
      case \/-( o ) ⇒ o
      case -\/( ex ) ⇒ {
        logger.error( s"test[${test.name}] failed", ex )
        system.terminate()
        throw ex
      }
    }
  }
}