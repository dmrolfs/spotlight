package spotlight.testkit

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.dispatch.Dispatchers
import akka.event.{Logging, LoggingAdapter}
import akka.testkit.TestEvent.Mute
import akka.testkit.{DeadLettersFilter, TestKit}
import scalaz.{-\/, \/, \/-}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{MustMatchers, Outcome, ParallelTestExecution, Tag, fixture}
import peds.commons.log.Trace
import peds.commons.util._


/**
 * Created by rolfsd on 10/28/15.
 */
object ParallelAkkaSpec {
  val sysId = new AtomicInteger()

  val testConf: Config = ConfigFactory.load.withFallback(
    ConfigFactory.parseString(
      """
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
        |      "akka.tcp://ClusterSystem@127.0.0.1:2551",
        |      "akka.tcp://ClusterSystem@127.0.0.1:2552"
        |    ]
        |
        |    auto-down-unreachable-after = 10s
        |  }
        |}
      """.stripMargin
    )
  )


  def getCallerName( clazz: Class[_] ): String = {
    val s = (Thread.currentThread.getStackTrace map (_.getClassName) drop 1)
            .dropWhile( _ matches "(java.lang.Thread|.*AkkaSpec.?$)" )
    val reduced = s.lastIndexWhere( _ == clazz.getName ) match {
      case -1 ⇒ s
      case z  ⇒ s drop (z + 1)
    }
    reduced.head.replaceFirst( """.*\.""", "" ).replaceAll( "[^a-zA-Z_0-9]", "_" )
  }

}

trait ParallelAkkaSpec
extends fixture.WordSpec
with MustMatchers
with ParallelTestExecution
with StrictLogging {
  outer =>

  val trace = Trace( getClass.safeSimpleName )

  object WIP extends Tag( "wip" )

  def makeSystem( name: String, config: Config ): ActorSystem = ActorSystem( name, config )

  type Fixture <: AkkaFixture
  type FixtureParam = Fixture
  def createAkkaFixture( test: OneArgTest ): Fixture

  import ParallelAkkaSpec.{ sysId, testConf }

  class AkkaFixture( val fixtureId: Int = sysId.incrementAndGet(), val config: Config = testConf )
  extends TestKit( makeSystem(s"Parallel-${fixtureId}", config) ) {
    def before(): Unit = { }
    def after(): Unit = { }

    val log: LoggingAdapter = Logging( system, outer.getClass )

    def spawn( dispatcherId: String = Dispatchers.DefaultDispatcherId )( body: => Unit ): Unit = {
      Future( body )( system.dispatchers.lookup(dispatcherId) )
    }

    def muteDeadLetters( messageClasses: Class[_]* )( sys: ActorSystem = system ): Unit = {
      if ( !sys.log.isDebugEnabled ) {
        def mute( clazz: Class[_] ): Unit = sys.eventStream.publish( Mute(DeadLettersFilter(clazz)(occurrences = Int.MaxValue)) )

        if ( messageClasses.isEmpty ) mute( classOf[AnyRef] )
        else messageClasses foreach mute
      }
    }
  }


  override def withFixture( test: OneArgTest ): Outcome = {
    val fixture = \/ fromTryCatchNonFatal { createAkkaFixture( test ) }
    val results = fixture map { f =>
      logger.debug( ".......... before test .........." )
      f.before()
      logger.debug( "++++++++++ starting test ++++++++++" )
      ( test(f), f )
    }

    val outcome = results map { case (outcome, f) =>
      logger.debug( "---------- finished test ------------" )
      f.after()
      logger.debug( ".......... after test .........." )

      Option(f.system) foreach { s =>
        val terminated = s.terminate()
        Await.ready( terminated, 1.second )
      }

      outcome
    }

    outcome match {
      case \/-( o ) => o
      case -\/( ex ) => {
        logger.error( s"test[${test.name}] failed", ex )
        throw ex
      }
    }
  }
}