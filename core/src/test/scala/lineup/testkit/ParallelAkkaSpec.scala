package lineup.testkit

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.dispatch.Dispatchers
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.testkit.TestEvent.Mute
import akka.testkit.{ DeadLettersFilter, ImplicitSender, TestKit }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ fixture, Outcome, Tag, ParallelTestExecution, MustMatchers }
import peds.commons.log.Trace
import peds.commons.util._


/**
 * Created by rolfsd on 10/28/15.
 */
object ParallelAkkaSpec {
  val sysId = new AtomicInteger()

  val testConf: Config = ConfigFactory.parseString(
    """
      |akka {
      |  loggers = ["akka.testkit.TestEventListener"]
      |  loglevel = "INFO"
      |  stdout-loglevel = "INFO"
      |  actor {
      |    default-dispatcher {
      |      executor = "fork-join-executor"
      |      fork-join-executor {
      |        parallelism-min = 8
      |        parallelism-factor = 2.0
      |        parallelism-max = 8
      |      }
      |    }
      |  }
      |}
    """.stripMargin
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
with ParallelTestExecution { outer =>
  import ParallelAkkaSpec._

  val trace = Trace( getClass.safeSimpleName )

  object WIP extends Tag( "wip" )

  def makeSystem( name: String, config: Config ): ActorSystem = ActorSystem( name, config )

  type Fixture <: AkkaFixture
  type FixtureParam = Fixture
  def makeAkkaFixture(): Fixture

  class AkkaFixture( id: Int = sysId.incrementAndGet(), config: Config = testConf )
  extends TestKit( makeSystem(s"Parallel-${id}", config) ) with ImplicitSender {
    def before(): Unit = { }
    def after(): Unit = { }

    implicit val materializer: Materializer = ActorMaterializer()

    val log: LoggingAdapter = Logging( system, outer.getClass )

    def spawn( dispatcherId: String = Dispatchers.DefaultDispatcherId )( body: => Unit ): Unit = {
      Future( body )( system.dispatchers.lookup(dispatcherId) )
    }

    def muteDeadLetters( messageClasses: Class[_]* )( sys: ActorSystem = system ): Unit = {
      if (!sys.log.isDebugEnabled) {
        def mute( clazz: Class[_] ): Unit = sys.eventStream.publish( Mute(DeadLettersFilter(clazz)(occurrences = Int.MaxValue)) )

        if ( messageClasses.isEmpty ) mute( classOf[AnyRef] )
        else messageClasses foreach mute
      }
    }
  }


  override def withFixture( test: OneArgTest ): Outcome = {
    val f = makeAkkaFixture()
    try {
      f.before()
      test( f )
    } finally {
      f.after()
      val terminated = f.system.terminate()
      Await.ready( terminated, 1.second )
    }
  }

}