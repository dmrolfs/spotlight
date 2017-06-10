package spotlight.checkpoint

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.stream.testkit.scaladsl.TestSink
import demesne.testkit.concurrent.CountDownFunction
import org.scalatest._
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

/** Created by rolfsd on 6/8/17 from gist by regis leray
  */
class ValveSpec extends FlatSpec with ScalaFutures {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = materializer.executionContext

  "A closed valve" should "emit only " in {
    val ( valve, seq ) = {
      Source( 1 to 3 )
        .viaMat( new Valve( Valve.Mode.Closed ) )( Keep.right )
        .toMat( Sink.seq )( Keep.both )
        .run()
    }

    val countDown = new CountDownFunction[String]
    countDown await 200.millis
    valve.open

    whenReady( seq, timeout( 200.millis ) ) { sum ⇒ sum should contain inOrder ( 1, 2, 3 ) }
  }

  "A closed valve" should "emit only 5 elements after it has been open" in {
    val ( valve, probe ) = {
      Source( 1 to 5 )
        .viaMat( new Valve( Valve.Mode.Closed ) )( Keep.right )
        .toMat( TestSink.probe[Int] )( Keep.both )
        .run()
    }

    probe request 2
    probe expectNoMsg 100.millis

    valve.open

    probe.expectNext shouldEqual 1
    probe.expectNext shouldEqual 2

    probe request 3
    probe.expectNext shouldEqual 3
    probe.expectNext shouldEqual 4
    probe.expectNext shouldEqual 5

    probe.expectComplete()
  }

  it should "emit 5 elements after it has been open/close/open" in {
    val ( valve, probe ) = {
      Source( 1 to 5 )
        .viaMat( new Valve )( Keep.right )
        .toMat( TestSink.probe[Int] )( Keep.both )
        .run()
    }

    probe request 2
    probe.expectNext shouldEqual 1
    probe.expectNext shouldEqual 2

    valve.close

    probe request 1
    probe expectNoMsg 100.millis

    valve.open
    probe.expectNext shouldEqual 3

    probe request 2
    probe.expectNext shouldEqual 4
    probe.expectNext shouldEqual 5

    probe.expectComplete()
  }
}
