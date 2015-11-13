package lineup.stream

import java.util.concurrent.atomic.AtomicInteger
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import scala.util.Failure
import akka.pattern
import akka.stream.OverflowStrategy
import akka.stream.testkit.scaladsl.{ TestSource, TestSink }
import akka.stream.scaladsl._
import akka.stream.io.Framing
import akka.util.ByteString
import akka.testkit._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.random.RandomDataGenerator
import org.joda.{ time => joda }
import com.github.nscala_time.time.OrderingImplicits._
import com.github.nscala_time.time.Imports.{ richSDuration, richDateTime }
import lineup.testkit.ParallelAkkaSpec
import lineup.analysis.outlier.{ DetectionAlgorithmRouter, OutlierDetection }
import lineup.analysis.outlier.algorithm.DBSCANAnalyzer
import lineup.model.outlier.{ SeriesOutliers, IsQuorum, OutlierPlan }
import lineup.model.timeseries.{ TimeSeries, DataPoint, Row }


/**
 * Created by rolfsd on 10/28/15.
 */
class GraphiteModelSpec extends ParallelAkkaSpec with LazyLogging {
  import GraphiteModelSpec._

  class Fixture extends AkkaFixture {
    def status[T]( label: String ): Flow[T, T, Unit] = Flow[T].map { e => logger info s"\n$label:${e.toString}"; e }

    val stringFlow: Flow[ByteString, ByteString, Unit] = Flow[ByteString]
      .via(
        Framing.delimiter(
          delimiter = ByteString("\n"),
          maximumFrameLength = scala.math.pow( 2, 20 ).toInt,
          allowTruncation = true
        )
      )

    val plans = Seq(
      OutlierPlan.default(
        name = "DEFAULT_PLAN",
        algorithms = Set(DBSCANAnalyzer.algorithm),
        timeout = 500.millis,
        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = 1, triggerPoint = 1 ),
        reduce = GraphiteModel.demoReduce,
        specification = ConfigFactory.parseString(
        s"""
         |algorithm-config.${DBSCANAnalyzer.EPS}: 5.0
         |algorithm-config.${DBSCANAnalyzer.MIN_DENSITY_CONNECTED_POINTS}: 3
        """.stripMargin
        )
      )
    )
  }

  object Fixture {
    case class TickA( topic: String = "[default]", values: Seq[Int] = Seq(TickA.tickId.incrementAndGet()) )

    object TickA {
      val tickId = new AtomicInteger()
      def merge( lhs: TickA, rhs: TickA ): TickA = lhs.copy( values = lhs.values ++ rhs.values )
    }
  }

  override def makeAkkaFixture(): Fixture = new Fixture

  "GraphiteModel" should {
    "convert graphite pickle into TimeSeries" in { f: Fixture =>
      import f._
      val now = joda.DateTime.now
      val dp = makeDataPoints( points, start = now ).take(5)
      val expected = TimeSeries( "foobar", dp )

      val flowUnderTest = Flow[ByteString].mapConcat( GraphiteModel.PickleProtocol.toDataPoints )
      val future = Source( Future.successful(ByteString(pickled(dp))) ).via( flowUnderTest ).runWith( Sink.head )
      val result = Await.result( future, 100.millis )
      result mustBe expected
    }

    "convert pickles from framed ByteStream" taggedAs (WIP) in { f: Fixture =>
      import f._
      val now = joda.DateTime.now
      val dp1 = makeDataPoints( points, start = now ).take(5)
      val dp2 = makeDataPoints( pointsA, start = joda.DateTime.now ).take(5)

      val expected = TimeSeries( "foobar", dp1 )

      val flowUnderTest = Flow[ByteString]
        .via(
          Framing.delimiter(
            delimiter = ByteString("]"),
            maximumFrameLength = scala.math.pow( 2, 20 ).toInt,
            allowTruncation = true
          )
        )
        .mapConcat( GraphiteModel.PickleProtocol.toDataPoints )

      val pickles = List( dp1, dp2 ).map{ pickled }.mkString( "\n" )
      trace( s"pickles = $pickles" )
      trace( s"byte-pickles = ${ByteString(pickles)}" )
      val future = Source( Future.successful(ByteString(pickles)) ).via( flowUnderTest ).runWith( Sink.head )
      val result = Await.result( future, 100.millis )
      result mustBe expected
    }

    "read sliding window" in { f: Fixture =>
      import f._
      import system.dispatcher

      val now = joda.DateTime.now
      val dp1 = makeDataPoints( points, start = now ).take(3)
      val dp2 = makeDataPoints( pointsA, start = joda.DateTime.now + 2L ).take(3)
      val dp3 = makeDataPoints( pointsB, start = joda.DateTime.now + 3L ).take(3)

      val expected = Set(
        TimeSeries( "bar", dp2 ),
        TimeSeries( "foo", (dp1 ++ dp3).sortBy( _.timestamp ) )
      )


      val flowUnderTest = GraphiteModel.graphiteTimeSeries( plans = plans, windowSize = 1.second, parallelism = 4 )
      val topics = List( "foo", "bar", "foo" )
      val pickles = topics.zip(List(dp1, dp2, dp3)).map{ pickled }.mkString( "\n" )

      val future = Source( Future.successful(ByteString(pickles)) )
                   .via( stringFlow )
                   .via( flowUnderTest )
                   .grouped( 10 )
                   .runWith( Sink.head )

      val result = Await.result( future, 10.seconds.dilated )
      result.toSet mustBe expected
    }


    "detect Outliers" in { f: Fixture =>
      import f._
      import system.dispatcher

      val algos = Set( DBSCANAnalyzer.algorithm )
      val defaultPlan = OutlierPlan.default(
        name = "DEFAULT_PLAN",
        algorithms = algos,
        timeout = 500.millis,
        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = algos.size, triggerPoint = 1 ),
        reduce = GraphiteModel.demoReduce,
        specification = ConfigFactory.parseString(
          s"""
             |algorithm-config.${DBSCANAnalyzer.EPS}: 5.0
             |algorithm-config.${DBSCANAnalyzer.MIN_DENSITY_CONNECTED_POINTS}: 3
          """.stripMargin
        )
      )

      val router = system.actorOf( DetectionAlgorithmRouter.props, "router" )
      val dbscan = system.actorOf( DBSCANAnalyzer.props(router), "dbscan" )
      val detector = system.actorOf( OutlierDetection.props(router, Seq(defaultPlan)), "detectOutliers" )

      val now = joda.DateTime.now
      val dp1 = makeDataPoints( points, start = now, period = 1.millis )
//      val dp2 = makeDataPoints( pointsA, start = joda.DateTime.now )
//      val dp3 = makeDataPoints( pointsB, start = joda.DateTime.now )

      val expectedValues = Row( 18.8, 25.2, 31.5, 22.0, 24.1, 39.2 )
      val expectedPoints = dp1 filter { expectedValues contains _.value } sortBy { _.timestamp }
      val expected = SeriesOutliers(
        algorithms = Set(DBSCANAnalyzer.algorithm),
        source = TimeSeries("foo", dp1),
        outliers = expectedPoints
      )
//      val expected = TimeSeries( "foo", (dp1 ++ dp3).sortBy( _.timestamp ) )

      val graphiteFlow = GraphiteModel.graphiteTimeSeries( parallelism = 4, windowSize = 20.millis, plans = plans )
      val detectFlow = OutlierDetection.detectOutlier( detector, maxAllowedWait = 2.seconds, parallelism = 4 )

      val flowUnderTest = graphiteFlow via detectFlow

      val topics = List( "foo", "bar", "foo" )
      val pickles = topics.zip(dp1 :: Nil).map{ pickled }.mkString( "\n" )

      val future = Source( Future.successful(ByteString(pickles)) ).via( stringFlow ).via(status("BEFORE")).via( flowUnderTest ).via(status("AFTER")).runWith( Sink.head )
      val result = Await.result( future, 2.seconds )
      result mustBe expected
    }

    "grouped Example" in { f: Fixture =>
      import f._

      val topics = IndexedSeq( "a", "b", "b", "b", "c" )

      val tickFn = () => {
        val next = Fixture.TickA.tickId.incrementAndGet()
        val topic = topics( next % topics.size )
        Fixture.TickA( topic, Seq(next) )
      }

      def conflateFlow[T](): Flow[T, T, Unit] = {
        Flow[T]
        .conflate( _ => List.empty[T] ){ (l, u) => u :: l }
        .mapConcat(identity)
      }

      val source = Source( 0.second, 50.millis, tickFn ).map { t => t() }

      val flowUnderTest: Flow[Fixture.TickA, Fixture.TickA, Unit] = {
        Flow[Fixture.TickA]
        .groupedWithin( n = 10000, d = 210.millis )
        .map {
          _.groupBy( _.topic )
          .map {case (topic, es) => es.tail.foldLeft( es.head ) {(acc, e) => Fixture.TickA.merge( acc, e ) } }
        }
        .mapConcat {identity}
      }

      val future = source
                   .via( flowUnderTest )
//                   .grouped( 5 )
                   .runWith( Sink.head )

      val result = Await.result( future, 5.seconds )
      result mustBe Fixture.TickA("b", Seq(1,2,3))
    }

    "ex1" in { f: Fixture =>
      import f._
      val sinkUnderTest = Flow[Int].map{ _ * 2 }.toMat{ Sink.fold( 0 ){ _ + _ } }( Keep.right )
      val future = Source( 1 to 4 ) runWith sinkUnderTest
      val result = Await.result( future, 100.millis )
      result mustBe 20
    }

    "ex2" in { f: Fixture =>
      import f._
      val sourceUnderTest = Source.repeat(1).map(_ * 2)
      val future = sourceUnderTest.grouped(10).runWith(Sink.head)
      val result = Await.result(future, 100.millis)
      result mustBe Seq.fill(10)(2)
    }

    "ex3" in { f: Fixture =>
      import f._
      val flowUnderTest = Flow[Int].takeWhile(_ < 5)
      val future = Source( 1 to 10 ).via( flowUnderTest ).runWith( Sink.fold(Seq.empty[Int]){ _ :+ _ } )
      val result = Await.result(future, 100.millis)
      result mustBe (1 to 4)
    }

    "ex4" in { f: Fixture =>
      import f._
      import f.system.dispatcher
      import akka.pattern.pipe

      val sourceUnderTest = Source( 1 to 4 ).grouped(2)
      val probe = TestProbe()
      sourceUnderTest.grouped(2).runWith(Sink.head).pipeTo(probe.ref)
      probe.expectMsg( 100.millis, Seq(Seq(1,2), Seq(3,4)))
    }

    "ex5" in { f: Fixture =>
      import f._
      case object Tick
      val sourceUnderTest = Source( 0.seconds, 200.millis, Tick )
      val probe = TestProbe()
      val cancellable = sourceUnderTest.to( Sink.actorRef(probe.ref, "completed") ).run()

      probe.expectMsg( 1.second, Tick )
      probe.expectNoMsg( 200.millis )
      probe.expectMsg( 200.millis, Tick )
      cancellable.cancel()
      probe.expectMsg( 200.millis, "completed" )
    }

    "ex6" in { f: Fixture =>
      import f._
      val sinkUnderTest = Flow[Int].map(_.toString).toMat(Sink.fold("")(_ + _))(Keep.right)
      val (ref, future) = Source.actorRef( 8, OverflowStrategy.fail ).toMat(sinkUnderTest)(Keep.both).run()

      ref ! 1
      ref ! 2
      ref ! 3
      ref ! akka.actor.Status.Success("done")

      val result = Await.result( future, 100.millis )
      result mustBe "123"
    }

    "ex7" in { f: Fixture =>
      import f._
      val sourceUnderTest = Source( 1 to 4 ).filter(_ % 2 == 0).map(_ * 2)
      sourceUnderTest
        .runWith( TestSink.probe[Int] )
        .request(2)
        .expectNext(4, 8)
        .expectComplete()
    }

    "ex8" in { f: Fixture =>
      import f._
      val sinkUnderTest = Sink.cancelled
      TestSource.probe[Int]
        .toMat( sinkUnderTest )( Keep.left )
        .run()
        .expectCancellation()
    }

    "ex9" in { f: Fixture =>
      import f._
      val sinkUnderTest = Sink.head[Int]
      val (probe, future) = TestSource.probe[Int].toMat( sinkUnderTest )( Keep.both ).run()
      probe.sendError( new Exception("BOOM") )
      Await.ready( future, 100.millis )
      val Failure( exception ) = future.value.get
      exception.getMessage mustBe "BOOM"
    }

    "ex10" in { f: Fixture =>
      import f._
      import system.dispatcher
      val flowUnderTest = Flow[Int].mapAsyncUnordered(2) { sleep =>
        pattern.after( 10.millis * sleep, using = system.scheduler )( Future.successful(sleep) )
      }

      val ( pub, sub) = TestSource.probe[Int]
        .via( flowUnderTest )
        .toMat( TestSink.probe[Int] )( Keep.both )
        .run()

      sub.request( n = 3 )
      pub.sendNext( 3 )
      pub.sendNext( 2 )
      pub.sendNext( 1 )
      sub.expectNextUnordered( 1, 2, 3 )

      pub.sendError( new Exception("Power surge in the linear subrountine C-47!") )
      val ex = sub.expectError
      ex.getMessage.contains( "C-47" ) mustBe true
    }
  }
}

object GraphiteModelSpec {
  def pickled( dp: Row[DataPoint] ): String = pickled( "foobar", dp )

  def pickled( topicDataPoints: (String, Row[DataPoint]) ): String = {
    val (topic, dp) = topicDataPoints
    dp.map{ p => s"(${topic}, (${p.timestamp.getMillis}, ${p.value}))" }.mkString( "[", ", ", "]" )
  }

  def makeDataPoints(
    values: Row[Double],
    start: joda.DateTime = joda.DateTime.now,
    period: FiniteDuration = 1.second,
    wiggleFactor: (Double, Double) = (1.0, 1.0)
  ): Row[DataPoint] = {
    val random = new RandomDataGenerator
    def nextFactor: Double = {
      if ( wiggleFactor._1 == wiggleFactor._2 ) wiggleFactor._1
      else random.nextUniform( wiggleFactor._1, wiggleFactor._2 )
    }

    values.zipWithIndex map { vi =>
      val (v, i) = vi
      val adj = (i * nextFactor) * period
      val ts = start + adj.toJodaDuration
      DataPoint( timestamp = ts, value = v )
    }
  }

  val points: Row[Double] = Row(
    9.46,
    9.9,
    11.6,
    14.5,
    17.3,
    19.2,
    18.4,
    14.5,
    12.2,
    10.8,
    8.58,
    8.36,
    8.58,
    7.5,
    7.1,
    7.3,
    7.71,
    8.14,
    8.14,
    7.1,
    7.5,
    7.1,
    7.1,
    7.3,
    7.71,
    8.8,
    9.9,
    14.2,
    18.8,
    25.2,
    31.5,
    22,
    24.1,
    39.2
  )


  val pointsA: Row[Double] = Row(
    9.46,
    9.9,
    11.6,
    14.5,
    17.3,
    19.2,
    18.4,
    14.5,
    12.2,
    10.8,
    8.58,
    8.36,
    8.58,
    7.5,
    7.1,
    7.3,
    7.71,
    8.14,
    8.14,
    7.1,
    7.5,
    7.1,
    7.1,
    7.3,
    7.71,
    8.8,
    9.9,
    14.2
  )

  val pointsB: Row[Double] = Row(
    10.1,
    10.1,
    9.68,
    9.46,
    10.3,
    11.6,
    13.9,
    13.9,
    12.5,
    11.9,
    12.2,
    13,
    13.3,
    13,
    12.7,
    11.9,
    13.3,
    12.5,
    11.9,
    11.6,
    10.5,
    10.1,
    9.9,
    9.68,
    9.68,
    9.9,
    10.8,
    11
  )

}
