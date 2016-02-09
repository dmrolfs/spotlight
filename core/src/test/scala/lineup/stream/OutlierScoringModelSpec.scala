package lineup.stream

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import scala.util.Failure

import akka.pattern
import akka.stream.OverflowStrategy
import akka.stream.testkit.scaladsl.{ TestSource, TestSink }
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.testkit._
import akka.actor.ActorRef
import scalaz.Scalaz._
import com.typesafe.config.ConfigFactory
import org.scalatest.Tag
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.random.RandomDataGenerator
import org.joda.{ time => joda }
import com.github.nscala_time.time.Imports.{ richSDuration, richDateTime }
import peds.commons.log.Trace
import lineup.protocol.PythonPickleProtocol
import lineup.testkit.ParallelAkkaSpec
import lineup.analysis.outlier.{ DetectionAlgorithmRouter, OutlierDetection }
import lineup.analysis.outlier.algorithm.SeriesDensityAnalyzer
import lineup.model.outlier.{ SeriesOutliers, IsQuorum, OutlierPlan }
import lineup.model.timeseries.{ TimeSeries, DataPoint, Row }


/**
 * Created by rolfsd on 10/28/15.
 */
class OutlierScoringModelSpec extends ParallelAkkaSpec with LazyLogging {
  import OutlierScoringModelSpec._

  class Fixture extends AkkaFixture { fixture =>
    def status[T]( label: String ): Flow[T, T, Unit] = Flow[T].map { e => logger info s"\n$label:${e.toString}"; e }

    val protocol = new PythonPickleProtocol
    val stringFlow: Flow[ByteString, ByteString, Unit] = Flow[ByteString].via( protocol.framingFlow() )

    trait TestConfigurationProvider extends OutlierDetection.ConfigurationProvider {
//      override def makePlans: Creator = () => { fixture.plans.right }
//      override def invalidateCaches(): Unit = { }
//      override def refreshInterval: FiniteDuration = 5.minutes
    }

    val configurationReloader = Configuration.reloader( Array.empty[String] )()()

    val algo = SeriesDensityAnalyzer.Algorithm

    val plan = OutlierPlan.default(
      name = "DEFAULT_PLAN",
      algorithms = Set( algo ),
      timeout = 500.millis,
      isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = 1, triggerPoint = 1 ),
      reduce = Configuration.defaultOutlierReducer,
      specification = ConfigFactory.parseString(
        s"""
          |algorithm-config.${algo.name}.eps: 5.0
          |algorithm-config.${algo.name}.minDensityConnectedPoints: 3
        """.stripMargin
      )
    )

    val plans = Seq( plan )
  }

  object Fixture {
    case class TickA( topic: String = "[default]", values: Seq[Int] = Seq(TickA.tickId.incrementAndGet()) )

    object TickA {
      val tickId = new AtomicInteger()
      def merge( lhs: TickA, rhs: TickA ): TickA = lhs.copy( values = lhs.values ++ rhs.values )
    }
  }

  override def makeAkkaFixture(): Fixture = new Fixture

  val NEXT = Tag( "next" )
  val DONE = Tag( "done" )

  "GraphiteModel" should {
    "convert pickle to TimeSeries" in { f: Fixture =>
      import f._
      val now = joda.DateTime.now
      val dp = makeDataPoints( points, start = now ).take( 5 )
      val expected = List( TimeSeries( "foobar", dp ) )
      val actual = protocol.toDataPoints( pickled(dp) )
      actual mustBe expected
    }

    "flow convert graphite pickle into TimeSeries" in { f: Fixture =>
      import f._
      val now = joda.DateTime.now
      val dp = makeDataPoints( points, start = now ).take( 5 )
      val expected = TimeSeries( "foobar", dp )

      val flowUnderTest = protocol.unmarshalTimeSeriesData
      //      val flowUnderTest = Flow[ByteString].mapConcat( PythonPickleProtocol.toDataPoints )
      val future = Source( List(pickled(dp)) ).via( flowUnderTest ).runWith( Sink.head )
      val result = Await.result( future, 100.millis.dilated )
      result mustBe expected
    }

    "framed flow convert graphite pickle into TimeSeries" in { f: Fixture =>
      import f._
      val now = joda.DateTime.now
      val dp = makeDataPoints( points, start = now ).take( 5 )
      val expected = TimeSeries( "foobar", dp )

      val flowUnderTest = Flow[ByteString]
        .via( protocol.framingFlow() )
        .via( protocol.unmarshalTimeSeriesData )

      val future = Source( List(withHeader(pickled(dp))) ).via( flowUnderTest ).runWith( Sink.head )
      val result = Await.result( future, 1.second.dilated )
      result mustBe expected
    }

    "convert pickles from framed ByteStream" in { f: Fixture =>
      import f._
      val now = joda.DateTime.now
      val dp1 = makeDataPoints( points, start = now ).take( 5 )
      val dp2 = makeDataPoints( pointsA, start = now+7 ).take( 5 )

//      val expected = TimeSeries.seriesMerging.merge( TimeSeries("foobar", dp1), TimeSeries("foobar", dp2) ).toOption.get
      val expected = TimeSeries( "foobar", dp1 ++ dp2 )
      trace( s"expected = $expected" )

      val flowUnderTest = Flow[ByteString]
        .via( protocol.framingFlow() )
        .via( protocol.unmarshalTimeSeriesData )

      val pickles = withHeader( pickled( Seq(dp1, dp2) map { ("foobar", _) } ) )
      trace( s"pickles = ${pickles.utf8String}" )
      trace( s"byte-pickles = ${pickles}" )
      val future = Source( List(pickles) ).via( flowUnderTest ).runWith( Sink.head )
      val result = Await.result( future, 1.second.dilated )
      result mustBe expected
    }

    "read sliding window" in { f: Fixture =>
      import f._
      import system.dispatcher

      val now = joda.DateTime.now
      val dp1 = makeDataPoints( points, start = now ).take( 3 )
      val dp2 = makeDataPoints( pointsA, start = joda.DateTime.now + 2L ).take( 3 )
      val dp3 = makeDataPoints( pointsB, start = joda.DateTime.now + 3L ).take( 3 )

      val expected = Set(
        TimeSeries( "bar", dp2 ),
        TimeSeries.seriesMerging.merge( TimeSeries("foo", dp1), TimeSeries("foo", dp3) ).toOption.get
      )


      val flowUnderTest: Flow[TimeSeries, TimeSeries, Unit] = OutlierScoringModel.batchSeries( windowSize = 1.second, parallelism = 4 )
      val topics = List( "foo", "bar", "foo" )
      val data: List[TimeSeries] = topics.zip(List(dp1, dp2, dp3)).map{ case (t,p) => TimeSeries(t, p) }
      trace( s"""data=[${data.mkString(",\n")}]""")

      val future = Source( data )
                   .via( flowUnderTest )
                   .grouped( 10 )
                   .runWith( Sink.head )

      val result = Await.result( future, 1.second.dilated )
      result.toSet mustBe expected
    }


    "detect Outliers" taggedAs (WIP) in { f: Fixture =>
      import f._
      import system.dispatcher

      val algos = Set( algo )
      val defaultPlan = OutlierPlan.default(
        name = "DEFAULT_PLAN",
        algorithms = algos,
        timeout = 5000.millis,
        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = algos.size, triggerPoint = 1 ),
        reduce = Configuration.defaultOutlierReducer,
        specification = ConfigFactory.parseString(
          algos
          .map { a =>
            s"""
               |algorithm-config.${a.name}.eps: 5000
               |algorithm-config.${a.name}.minDensityConnectedPoints: 9
               |algorithm-config.${a.name}.distance: Euclidean
            """.stripMargin
          }
          .mkString( "\n" )
        )
      )

      val routerRef = system.actorOf( DetectionAlgorithmRouter.props, "router" )
      val dbscan = system.actorOf( SeriesDensityAnalyzer.props( routerRef ), "dbscan" )
      val detector = system.actorOf(
        OutlierDetection.props {
          new OutlierDetection with TestConfigurationProvider {
            override def router: ActorRef = routerRef
//            override def makePlans: Creator = () => { Seq(defaultPlan).right }
          }
        },
        "detectOutliers"
      )

      val now = new joda.DateTime( joda.DateTime.now.getMillis / 1000L * 1000L )
      val dp1 = makeDataPoints( points, start = now, period = 1.seconds )
//      val dp2 = makeDataPoints( pointsA, start = joda.DateTime.now )
//      val dp3 = makeDataPoints( pointsB, start = joda.DateTime.now )

//      val expectedValues = Row( 18.8, 25.2, 31.5, 22.0, 24.1, 39.2 )
//      val expectedPoints = dp1 filter { expectedValues contains _.value } sortBy { _.timestamp }
      val largestCluster = Row(
        DataPoint( new joda.DateTime(now.getMillis + 10000L), 8.58),
        DataPoint( new joda.DateTime(now.getMillis + 11000L), 8.36),
        DataPoint( new joda.DateTime(now.getMillis + 12000L), 8.58),
        DataPoint( new joda.DateTime(now.getMillis + 13000L), 7.5),
        DataPoint( new joda.DateTime(now.getMillis + 14000L), 7.1),
        DataPoint( new joda.DateTime(now.getMillis + 15000L), 7.3),
        DataPoint( new joda.DateTime(now.getMillis + 16000L), 7.71),
        DataPoint( new joda.DateTime(now.getMillis + 17000L), 8.14),
        DataPoint( new joda.DateTime(now.getMillis + 18000L), 8.14),
        DataPoint( new joda.DateTime(now.getMillis + 19000L), 7.1),
        DataPoint( new joda.DateTime(now.getMillis + 20000L), 7.5),
        DataPoint( new joda.DateTime(now.getMillis + 21000L), 7.1),
        DataPoint( new joda.DateTime(now.getMillis + 22000L), 7.1),
        DataPoint( new joda.DateTime(now.getMillis + 23000L), 7.3)
      )
      val expectedPoints = dp1 filterNot { largestCluster contains _ }

      val expected = SeriesOutliers(
        algorithms = algos,
        source = TimeSeries("foo", dp1),
        outliers = expectedPoints,
        plan = defaultPlan
      )
//      val expected = TimeSeries( "foo", (dp1 ++ dp3).sortBy( _.timestamp ) )

      val graphiteFlow = OutlierScoringModel.batchSeries( parallelism = 4, windowSize = 20.millis )
      val detectFlow = OutlierDetection.detectOutlier(
        detector,
        maxAllowedWait = 2.seconds,
        plans = immutable.Seq(defaultPlan),
        parallelism = 4
      )

      val flowUnderTest = graphiteFlow via detectFlow

      val topics = List( "foo", "bar", "foo" )
      val data: List[TimeSeries] = topics.zip(List(dp1)).map{ case (t,p) => TimeSeries(t, p) }

      val future = Source( data )
//                   .via(status("BEFORE"))
                   .via( flowUnderTest )
//                   .via(status("AFTER"))
                   .runWith( Sink.head )
      val result = Await.result( future, 2.seconds.dilated )
      trace( s"result class   [${result.hashCode}] = ${result.getClass}")
      trace( s"expected class [${expected.hashCode}] = ${expected.getClass}")
      result.algorithms mustBe expected.algorithms
      result.anomalySize mustBe expected.anomalySize
      result.hasAnomalies mustBe expected.hasAnomalies
      result.size mustBe expected.size
      result.source mustBe expected.source
      result.topic mustBe expected.topic
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

      val source = Source.tick( 0.second, 50.millis, tickFn ).map { t => t() }

      val flowUnderTest: Flow[Fixture.TickA, Fixture.TickA, Unit] = {
        Flow[Fixture.TickA]
        .groupedWithin( n = 10000, d = 210.millis )
        .map {
          _.groupBy( _.topic )
          .map {case (topic, es) => es.tail.foldLeft( es.head ) {(acc, e) => Fixture.TickA.merge( acc, e ) } }
        }
        .mapConcat { identity }
      }

      val future = source
                   .via( flowUnderTest )
//                   .grouped( 5 )
                   .runWith( Sink.head )

      val result = Await.result( future, 5.seconds.dilated )
      result mustBe Fixture.TickA("b", Seq(1,2,3))
    }

    "ex1" in { f: Fixture =>
      import f._
      val sinkUnderTest = Flow[Int].map{ _ * 2 }.toMat{ Sink.fold( 0 ){ _ + _ } }( Keep.right )
      val future = Source( 1 to 4 ) runWith sinkUnderTest
      val result = Await.result( future, 2.seconds.dilated )
      result mustBe 20
    }

    "ex2" in { f: Fixture =>
      import f._
      val sourceUnderTest = Source.repeat(1).map(_ * 2)
      val future = sourceUnderTest.grouped(10).runWith(Sink.head)
      val result = Await.result( future, 2.second.dilated )
      result mustBe Seq.fill(10)(2)
    }

    "ex3" in { f: Fixture =>
      import f._
      val flowUnderTest = Flow[Int].takeWhile(_ < 5)
      val future = Source( 1 to 10 ).via( flowUnderTest ).runWith( Sink.fold(Seq.empty[Int]){ _ :+ _ } )
      val result = Await.result( future, 2.seconds.dilated )
      result mustBe (1 to 4)
    }

    "ex4" in { f: Fixture =>
      import f._
      import f.system.dispatcher
      import akka.pattern.pipe

      val sourceUnderTest = Source( 1 to 4 ).grouped(2)
      val probe = TestProbe()
      sourceUnderTest.grouped(2).runWith(Sink.head).pipeTo(probe.ref)
      probe.expectMsg( 2.seconds.dilated, Seq(Seq(1,2), Seq(3,4)))
    }

    "ex5" in { f: Fixture =>
      import f._
      case object Tick
      val sourceUnderTest = Source.tick( 0.seconds, 200.millis, Tick )
      val probe = TestProbe()
      val cancellable = sourceUnderTest.to( Sink.actorRef(probe.ref, "completed") ).run()

      probe.expectMsg( 1.second, Tick )
      probe.expectNoMsg( 175.millis )
      probe.expectMsg( 200.millis.dilated, Tick )
      cancellable.cancel()
      probe.expectMsg( 200.millis.dilated, "completed" )
    }

    "ex6" in { f: Fixture =>
      import f._
      val sinkUnderTest = Flow[Int].map(_.toString).toMat(Sink.fold("")(_ + _))(Keep.right)
      val (ref, future) = Source.actorRef( 8, OverflowStrategy.fail ).toMat(sinkUnderTest)(Keep.both).run()

      ref ! 1
      ref ! 2
      ref ! 3
      ref ! akka.actor.Status.Success("done")

      val result = Await.result( future, 1.second.dilated )
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

    "ex10" taggedAs (WIP) in { f: Fixture =>
      pending
//      akka docs seem to req updating from 1.x
      import f._
      import system.dispatcher
      val flowUnderTest = Flow[Int].mapAsyncUnordered(2) { sleep =>
        pattern.after( 10.millis * sleep, using = system.scheduler )( Future.successful(sleep) )
      }

      val ( pub, sub ) = TestSource.probe[Int]
        .via( flowUnderTest )
        .toMat( TestSink.probe[Int] )( Keep.both )
        .run()

      sub.request( n = 3 )
      pub.sendNext( 3 )
      pub.sendNext( 2 )
      pub.sendNext( 1 )
      sub.expectNextUnordered( 1, 2, 3 )

      pub.sendError( new Exception("Power surge in the linear subroutine C-47!") )
      val ex = sub.expectError
      ex.getMessage.contains( "C-47" ) mustBe true
    }
  }
}

object OutlierScoringModelSpec {
  val trace = Trace[OutlierScoringModelSpec.type]

  def withHeader( body: ByteString ): ByteString = {
    val result = ByteBuffer.allocate( 4 + body.size )
    result putInt body.size
    result put body.toArray
    result.flip()
    ByteString( result )
  }

  def pickled( dp: Row[DataPoint] ): ByteString = pickled( Seq(("foobar", dp)) )

  def pickled(metrics: Seq[(String, Row[DataPoint])] ): ByteString = trace.block( s"pickled($metrics)" ) {
    import net.razorvine.pickle.Pickler
    import scala.collection.convert.wrapAll._

    val data = new java.util.LinkedList[AnyRef]
    for {
      metric <- metrics
      (topic, points) = metric
      p <- points
    } {
      val dp: Array[Any] = Array( p.timestamp.getMillis / 1000L, p.value )
      val metric: Array[AnyRef] = Array( topic, dp )
      data add metric
    }
    trace( s"data = $data")

    val pickler = new Pickler( false )
    val out = pickler dumps data

    trace( s"""payload[${out.size}] = ${ByteString(out).decodeString("ISO-8859-1")}""" )
    ByteString( out )
  }

  def makeDataPoints(
    values: Row[Double],
    start: joda.DateTime = joda.DateTime.now,
    period: FiniteDuration = 1.second,
    wiggleFactor: (Double, Double) = (1.0, 1.0)
  ): Row[DataPoint] = {
    val secs = start.getMillis / 1000L
    val epochStart = new joda.DateTime( secs * 1000L )
    val random = new RandomDataGenerator
    def nextFactor: Double = {
      if ( wiggleFactor._1 == wiggleFactor._2 ) wiggleFactor._1
      else random.nextUniform( wiggleFactor._1, wiggleFactor._2 )
    }

    values.zipWithIndex map { vi =>
      val (v, i) = vi
      val adj = (i * nextFactor) * period
      val ts = epochStart + adj.toJodaDuration
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
