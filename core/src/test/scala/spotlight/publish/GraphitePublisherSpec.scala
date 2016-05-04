package spotlight.publish

import java.io.ByteArrayOutputStream
import java.net.{InetAddress, InetSocketAddress, Socket}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import javax.net._
import javax.script.{Compilable, ScriptEngineManager, SimpleBindings}

import akka.actor.Props
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import spotlight.model.outlier.{NoOutliers, OutlierPlan, Outliers, SeriesOutliers}
import spotlight.model.timeseries.{ControlBoundary, DataPoint, TimeSeries, Topic}
import spotlight.protocol.PythonPickleProtocol
import spotlight.testkit.ParallelAkkaSpec
import org.joda.{time => joda}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.python.core.{PyList, PyTuple}
import org.scalatest.Tag
import org.scalatest.mock.MockitoSugar
import peds.commons.log.Trace


/**
  * Created by rolfsd on 12/31/15.
  */
object GraphitePublisherSpec {
  val engine = new ScriptEngineManager().getEngineByName( "python" )
  val compilable = engine.asInstanceOf[Compilable]
  val unpickleScript = compilable.compile(
    """
     |import pickle
     |import struct
     |format = '!L'
     |headerLength = struct.calcsize(format)
     |print "D headerLength=%d" % headerLength
     |payloadLength, = struct.unpack(format, payload[:headerLength])
     |print "E payloadLength=%d" % payloadLength.intValue()
     |batchLength = headerLength + payloadLength.intValue()
     |print "F batchLength=%d" % batchLength
     |metrics = pickle.loads(payload[headerLength:batchLength])
     |print "G metrics=%s" % metrics
    """.stripMargin
  )
}


class GraphitePublisherSpec
extends ParallelAkkaSpec
with MockitoSugar {
  import GraphitePublisher._
  import GraphitePublisherSpec._
  import OutlierPublisher._

  override val trace = Trace[GraphitePublisherSpec]

  class Fixture extends AkkaFixture { outer =>
    val senderProbe = TestProbe( "test-sender" )
    val connected: AtomicBoolean = new AtomicBoolean( true )
    val closed: AtomicBoolean = new AtomicBoolean( false )
    val openCount: AtomicInteger = new AtomicInteger( 0 )
    val address: InetSocketAddress = new InetSocketAddress( "example.com", 1234 )
    val output: ByteArrayOutputStream = spy( new ByteArrayOutputStream )
    val socketFactory: SocketFactory = mock[SocketFactory]

    val plan = mock[OutlierPlan]
    when( plan.name ) thenReturn "plan"
//    when( plan.algorithmConfig ) thenReturn ConfigFactory.parseString( "" )

    val socket: Socket = mock[Socket]
    when( socket.isConnected ).thenAnswer(
      new Answer[Boolean] {
        override def answer( invocation: InvocationOnMock ): Boolean = connected.get
      }
    )

    when( socket.isClosed ).thenAnswer(
      new Answer[Boolean] {
        override def answer( invocation: InvocationOnMock ): Boolean = closed.get
      }
    )

    doAnswer(
      new Answer[Unit] {
        override def answer( invocation: InvocationOnMock ): Unit = {
          connected set false
          closed set true
        }
      }
    ).when( socket ).close

    when( socket.getOutputStream ).thenReturn( output )

    // Mock behavior of socket.getOutputStream().close() calling socket.close();
    doAnswer(
      new Answer[Unit] {
        override def answer( invocation: InvocationOnMock ): Unit = {
          invocation.callRealMethod()
          socket.close
        }
      }
    ).when(output).close()

    when( socketFactory.createSocket( any(classOf[InetAddress]), anyInt ) ).thenReturn( socket )

    val publisherProps = Props(
      new GraphitePublisher with GraphitePublisher.PublishProvider {
        import scala.concurrent.duration._

        override lazy val maxOutstanding: Int = 1000000
        override lazy val separation: FiniteDuration = 1.second
        override def initializeMetrics(): Unit = { }
        override lazy val batchSize: Int = 100
        override lazy val destinationAddress: InetSocketAddress = outer.address
        override def createSocket( address: InetSocketAddress ): Socket = {
          openCount.incrementAndGet()
          outer.socket
        }
        override def publishingTopic( p: OutlierPlan, t: Topic ): Topic = t
      }
    )
    val graphite = TestActorRef[GraphitePublisher]( publisherProps )

    val dp1 = DataPoint( new joda.DateTime(100000L), 17D )
    val dp1b = DataPoint( new joda.DateTime(103000L), 19D )
    val dp2 = DataPoint( new joda.DateTime(117000L), 3.1415926D )
    val dp3 = DataPoint( new joda.DateTime(9821000L), 983.120D )


    def unpickleOutput( pickle: ByteString = ByteString(output.toByteArray) ): String = {
      import scala.collection.mutable
      val results = mutable.StringBuilder.newBuilder
      // the charset is important. if the GraphitePickleReporter and this test
      // don't agree, the header is not always correctly unpacked.
      val payload = pickle.decodeString( "UTF-8" )
      trace( s"payload = $payload" )
      val result = new PyList
      var nextIndex = 0
      while ( nextIndex < payload.length ) {
        val bindings = new SimpleBindings
        bindings.put( "payload", payload substring nextIndex )
        unpickleScript eval bindings
        result.addAll( result.size, bindings.get("metrics").asInstanceOf[java.util.Collection[_]] )
        nextIndex += bindings.get( "batchLength" ).asInstanceOf[Int]
      }

      import scala.collection.JavaConverters._
      result.iterator.asScala.foreach { case datapoint: PyTuple =>
        val name = datapoint.get( 0 ).toString
        val valueTuple = datapoint.get( 1 ).asInstanceOf[PyTuple]
        val timestamp = valueTuple get 0
        val value = valueTuple get 1
        results.append( name ).append( " " ).append( value ).append( " " ).append( timestamp ).append( "\n" )
      }

      results.toString()
    }
  }

  def makeAkkaFixture(): Fixture = new Fixture


  val DONE = Tag("done")

  "GraphitePublisher" should {
    "disconnects from graphite" in { f: Fixture =>
      import f._
      graphite ! Close
//      graphite.receive( Close )
      verify( socket ).close()
    }

    "first replicate python protocol test" in { f: Fixture =>
      import f._
      import org.joda.{ time => joda }
      import spotlight.model.timeseries._

      val batch = Seq(
        ("foo".toTopic, new joda.DateTime(100000L), 1D),
        ("bar".toTopic, new joda.DateTime(117000L), 0D),
        ("zed".toTopic, new joda.DateTime(9821000L), 0D)
      )

      unpickleOutput( new PythonPickleProtocol().pickleFlattenedTimeSeries( batch:_* )) mustBe {
        // timestamp long are be divided by 1000L to match graphite's epoch time
        "foo 1.0 100\nbar 0.0 117\nzed 0.0 9821\n"
      }
    }

    "write one-point batch" in { f: Fixture =>
      import f._
      val outliers = NoOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo", Seq(dp1)),
        plan = plan
      )
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush, senderProbe.ref )
      senderProbe.expectMsg( GraphitePublisher.Flushed(true) )
      val actual = ByteString( output.toByteArray )
      unpickleOutput( actual ) mustBe "foo 0.0 100\n"
    }

    "write full batch" in { f: Fixture =>
      import f._
      val outliers = SeriesOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo", Seq(dp1, dp2)),
        outliers = Seq(dp2),
        plan = plan
      )
      // NoOutlier pickle will be include 0.0 for each second in source range
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush, senderProbe.ref )
      senderProbe.expectMsg( GraphitePublisher.Flushed(true) )
      unpickleOutput() mustBe "foo 0.0 100\nfoo 1.0 117\n"
    }

    "write past full batch" in { f: Fixture =>
      import f._

      val graphite2 = TestActorRef[GraphitePublisher](
        Props(
          new GraphitePublisher with GraphitePublisher.PublishProvider {
            import scala.concurrent.duration._

            override val maxOutstanding: Int = 1000000
            override val separation: FiniteDuration = 1.second
            override def initializeMetrics(): Unit = { }
            override val batchSize: Int = 100
            override def destinationAddress: InetSocketAddress = f.address
            override def createSocket( address: InetSocketAddress ): Socket = {
              openCount.incrementAndGet()
              f.socket
            }
            override def publishingTopic( p: OutlierPlan, t: Topic ): Topic = "spotlight.outlier." + super.publishingTopic( p, t )
          }
        )
      )

      val outliers = SeriesOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo", Seq(dp1, dp2, dp3)),
        outliers = Seq(dp1),
        plan = plan
      )
      graphite2.receive( Publish(outliers) )
      graphite2.receive( Flush, senderProbe.ref )
      senderProbe.expectMsg( GraphitePublisher.Flushed(true) )
      unpickleOutput() mustBe "spotlight.outlier.plan.foo 1.0 100\nspotlight.outlier.plan.foo 0.0 117\nspotlight.outlier.plan.foo 0.0 9821\n"
    }

    "write full no-outlier batch" in { f: Fixture =>
      import f._
      val outliers = NoOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo", Seq(dp1, dp1b)),
        plan = plan
      )
      // NoOutlier pickle will be include 0.0 for each second in source range
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush, senderProbe.ref )
      senderProbe.expectMsg( GraphitePublisher.Flushed(true) )
      unpickleOutput() mustBe "foo 0.0 100\nfoo 0.0 101\nfoo 0.0 102\nfoo 0.0 103\n"
    }

    "write sanitize names" in { f: Fixture =>
      import f._
      val outliers = SeriesOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo bar", Seq(dp1, dp2, dp3)),
        outliers = Seq(dp1),
        plan = plan
      )
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush, senderProbe.ref )
      senderProbe.expectMsg( GraphitePublisher.Flushed(true) )
      unpickleOutput() mustBe "foo-bar 1.0 100\nfoo-bar 0.0 117\nfoo-bar 0.0 9821\n"
    }

    "ignores double opens" in { f: Fixture =>
      import f._
      graphite.receive( Open )
      graphite.receive( Open )
      openCount.get mustBe 1
    }



    "write past full batch with control boundaries" taggedAs (WIP) in { f: Fixture =>
      import f._

      val algos = Set( 'dbscan, 'x, 'y )
      val algConfig = ConfigFactory.parseString( algos.map{ a => s"${a.name}.publish-controls: yes" }.mkString("\n") )
      when( plan.algorithmConfig ) thenReturn algConfig

      algos foreach { a => plan.algorithmConfig.getBoolean( s"${a.name}.publish-controls" ) mustBe true }


      val graphite2 = TestActorRef[GraphitePublisher](
        Props(
          new GraphitePublisher with GraphitePublisher.PublishProvider {
            import scala.concurrent.duration._

            override val maxOutstanding: Int = 1000000
            override val separation: FiniteDuration = 1.second
            override def initializeMetrics(): Unit = { }
            override val batchSize: Int = 2
            override def destinationAddress: InetSocketAddress = f.address
            override def createSocket( address: InetSocketAddress ): Socket = {
              openCount.incrementAndGet()
              f.socket
            }
            override def publishingTopic( p: OutlierPlan, t: Topic ): Topic = "spotlight.outlier." + super.publishingTopic( p, t )
          }
        )
      )

      val controlBoundaries = Map(
        'x -> Seq(
          ControlBoundary.fromExpectedAndDistance(dp1.timestamp, 1, 0.1),
          ControlBoundary.fromExpectedAndDistance(dp2.timestamp, 1, 0.25),
          ControlBoundary.fromExpectedAndDistance(dp3.timestamp, 1, 0.3)
        ),
        'y -> Seq(
          ControlBoundary.fromExpectedAndDistance(dp1.timestamp, 3, 0.3),
          ControlBoundary.fromExpectedAndDistance(dp2.timestamp, 3, 0.5),
          ControlBoundary.fromExpectedAndDistance(dp3.timestamp, 3, 0.7)
        )
      )

      val outliers = SeriesOutliers(
        algorithms = algos,
        source = TimeSeries("foo", Seq(dp1, dp2, dp3)),
        outliers = Seq(dp1),
        plan = plan,
        algorithmControlBoundaries = controlBoundaries
      )

      graphite2.receive( Publish(outliers) )
      graphite2.receive( Flush, senderProbe.ref )
      senderProbe.expectMsg( GraphitePublisher.Flushed(true) )
      unpickleOutput() mustBe (
        "spotlight.outlier.plan.foo 1.0 100\nspotlight.outlier.plan.foo 0.0 117\nspotlight.outlier.plan.foo 0.0 9821\n" +
        "spotlight.outlier.plan.x.floor.foo 0.9 100\nspotlight.outlier.plan.x.expected.foo 1.0 100\nspotlight.outlier.plan.x.ceiling.foo 1.1 100\n" +
        "spotlight.outlier.plan.x.floor.foo 0.75 117\nspotlight.outlier.plan.x.expected.foo 1.0 117\nspotlight.outlier.plan.x.ceiling.foo 1.25 117\n" +
        "spotlight.outlier.plan.x.floor.foo 0.7 9821\nspotlight.outlier.plan.x.expected.foo 1.0 9821\nspotlight.outlier.plan.x.ceiling.foo 1.3 9821\n" +
        "spotlight.outlier.plan.y.floor.foo 2.7 100\nspotlight.outlier.plan.y.expected.foo 3.0 100\nspotlight.outlier.plan.y.ceiling.foo 3.3 100\n" +
        "spotlight.outlier.plan.y.floor.foo 2.5 117\nspotlight.outlier.plan.y.expected.foo 3.0 117\nspotlight.outlier.plan.y.ceiling.foo 3.5 117\n" +
        "spotlight.outlier.plan.y.floor.foo 2.3 9821\nspotlight.outlier.plan.y.expected.foo 3.0 9821\nspotlight.outlier.plan.y.ceiling.foo 3.7 9821\n"
      )
    }

  }
}


