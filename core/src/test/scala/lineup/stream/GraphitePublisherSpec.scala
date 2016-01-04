package lineup.stream

import java.io.ByteArrayOutputStream
import java.net.{ InetAddress, Socket, InetSocketAddress }
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import javax.net._
import javax.script.{ SimpleBindings, Compilable, ScriptEngineManager }
import akka.actor.Props
import akka.testkit.TestActorRef
import akka.util.ByteString
import org.joda.{ time => joda }
import lineup.model.outlier.{ SeriesOutliers, NoOutliers }
import lineup.model.timeseries.{ DataPoint, Row, TimeSeries }
import lineup.testkit.ParallelAkkaSpec
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.python.core.{ PyList, PyTuple }
import peds.commons.log.Trace

import scala.collection.generic.AtomicIndexFlag


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
  import GraphitePublisherSpec._
  import GraphitePublisher._
  import OutlierPublisher._

  override val trace = Trace[GraphitePublisherSpec]

  class Fixture extends AkkaFixture { outer =>
    val connected: AtomicBoolean = new AtomicBoolean( true )
    val closed: AtomicBoolean = new AtomicBoolean( false )
    val openCount: AtomicInteger = new AtomicInteger( 0 )
    val address: InetSocketAddress = new InetSocketAddress( "example.com", 1234 )
    val output: ByteArrayOutputStream = spy( new ByteArrayOutputStream )
    val socketFactory: SocketFactory = mock[SocketFactory]

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

    when( socketFactory.createSocket( any(classOf[InetAddress]) , anyInt ) ).thenReturn( socket )

    val graphite = TestActorRef[GraphitePublisher](
      Props(
        new GraphitePublisher(address) with GraphitePublisher.SocketProvider {
          override def createSocket( address: InetSocketAddress ): Socket = {
            openCount.incrementAndGet()
            outer.socket
          }
        }
      )
    )

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
      graphite.receive( Close )
      verify( socket ).close()
    }

    "first replicate python protocol test" in { f: Fixture =>
      import f._
      import org.joda.{ time => joda }

      val batch = Seq(
        ("foo", new joda.DateTime(100000L), 1D),
        ("bar", new joda.DateTime(117000L), 0D),
        ("zed", new joda.DateTime(9821000L), 0D)
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
        source = TimeSeries("foo", Row(dp1))
      )
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush )
      val actual = ByteString( output.toByteArray )
      unpickleOutput( actual ) mustBe "foo 0.0 100\n"
    }

    "write full batch" taggedAs (WIP) in { f: Fixture =>
      import f._
      val outliers = SeriesOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo", Row(dp1, dp2)),
        outliers = Row(dp2)
      )
      // NoOutlier pickle will be include 0.0 for each second in source range
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush )
      unpickleOutput() mustBe "foo 0.0 100\nfoo 1.0 117\n"
    }

    "write past full batch" in { f: Fixture =>
      import f._
      val outliers = SeriesOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo", Row(dp1, dp2, dp3)),
        outliers = Row(dp1)
      )
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush )
      unpickleOutput() mustBe "foo 1.0 100\nfoo 0.0 117\nfoo 0.0 9821\n"
    }

    "write full no-outlier batch" taggedAs (WIP) in { f: Fixture =>
      import f._
      val outliers = NoOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo", Row(dp1, dp1b))
      )
      // NoOutlier pickle will be include 0.0 for each second in source range
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush )
      unpickleOutput() mustBe "foo 0.0 100\nfoo 0.0 101\nfoo 0.0 102\nfoo 0.0 103\n"
    }

    "write sanitize names" in { f: Fixture =>
      import f._
      val outliers = SeriesOutliers(
        algorithms = Set('dbscan),
        source = TimeSeries("foo bar", Row(dp1, dp2, dp3)),
        outliers = Row(dp1)
      )
      graphite.receive( Publish(outliers) )
      graphite.receive( Flush )
      unpickleOutput() mustBe "foo-bar 1.0 100\nfoo-bar 0.0 117\nfoo-bar 0.0 9821\n"
    }

    "ignores double opens" in { f: Fixture =>
      import f._
      graphite.receive( Open )
      graphite.receive( Open )
      openCount.get mustBe 1
    }
  }
}


