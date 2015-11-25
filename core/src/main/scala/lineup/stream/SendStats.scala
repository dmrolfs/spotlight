package lineup.stream

import java.io._
import java.net.Socket
import java.nio.ByteBuffer

import akka.util.ByteString

import scala.concurrent.duration._
import com.typesafe.scalalogging.LazyLogging
import lineup.model.timeseries._
import org.apache.commons.math3.random.RandomDataGenerator
import org.joda.{ time => joda }
import com.github.nscala_time.time.OrderingImplicits._
import com.github.nscala_time.time.Imports.{ richSDuration, richDateTime }
import scopt.OptionParser
import peds.commons.log.Trace

import resource._

/**
  * Created by rolfsd on 11/23/15.
  */
object SendStats extends LazyLogging {
  val trace = Trace[SendStats.type]

  def main( args: Array[String] ): Unit = {
    val settings = Settings.parser.parse( args, Settings() ) getOrElse Settings()
    val usageMessage = s"""
      |\nRunning SendStats using the following configuration:
      |\tbinding       : ${settings.host}:${settings.port}
    """.stripMargin
    System.out.println( usageMessage )

    val now = new joda.DateTime( joda.DateTime.now.getMillis / 1000L * 1000L )
    val dps = makeDataPoints( points, start = now, period = 1.second )

    val tss = Seq( dps )
    val topics = Seq( "foo", "bar", "foo" )

    val message = withHeader( pickled( topics zip tss ) )

    for {
      connection <- managed( new Socket(settings.host, settings.port) )
      outStream <- managed( connection.getOutputStream )
      out = new PrintWriter( new BufferedWriter( new OutputStreamWriter(outStream) ) )
      inStream <- managed( new InputStreamReader(connection.getInputStream) )
      in = new BufferedReader( inStream )
    } {
      System.out.println( s"Sending to ${settings.host}:${settings.port} [${tss.map{_.size}.sum}] data points" )
      outStream write message.toArray
      outStream.flush
//      out.write( message.decodeString("ISO-8859-1") )
//      out.flush
      System.out.println( s"""receiving:\n${in.readLine}\n\n""")
    }
  }

  case class Settings( host: String = "127.0.0.1", port: Int = 2004 )
  object Settings {
    def parser = new OptionParser[Settings]( "SendStats" ) {
      head( "SendStats", "0.1.0" )

      opt[String]( 'h', "host" ) action { (e, c) =>
        c.copy( host = e )
      } text( "address to host" )

      opt[Int]( 'p', "port" ) action { (e, c) =>
        c.copy( port = e )
      } text( "address port" )
    }
  }


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
}
