package lineup.stream

import java.io.{ OutputStreamWriter, ByteArrayOutputStream }
import java.math.BigInteger
import java.nio.charset.Charset
import java.nio.{ ByteBuffer, ByteOrder }
import java.util
import akka.stream.io.Framing
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import net.razorvine.pickle.Pickler
import org.joda.{ time => joda }
import peds.commons.log.Trace
import lineup.model.timeseries._


/**
  * Created by rolfsd on 11/25/15.
  */
object PythonPickleProtocol {
  val SizeFieldLength = 4
  val GraphiteMaximumFrameLength = SizeFieldLength + scala.math.pow( 2, 20 ).toInt

  implicit class PickleMessage( val body: ByteString ) extends AnyVal {
    def withHeader: ByteString = {
      val result = ByteBuffer.allocate( 4 + body.size )
      result putInt body.size
      result put body.toArray
      result.flip()
      ByteString( result )
    }
  }


  final case class PickleException private[stream]( part: String, value: Any )
  extends Exception( s"failed to parse ${part} for [${value.toString}] of type [${value.getClass.getName}]" )
  with GraphiteSerializationProtocol.ProtocolException

  private[stream] object Opcodes {
    val charset = Charset forName "UTF-8"

    val MARK = '('
    val STOP = '.'
    val LONG = 'L'
    val STRING = 'S'
    val APPEND = 'a'
    val LIST = 'l'
    val TUPLE = 't'
    val QUOTE = '\''
    val LF = '\n'
  }
}

class PythonPickleProtocol extends GraphiteSerializationProtocol with LazyLogging {
  import PythonPickleProtocol._

  val trace = Trace[PythonPickleProtocol]

  // Pickler is not thread safe so protocols need to be instantiated per thread.
  // If perf is an issue, may consider hand-coding pickler based on dropwizard graphite reporter.
  val pickler = new Pickler( false )

  override def framingFlow( maximumFrameLength: Int = GraphiteMaximumFrameLength ): Flow[ByteString, ByteString, Unit] = {
    Framing.lengthField(
      fieldLength = SizeFieldLength,
      fieldOffset = 0,
      maximumFrameLength = maximumFrameLength,
      byteOrder = ByteOrder.BIG_ENDIAN
    ).map { _ drop SizeFieldLength }
  }

  case class Metric( topic: Topic, timestamp: joda.DateTime, value: Double )

  override def toDataPoints( bytes: ByteString ): List[TimeSeries] = {
    import scala.collection.convert.wrapAll._
    import net.razorvine.pickle.Unpickler

    if ( bytes.isEmpty ) throw PickleException( part = "all", value = bytes )
    else {
      val unpickler = new Unpickler
      val pck = unpickler.loads( bytes.toArray )
      val pickles = pck.asInstanceOf[java.util.ArrayList[Array[AnyRef]]]

      val metrics = for {
        p <- pickles
      } yield {
        val tuple = p
        val dp = tuple(1).asInstanceOf[Array[AnyRef]]

        val topic: String = tuple( 0 ).asInstanceOf[Any] match {
          case s: String => s
          case unknown => throw PickleException( part = "topic", value = unknown )
        }

        val unixEpochSeconds: Long = dp(0).asInstanceOf[Any] match {
          case l: Long => l
          case i: Int => i.toLong
          case bi: BigInteger => bi.longValue
          case d: Double => d.toLong
          case s: String => s.toLong
          case unknown => throw PickleException( part = "timestamp", value = unknown )
        }

        val v: Double = dp(1).asInstanceOf[Any] match {
          case d: Double => d
          case f: Float => f.toDouble
          case bd: BigDecimal => bd.doubleValue
          case i: Int => i.toDouble
          case l: Long => l.toDouble
          case s: String =>s.toDouble
          case unknown => throw PickleException( part = "value", value = unknown )
        }

        Metric( topic = topic, timestamp = new joda.DateTime(unixEpochSeconds * 1000L), value = v )
      }

      val result = metrics groupBy { _.topic } map { case (t, ms) =>
        val points = ms map { m => DataPoint( timestamp = m.timestamp, value = m.value ) }
        TimeSeries( topic = t, points = points.toIndexedSeq )
      }

      result.toList
    }
  }

  type DataPointTuple = (String, Long, String)

  def pickle( points: DataPointTuple* )( implicit charset: Charset = Opcodes.charset ): ByteString = {
    val out = new ByteArrayOutputStream( points.size * 75 ) // Extremely rough estimate of 75 bytes per message
    val pickled = new OutputStreamWriter( out, charset )

    import Opcodes._

    pickled append MARK
    pickled append LIST

    points map { case (n, ts, v) =>
      (sanitize(n), ts, sanitize(v))
    } foreach { case (name, timestamp, value) =>
      // start the outer tuple
      pickled append MARK

      // the metric name is a string.
      pickled append STRING
      // the single quotes are to match python's repr("abcd")
      pickled append QUOTE
      pickled append name
      pickled append QUOTE
      pickled append LF

      // start the inner tuple
      pickled append MARK

      // timestamp is a long
      pickled append LONG
      pickled append timestamp.toString
      // the trailing L is to match python's repr(long(1234))
      pickled append LONG
      pickled append LF

      // and the value is a string.
      pickled append STRING
      pickled append QUOTE
      pickled append value
      pickled append QUOTE
      pickled append LF

      pickled append TUPLE // inner close
      pickled append TUPLE // outer close

      pickled append APPEND
    }

    // every pickle ends with STOP
    pickled append STOP

    pickled.flush

    return ByteString( out.toByteArray ).withHeader
  }


  def pickleFlattenedTimeSeries(
    points: (String, joda.DateTime, Double)*
  )(
    implicit charset: Charset = Opcodes.charset
  ): ByteString = {
    pickle( points map { case (name, timestamp, value) => ( name, timestamp.getMillis / 1000L, value.toString ) }:_* )
  }

  def pickleTimeSeries( ts: TimeSeries )( implicit charset: Charset = Opcodes.charset ): ByteString = {
    pickleFlattenedTimeSeries( ts.points map { dp => ( ts.topic.name, dp.timestamp, dp.value ) }:_* )
  }

  val Whitespace = "[\\s]+".r
  def sanitize( s: String ): String = Whitespace.replaceAllIn( s, "-" )
}
