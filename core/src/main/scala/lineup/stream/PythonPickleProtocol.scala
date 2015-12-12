package lineup.stream

import java.math.BigInteger
import java.nio.{ByteBuffer, ByteOrder}
import akka.stream.io.Framing
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import org.joda.{ time => joda }
import peds.commons.log.Trace
import lineup.model.timeseries._


/**
  * Created by rolfsd on 11/25/15.
  */
case object PythonPickleProtocol extends GraphiteSerializationProtocol {
  val trace = Trace[PythonPickleProtocol.type]

  final case class PickleException private[stream]( part: String, value: Any )
  extends Exception( s"failed to parse ${part} for [${value.toString}] of type [${value.getClass.getName}]" )
  with GraphiteSerializationProtocol.ProtocolException


  val SizeFieldLength = 4
  val GraphiteMaximumFrameLength = SizeFieldLength + scala.math.pow( 2, 20 ).toInt

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


  implicit class PickleMessage( val body: ByteString ) extends AnyVal {
    def withHeader: ByteString = {
      val result = ByteBuffer.allocate( 4 + body.size )
      result putInt body.size
      result put body.toArray
      result.flip()
      ByteString( result )
    }
  }

  def pickle(dp: Row[DataPoint] ): ByteString = pickle( Seq(("foobar", dp) ) )

  def pickle(ts: TimeSeries ): ByteString = pickle( Seq( (ts.topic.name, ts.points) ) )

  def pickle(metrics: Seq[(String, Row[DataPoint])] ): ByteString = {
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

    val pickler = new Pickler( false )
    val out = pickler dumps data

    //    trace( s"""payload[${out.size}] = ${ByteString(out).decodeString("ISO-8859-1")}""" )
    ByteString( out )
  }

}
