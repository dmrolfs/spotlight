package lineup.stream

import java.math.BigInteger
import java.nio.ByteOrder

import akka.stream.io.Framing
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import lineup.model.timeseries.{DataPoint, Topic, TimeSeries}
import org.joda.{ time => joda }
import peds.commons.log.Trace


/**
  * Created by rolfsd on 11/25/15.
  */
case object PythonPickleProtocol extends GraphiteSerializationProtocol {
  val trace = Trace[PythonPickleProtocol.type]

  final case class PickleException private[stream]( message: String ) extends Exception( message )

  val SizeFieldLength = 4
  val GraphiteMaximumFrameLength = SizeFieldLength + scala.math.pow( 2, 20 ).toInt

  override val charset: String = "ISO-8859-1"

  override def framingFlow( maximumFrameLength: Int = GraphiteMaximumFrameLength ): Flow[ByteString, ByteString, Unit] = {
    Framing.lengthField(
      fieldLength = SizeFieldLength,
      fieldOffset = 0,
      maximumFrameLength = maximumFrameLength,
      byteOrder = ByteOrder.BIG_ENDIAN
    ).map { _ drop SizeFieldLength }
  }

  case class Metric( topic: Topic, timestamp: joda.DateTime, value: Double )

  override def toDataPoints( bytes: ByteString ): List[TimeSeries] = trace.briefBlock( "toDataPoints" ) {
    import scala.collection.convert.wrapAll._
    import net.razorvine.pickle.Unpickler

    if ( bytes.isEmpty ) throw PickleException( "stream pushing with empty bytes" )
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
          case unknown => throw PickleException( s"failed to parse topic [$unknown] type not handled [${unknown.getClass}]" )
        }

        val unixEpochSeconds: Long = dp(0).asInstanceOf[Any] match {
          case l: Long => l
          case i: Int => i.toLong
          case bi: BigInteger => bi.longValue
          case d: Double => d.toLong
          case unknown => throw PickleException( s"failed to parse timestamp [$unknown] type not handled [${unknown.getClass}]" )
        }

        val v: Double = dp(1).asInstanceOf[Any] match {
          case d: Double => d
          case f: Float => f.toDouble
          case bd: BigDecimal => bd.doubleValue
          case i: Int => i.toDouble
          case l: Long => l.toDouble
          case unknown => throw PickleException( s"failed to parse value [$unknown] type not handled [${unknown.getClass}]" )
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
}
