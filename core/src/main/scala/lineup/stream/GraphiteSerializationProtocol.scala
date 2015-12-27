package lineup.stream

import java.nio.charset.Charset
import akka.stream.{ActorAttributes, Supervision}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import lineup.model.timeseries.TimeSeries


/**
  * Created by rolfsd on 11/25/15.
  */
trait GraphiteSerializationProtocol {
  def framingFlow( maximumFrameLength: Int ): Flow[ByteString, ByteString, Unit]
  def toDataPoints( bytes: ByteString ): List[TimeSeries]
  def loadTimeSeriesData: Flow[ByteString, TimeSeries, Unit] = {
    Flow[ByteString]
    .mapConcat { toDataPoints }
    .withAttributes( ActorAttributes.supervisionStrategy(GraphiteSerializationProtocol.decider) )
  }
}

object GraphiteSerializationProtocol extends LazyLogging {
  trait ProtocolException extends Exception {
    def part: String
    def value: Any
  }

  val decider: Supervision.Decider = {
    case ex: ProtocolException => {
      logger.warn(
        "ignoring message - could not parse " +
        s"part:[${ex.part}] message-value:[${ex.value.toString}] type:[${ex.value.getClass.getName}]"
      )

      Supervision.Resume
    }

    case _ => Supervision.Stop
  }
}