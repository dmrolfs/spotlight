package spotlight.protocol

import akka.NotUsed
import akka.stream.{ActorAttributes, Supervision}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import spotlight.model.timeseries.TimeSeries


/**
  * Created by rolfsd on 1/12/16.
  */
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

trait GraphiteSerializationProtocol {
  def framingFlow( maximumFrameLength: Int ): Flow[ByteString, ByteString, NotUsed]
  def toTimeSeries( bytes: ByteString ): List[TimeSeries]

  def unmarshalTimeSeriesData: Flow[ByteString, TimeSeries, NotUsed] = {
    Flow[ByteString]
    .mapConcat { toTimeSeries }
    .withAttributes( ActorAttributes.supervisionStrategy(GraphiteSerializationProtocol.decider) )
  }
}
