package lineup.stream

import akka.stream.scaladsl.Flow
import akka.util.ByteString
import lineup.model.timeseries.TimeSeries


/**
  * Created by rolfsd on 11/25/15.
  */
trait GraphiteSerializationProtocol {
  def charset: String = ByteString.UTF_8
  def framingFlow( maximumFrameLength: Int ): Flow[ByteString, ByteString, Unit]
  def loadTimeSeriesData: Flow[ByteString, TimeSeries, Unit] = Flow[ByteString] mapConcat { toDataPoints }
  def toDataPoints( bytes: ByteString ): List[TimeSeries]
}
