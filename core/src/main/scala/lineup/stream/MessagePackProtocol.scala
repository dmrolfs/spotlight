package lineup.stream

import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import lineup.model.timeseries.TimeSeries


/**
  * Created by rolfsd on 11/25/15.
  */
case object MessagePackProtocol extends GraphiteSerializationProtocol with LazyLogging {
  override def framingFlow(maximumFrameLength: Int): Flow[ByteString, ByteString, Unit] = ???

  override def toDataPoints( bytes: ByteString ): List[TimeSeries] = {
    import org.velvia.MsgPack
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import com.fasterxml.jackson.databind.SerializationFeature
    org.json4s.jackson.JsonMethods.mapper.configure( SerializationFeature.CLOSE_CLOSEABLE, false )

    import org.velvia.msgpack.Json4sCodecs._
    logger info "unpacking..."
    val payload = MsgPack unpack bytes.toArray
    logger info "...unpacked"
    logger error s"payload class: ${payload.getClass}"
    logger error s"payload: $payload"
    throw new IllegalArgumentException( s"UNPACKED: [[${payload}]]" )
    //      List.empty[TimeSeries]
  }
}
