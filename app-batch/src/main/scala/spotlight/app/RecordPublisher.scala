package spotlight.app

import scala.annotation.tailrec
import scala.collection.immutable
import akka.actor.Props
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }

/** Created by rolfsd on 4/21/17.
  */
object RecordPublisher {
  def props: Props = Props( new RecordPublisher )

  final case class Record( payload: immutable.Iterable[String] )
  case object RecordAccepted
  case object RecordDenied
}

class RecordPublisher extends ActorPublisher[RecordPublisher.Record] {
  import RecordPublisher.{ Record, RecordAccepted, RecordDenied }

  val MaxBufferSize = 100
  var buf = Vector.empty[Record]

  override def receive: Receive = {
    case r: Record if buf.size == MaxBufferSize ⇒ sender() ! RecordDenied

    case r: Record ⇒ {
      if ( buf.isEmpty && totalDemand > 0 ) onNext( r )
      else {
        buf :+= r
        deliverBuf()
      }

      sender() ! RecordAccepted
    }

    case Request( _ ) ⇒ deliverBuf()

    case Cancel ⇒ context stop self
  }

  @tailrec final def deliverBuf(): Unit = {
    if ( totalDemand > 0 ) {
      if ( totalDemand <= Int.MaxValue ) {
        val ( use, keep ) = buf.splitAt( totalDemand.toInt )
        use foreach onNext
        buf = keep
      } else {
        val ( use, keep ) = buf.splitAt( Int.MaxValue )
        use foreach onNext
        buf = keep
        deliverBuf()
      }
    }
  }
}
