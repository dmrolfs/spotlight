package spotlight.publish

import akka.actor.Props
import akka.event.LoggingReceive
import com.typesafe.scalalogging.Logger
import spotlight.model.outlier.Outliers
import org.slf4j.LoggerFactory


/**
  * Created by rolfsd on 12/31/15.
  */
object LogPublisher {
  def props: Props = Props( new LogPublisher )
}

class LogPublisher extends OutlierPublisher {
  import OutlierPublisher._

  val outlierLogger = Logger( LoggerFactory getLogger "Outliers" )

  override def receive: Receive = around{
    LoggingReceive {
      case Publish( outliers ) => {
        publish( outliers )
        sender() ! Published( outliers )
      }
    }
  }

  override def publish( outliers: Outliers ): Unit = outlierLogger info outliers.toString
}
