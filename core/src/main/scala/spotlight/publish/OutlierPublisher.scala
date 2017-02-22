package spotlight.publish

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorLogging
import akka.stream.actor.{ ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy }
import org.joda.{ time ⇒ joda }
import omnibus.akka.metrics.InstrumentedActor
import spotlight.model.outlier.{ NoOutliers, Outliers, SeriesOutliers }
import spotlight.model.timeseries._

/** Created by rolfsd on 12/31/15.
  */
object OutlierPublisher {
  sealed trait Protocol
  case class Publish( outliers: Outliers ) extends Protocol
  case class Published( outliers: Outliers ) extends Protocol

  type TopicPoint = ( Topic, joda.DateTime, Double )

}

trait OutlierPublisher extends ActorSubscriber with InstrumentedActor with ActorLogging {

  import OutlierPublisher.TopicPoint

  def publish( o: Outliers ): Unit

  def markPoints( o: Outliers ): Seq[TopicPoint] = mark( o ) map { dp ⇒ ( o.topic, dp.timestamp, dp.value ) }

  def mark( o: Outliers ): Seq[DataPoint] = {
    o match {
      case SeriesOutliers( _, source, _, outliers, _ ) ⇒ {
        val identified = outliers.map { _.timestamp }.toSet
        source.points.collect {
          case dp if identified contains dp.timestamp ⇒ dp.copy( value = 1D )
          case dp ⇒ dp.copy( value = 0D )
        }
      }

      case _ ⇒ Seq.empty[DataPoint]
    }
  }
}

trait DenseOutlierPublisher extends OutlierPublisher {
  def fillSeparation: FiniteDuration

  override def mark( o: Outliers ): Seq[DataPoint] = {
    val result = o match {
      case expected: NoOutliers ⇒ {
        import com.github.nscala_time.time.Imports._

        @tailrec def fillInterval( timePoint: joda.DateTime, range: joda.Interval, acc: Seq[DataPoint] ): Seq[DataPoint] = {
          log.debug( "fillInterval( timePoint:[{}], range:[{}], acc:[{}] )", timePoint, range, acc )
          if ( !range.contains( timePoint ) ) {
            log.debug( "fillInterval result: [{}]", acc )
            acc
          } else fillInterval( timePoint + fillSeparation.toMillis, range, acc :+ DataPoint( timePoint, 0D ) )
        }

        log.debug( "fillInterval loop: fillSeparation=[{}]", fillSeparation )
        expected.source.interval map { i ⇒ Some( fillInterval( i.start, i, Seq.empty[DataPoint] ) ) } getOrElse { None }
      }

      case _ ⇒ None
    }

    result getOrElse super.mark( o )
  }
}
