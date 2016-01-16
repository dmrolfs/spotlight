package lineup.publish

import akka.actor.{ Actor, ActorLogging }
import lineup.model.outlier.{ NoOutliers, Outliers, SeriesOutliers }
import lineup.model.timeseries._
import org.joda.{ time => joda }
import peds.akka.metrics.InstrumentedActor

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration


/**
  * Created by rolfsd on 12/31/15.
  */
object OutlierPublisher {
  sealed trait Protocol
  case class Publish( outliers: Outliers ) extends Protocol
  case class Published( outliers: Outliers ) extends Protocol
}

trait OutlierPublisher extends Actor with InstrumentedActor with ActorLogging {
  def publish( o: Outliers ): Unit

  type MarkPoint = (String, joda.DateTime, Double)

  def markPoints( o: Outliers ): Seq[MarkPoint] = mark( o ) map { dp => ( o.topic.name, dp.timestamp, dp.value ) }

  def mark( o: Outliers ): Seq[DataPoint] = {
    o match {
      case SeriesOutliers(_, source, outliers) => {
        val identified = outliers.map{ _.timestamp }.toSet
        source.points.collect{
          case DataPoint(ts, _) if identified.contains(ts) => DataPoint( ts, 1D )
          case DataPoint(ts, _) => DataPoint( ts, 0D )
        }
      }

      case _ => Seq.empty[DataPoint]
    }
  }
}


trait DenseOutlierPublisher extends OutlierPublisher {
  def fillSeparation: FiniteDuration

  override def mark( o: Outliers ): Seq[DataPoint] = {
    val result = o match {
      case NoOutliers(_, source) => {
        import com.github.nscala_time.time.Imports._

        @tailrec def fillInterval( timePoint: joda.DateTime, range: joda.Interval, acc: Seq[DataPoint] ): Seq[DataPoint] = {
          if ( !range.contains(timePoint) ) acc
          else fillInterval( timePoint + fillSeparation.toMillis, range, acc :+ DataPoint(timePoint, 0D) )
        }

        source.interval map { i =>
          Some( fillInterval(i.start, i, Seq.empty[DataPoint]) )
        } getOrElse {
          None
        }
      }

      case _ => None
    }

    result getOrElse super.mark( o )
  }
}
