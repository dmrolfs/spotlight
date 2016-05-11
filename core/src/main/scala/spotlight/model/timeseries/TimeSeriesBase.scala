package spotlight.model.timeseries

import org.joda.{ time => joda }
import com.github.nscala_time.time.Imports._
import scalaz.Scalaz._
import scalaz.Validation
import peds.commons._


trait TimeSeriesBase {
  def topic: Topic
  def points: Seq[DataPoint]
  def pointsAsPairs: Seq[PointT] = points.map{ dp => (dp.timestamp.getMillis.toDouble, dp.value) }
  def size: Int
  def start: Option[joda.DateTime]
  def end: Option[joda.DateTime]
  def interval: Option[joda.Interval] = {
    for {
      s <- start
      e <- end
    } yield new joda.Interval( s, e + 1.millis )
  }
}

object TimeSeriesBase {
  final case class IncompatibleTopicsError private[timeseries]( originalTopic: Topic, newTopic: Topic )
    extends IllegalArgumentException( s"cannot merge time series topics: original [${originalTopic}] amd mew topic [${newTopic}]" )
            with TimeSeriesError


  trait Merging[T <: TimeSeriesBase] {
    def merge( lhs: T, rhs: T ): Valid[T]
    def zero( topic: Topic ): T

    protected def checkTopic( lhs: Topic, rhs: Topic ): Valid[Topic] = {
      if ( lhs == rhs ) lhs.successNel[Throwable]
      else Validation.failureNel( IncompatibleTopicsError(originalTopic = lhs, newTopic = rhs) )
    }
  }
}


