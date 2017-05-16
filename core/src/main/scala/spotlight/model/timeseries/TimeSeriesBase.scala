package spotlight.model.timeseries

import cats.syntax.validated._
import org.joda.{ time ⇒ joda }
import com.github.nscala_time.time.Imports._
import omnibus.commons.AllIssuesOr

trait TimeSeriesBase {
  def topic: Topic
  def points: Seq[DataPoint]
  def size: Int
  def start: Option[joda.DateTime]
  def end: Option[joda.DateTime]
  def interval: Option[joda.Interval] = {
    for {
      s ← start
      e ← end
    } yield new joda.Interval( s, e + 1.millis )
  }
}

object TimeSeriesBase {
  final case class IncompatibleTopicsError private[timeseries] ( originalTopic: Topic, newTopic: Topic )
    extends IllegalArgumentException( s"cannot merge time series topics: original [${originalTopic}] amd mew topic [${newTopic}]" )
    with TimeSeriesError

  trait Merging[T <: TimeSeriesBase] {
    def merge( lhs: T, rhs: T ): AllIssuesOr[T]
    def zero( topic: Topic ): T

    protected def checkTopic( lhs: Topic, rhs: Topic ): AllIssuesOr[Topic] = {
      if ( lhs == rhs ) lhs.validNel else IncompatibleTopicsError( originalTopic = lhs, newTopic = rhs ).invalidNel
    }
  }
}
