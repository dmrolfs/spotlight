package spotlight.model.timeseries

import cats.syntax.cartesian._
import cats.syntax.validated._
import shapeless.Lens
import org.joda.{ time ⇒ joda }
import com.github.nscala_time.time.Imports._
import omnibus.commons.AllIssuesOr
import omnibus.commons.util._

sealed trait TimeSeries extends TimeSeriesBase {
  override def size: Int = points.size
  def contains( ts: DateTime ): Boolean = points exists { _.timestamp == ts }
  override def toString: String = s"""${getClass.safeSimpleName}:"${topic}"[${points.mkString( "," )}]"""
}

object TimeSeries {
  def apply( topic: Topic, points: Seq[DataPoint] = Seq.empty[DataPoint] ): TimeSeries = {
    val sorted = points sortBy { _.timestamp }
    val ( start, end ) = if ( sorted.nonEmpty ) ( Some( sorted.head.timestamp ), Some( sorted.last.timestamp ) ) else ( None, None )
    SimpleTimeSeries( topic = topic, points = sorted, start = start, end = end )
  }

  val topicLens: Lens[TimeSeries, Topic] = new Lens[TimeSeries, Topic] {
    override def get( ts: TimeSeries ): Topic = ts.topic
    override def set( ts: TimeSeries )( t: Topic ): TimeSeries = TimeSeries( topic = t, points = ts.points )
  }

  val pointsLens: Lens[TimeSeries, Seq[DataPoint]] = new Lens[TimeSeries, Seq[DataPoint]] {
    override def get( ts: TimeSeries ): Seq[DataPoint] = ts.points
    override def set( ts: TimeSeries )( ps: Seq[DataPoint] ): TimeSeries = TimeSeries( topic = ts.topic, points = ps )
  }

  implicit val seriesMerging: TimeSeriesBase.Merging[TimeSeries] = new TimeSeriesBase.Merging[TimeSeries] {
    override def zero( topic: Topic ): TimeSeries = TimeSeries( topic )

    override def merge( lhs: TimeSeries, rhs: TimeSeries ): AllIssuesOr[TimeSeries] = {
      ( checkTopic( lhs.topic, rhs.topic ) |@| combinePoints( lhs.points, rhs.points ) ) map { ( _, merged ) ⇒
        pointsLens.set( lhs )( merged )
      }
    }

    private def combinePoints( lhs: Seq[DataPoint], rhs: Seq[DataPoint] ): AllIssuesOr[Seq[DataPoint]] = {
      val merged = lhs ++ rhs
      val ( uniques, dups ) = merged.groupBy { _.timestamp }.values.partition { _.size == 1 }

      val dupsAveraged = for {
        d ← dups
        ts = d.head.timestamp
        values = d map { _.value }
        avg = values.sum / values.size.toDouble
      } yield DataPoint( timestamp = ts, value = avg )

      val normalized = uniques.flatten ++ dupsAveraged
      normalized.toIndexedSeq.sortBy { _.timestamp }.validNel
    }

  }

  final case class SimpleTimeSeries private[timeseries] (
    override val topic: Topic,
    override val points: Seq[DataPoint],
    override val start: Option[joda.DateTime],
    override val end: Option[joda.DateTime]
  ) extends TimeSeries
}
