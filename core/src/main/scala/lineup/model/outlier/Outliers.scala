package lineup.model.outlier

import scala.annotation.tailrec
import org.joda.{ time => joda }
import peds.commons.log.Trace
import peds.commons.util._
import lineup.model.timeseries._


sealed trait Outliers {
  type Source <: TimeSeriesBase
  def topic: Topic
  def algorithms: Set[Symbol]
  def hasAnomalies: Boolean
  def source: Source
}

object Outliers {
  def unapply( so: Outliers ): Option[(Topic, Set[Symbol], Boolean, so.Source)] = {
    Some( (so.topic, so.algorithms, so.hasAnomalies, so.source) )
  }
}

case class NoOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeriesBase
) extends Outliers {
  override type Source = TimeSeriesBase
  override val topic: Topic = source.topic
  override val hasAnomalies: Boolean = false

  override def toString: String = s"""${getClass.safeSimpleName}:${topic}"""
}

case class SeriesOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeries,
  outliers: IndexedSeq[DataPoint]
) extends Outliers {
  val trace = Trace[SeriesOutliers]
  override type Source = TimeSeries
  override val topic: Topic = source.topic
  override val hasAnomalies: Boolean = outliers.nonEmpty

  type OutlierGroups = Map[joda.DateTime, Double]

  def anomalousGroups: Seq[OutlierGroups] = trace.block( "anomalousGroups" ) {
    def nonEmptyAccumulator( acc: List[OutlierGroups] ): List[OutlierGroups] = trace.briefBlock( s"nonEmptyAccumulator" ) {
      if ( acc.nonEmpty ) acc else List[OutlierGroups]( Map.empty[joda.DateTime, Double] )
    }

    @tailrec def loop( points: List[DataPoint], isPreviousOutlier: Boolean, acc: List[OutlierGroups] ): Seq[OutlierGroups] = {
      points match {
        case Nil => acc.reverse.toSeq

        case h :: tail if isPreviousOutlier == true && outliers.contains(h) => {
          val cur :: accTail = nonEmptyAccumulator( acc )
          val newCurrent = cur + (h.timestamp -> h.value)
          loop( points = tail, isPreviousOutlier = true, acc = newCurrent :: accTail )
        }

        case h :: tail if isPreviousOutlier == false && outliers.contains(h) => {
          val newGroup: OutlierGroups = Map( (h.timestamp -> h.value) )
          loop( points = tail, isPreviousOutlier = true, acc = newGroup :: acc )
        }

        case h :: tail => loop( points = tail, isPreviousOutlier = false, acc )
      }
    }

    trace( s"""outliers=[${outliers.mkString(",")}]""" )
    trace( s"""points=[${source.points.mkString(",")}]""" )

    loop( points = source.points.toList, isPreviousOutlier = false, acc = List.empty[OutlierGroups] )
  }

  override def toString: String = s"""${getClass.safeSimpleName}:${topic}[${outliers.mkString(",")}]"""
}


case class CohortOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeriesCohort,
  outliers: Set[TimeSeries] = Set()
) extends Outliers {
  override type Source = TimeSeriesCohort
  override val topic: Topic = source.topic
  override val hasAnomalies: Boolean = outliers.nonEmpty

  override def toString: String = s"""${getClass.safeSimpleName}:${topic}[${outliers.mkString(",")}]"""
}
