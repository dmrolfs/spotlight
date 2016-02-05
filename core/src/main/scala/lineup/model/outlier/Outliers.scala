package lineup.model.outlier

import scala.annotation.tailrec
import org.joda.{ time => joda }
import peds.commons.log.Trace
import peds.commons.util._
import lineup.model.timeseries._


//todo re-seal with FanOutShape Outlier Detection
trait Outliers extends Equals {
  private val trace = Trace[Outliers]
  type Source <: TimeSeriesBase
  def topic: Topic
  def algorithms: Set[Symbol]
  def hasAnomalies: Boolean
  def size: Int
  def anomalySize: Int
  def source: Source
  def plan: OutlierPlan

  override def hashCode: Int = {
    41 * (
      41 * (
        41 * (
          41 + topic.##
        ) + algorithms.##
      ) + anomalySize.##
    ) + source.##
  }

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: Outliers => {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
          ( that canEqual this ) &&
          ( this.topic == that.topic ) &&
          ( this.algorithms == that.algorithms ) &&
          ( this.anomalySize == that.anomalySize ) &&
          ( this.source == that.source )
        }
      }

      case _ => false
    }
  }
}

object Outliers {
  type OutlierGroups = Map[joda.DateTime, Double]

  def unapply( so: Outliers ): Option[(Topic, Set[Symbol], Boolean, so.Source)] = {
    Some( (so.topic, so.algorithms, so.hasAnomalies, so.source) )
  }
}

case class NoOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeriesBase,
  override val plan: OutlierPlan
) extends Outliers {
  override type Source = TimeSeriesBase
  override def topic: Topic = source.topic
  override def size: Int = source.size
  override val hasAnomalies: Boolean = false
  override val anomalySize: Int = 0

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[NoOutliers]

  override def toString: String = {
    s"""${getClass.safeSimpleName}:"${topic}"[source:[${source.size}] interval:[${source.interval getOrElse "No Interval"}]"""
  }
}

case class SeriesOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeries,
  override val plan: OutlierPlan,
  outliers: IndexedSeq[DataPoint]
) extends Outliers {
  import Outliers._
  private val trace: Trace[_] = Trace[SeriesOutliers]

  override type Source = TimeSeries
  override def topic: Topic = source.topic
  override def size: Int = source.size
  override def hasAnomalies: Boolean = outliers.nonEmpty
  override def anomalySize: Int = outliers.size

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

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[SeriesOutliers]

  override def hashCode: Int = {
    41 * (
      41 + super.hashCode
      ) + outliers.##
  }

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: SeriesOutliers => {
        if ( this eq that ) true
        else {
          super.equals( that ) &&
          ( this.outliers == that.outliers )
        }
      }

      case _ => false
    }
  }


  override def toString: String = {
    s"""${getClass.safeSimpleName}:"${topic}"[source:[${source.size}] outliers[${outliers.size}]:[${outliers.mkString(",")}]]"""
  }
}


case class CohortOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeriesCohort,
  override val plan: OutlierPlan,
  outliers: Set[TimeSeries] = Set()
) extends Outliers {
  private val trace: Trace[_] = Trace[CohortOutliers]

  override type Source = TimeSeriesCohort
  override def size: Int = source.size
  override val topic: Topic = source.topic
  override val hasAnomalies: Boolean = outliers.nonEmpty
  override def anomalySize: Int = outliers.size

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[CohortOutliers]

  override def hashCode: Int = {
    41 * (
      41 + super.hashCode
    ) + outliers.##
  }

  override def equals( rhs: Any ): Boolean = trace.briefBlock("equals") {
    rhs match {
      case that: CohortOutliers => {
        if ( this eq that ) true
        else {
          super.equals( that ) &&
          ( this.outliers == that.outliers )
        }
      }

      case _ => false
    }
  }

  override def toString: String = s"""${getClass.safeSimpleName}:"${topic}"[source:[${source.size}] outliers:[${outliers.mkString(",")}]"""
}
