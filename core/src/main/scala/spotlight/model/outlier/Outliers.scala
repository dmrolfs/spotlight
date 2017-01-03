package spotlight.model.outlier

import scala.annotation.tailrec
import org.joda.{ time => joda }
import peds.commons.util._
import peds.commons.Valid
import spotlight.model.timeseries._


//todo re-seal with FanOutShape Outlier Detection
abstract class Outliers extends Equals {
  type Source <: TimeSeriesBase
  def topic: Topic
  def algorithms: Set[Symbol]
  def hasAnomalies: Boolean
  def size: Int
  def anomalySize: Int
  def source: Source
  def plan: OutlierPlan
  def thresholdBoundaries: Map[Symbol, Seq[ThresholdBoundary]]

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

  override def toString: String = {
    s"""${getClass.safeSimpleName}:outliers:[${anomalySize}].plan:[${plan.name}].topic:[${topic}].source:[${source.size}].""" +
    s"""interval:[${source.interval getOrElse "No Interval"}]"""
  }
}

object Outliers {
  import scalaz._, Scalaz._

  type OutlierGroups = Map[joda.DateTime, Double]

  def forSeries(
    algorithms: Set[Symbol],
    plan: OutlierPlan,
    source: TimeSeriesBase,
    outliers: Seq[DataPoint],
    thresholdBoundaries: Map[Symbol, Seq[ThresholdBoundary]]
  ): Valid[Outliers] = {
    (
      checkAlgorithms(algorithms, plan)
      |@| checkSeriesSource(source, plan)
      |@| checkOutliers(outliers, source)
      |@| checkThresholdBoundaries( algorithms, thresholdBoundaries )
    ) { (a, s, o, t) =>
      if ( o.isEmpty ) NoOutliers( algorithms = a, source = s, plan = plan, thresholdBoundaries = t )
      else SeriesOutliers( algorithms = a, source = s, plan = plan, outliers = o, thresholdBoundaries = t )
    }
  }

  //todo
  def forCohort( algorithms: Set[Symbol], plan: OutlierPlan, source: TimeSeriesBase, outliers: Seq[TimeSeries] ): Valid[Outliers] = ???

  def unapply( so: Outliers ): Option[(Topic, Set[Symbol], Boolean, so.Source)] = {
    Some( (so.topic, so.algorithms, so.hasAnomalies, so.source) )
  }


  def checkAlgorithms( algorithms: Set[Symbol], plan: OutlierPlan ): Valid[Set[Symbol]] = {
    val notIncluded = algorithms filter { !plan.algorithms.contains(_) }
    if ( notIncluded.isEmpty ) algorithms.successNel
    else Validation.failureNel( PlanAlgorithmsMismatchError( notIncluded, plan ) )
  }

  def checkSeriesSource( source: TimeSeriesBase, plan: OutlierPlan ): Valid[TimeSeries] = {
    if ( !plan.appliesTo( source ) ) Validation.failureNel( PlanSourceMismatchError(source, plan) )
    else {
      source match {
        case series: TimeSeries => series.successNel
        case s => Validation.failureNel( PlanSourceMismatchError(s, plan) )
      }
    }
  }

  def checkOutliers( outliers: Seq[DataPoint], source: TimeSeriesBase ): Valid[Seq[DataPoint]] = {
    val timestamps = source.points.map{ _.timestamp }.toSet
    val notIncluded = outliers filter { o => !timestamps.contains( o.timestamp ) }
    if ( notIncluded.isEmpty ) outliers.successNel
    else Validation.failureNel( SourceOutliersMismatchError( notIncluded, source ) )
  }

  def checkThresholdBoundaries(
    algorithms: Set[Symbol],
    thresholdBoundaries: Map[Symbol, Seq[ThresholdBoundary]]
  ): Valid[Map[Symbol, Seq[ThresholdBoundary]]] = {
    val boundaryAlgorithms = thresholdBoundaries.keySet
    if ( boundaryAlgorithms == boundaryAlgorithms.intersect( algorithms ) ) thresholdBoundaries.successNel
    else Validation.failureNel( ThresholdBoundaryAlgorithmMismatchError( boundaryAlgorithms, algorithms ) )
  }

  final case class PlanAlgorithmsMismatchError private[outlier]( algorithms: Set[Symbol], plan: OutlierPlan )
    extends IllegalArgumentException( s"""cannot create Outliers for algorithms[${algorithms.mkString(",")}] not included in plan [$plan]""" )


  final case class PlanSourceMismatchError private[outlier]( source: TimeSeriesBase, plan: OutlierPlan )
    extends IllegalArgumentException( s"""cannot create Outliers since plan [$plan] does not apply to source[${source.topic}]""" )


  final case class SourceOutliersMismatchError private[outlier]( outliers: Seq[DataPoint], source: TimeSeriesBase )
    extends IllegalArgumentException( s"""cannot create Outliers for outliers[${outliers.mkString(",")}] not included in source [${source}]""" )

  final case class ThresholdBoundaryAlgorithmMismatchError private[outlier](
    boundaryAlgorithms: Set[Symbol],
    algorithms: Set[Symbol]
  ) extends IllegalArgumentException(
    s"""threshold algorithms [${boundaryAlgorithms.mkString(",")}] don't match plan algorithms [${algorithms.mkString(",")}]"""
  )
}

case class NoOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeriesBase,
  override val plan: OutlierPlan,
  override val thresholdBoundaries: Map[Symbol, Seq[ThresholdBoundary]] = Map.empty[Symbol, Seq[ThresholdBoundary]]
) extends Outliers {
  override type Source = TimeSeriesBase
  override def topic: Topic = source.topic
  override def size: Int = source.size
  override val hasAnomalies: Boolean = false
  override val anomalySize: Int = 0

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[NoOutliers]
}

case class SeriesOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeries,
  override val plan: OutlierPlan,
  outliers: Seq[DataPoint],
  override val thresholdBoundaries: Map[Symbol, Seq[ThresholdBoundary]] = Map.empty[Symbol, Seq[ThresholdBoundary]]
) extends Outliers {
  import Outliers._
  override type Source = TimeSeries
  override def topic: Topic = source.topic
  override def size: Int = source.size
  override def hasAnomalies: Boolean = outliers.nonEmpty
  override def anomalySize: Int = outliers.size

  def anomalousGroups: Seq[OutlierGroups] = {
    def nonEmptyAccumulator( acc: List[OutlierGroups] ): List[OutlierGroups] = {
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


  override def toString: String = super.toString + s""".outliers:[${outliers.mkString(",")}]"""
}


case class CohortOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeriesCohort,
  override val plan: OutlierPlan,
  outliers: Set[TimeSeries],
  override val thresholdBoundaries: Map[Symbol, Seq[ThresholdBoundary]] = Map.empty[Symbol, Seq[ThresholdBoundary]]
) extends Outliers {
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

  override def equals( rhs: Any ): Boolean = {
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

  override def toString: String = super.toString + s""".outliers:[${outliers.mkString(",")}]"""
}
