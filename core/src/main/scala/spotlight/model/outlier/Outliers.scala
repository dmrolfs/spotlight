package spotlight.model.outlier

import scala.annotation.tailrec
import org.joda.{ time => joda }
import peds.commons.log.Trace
import peds.commons.util._
import peds.commons.Valid
import spotlight.model.timeseries._


//todo re-seal with FanOutShape Outlier Detection
abstract class Outliers extends Equals {
  private val trace = Trace[Outliers]
  type Source <: TimeSeriesBase
  def topic: Topic
  def algorithms: Set[Symbol]
  def hasAnomalies: Boolean
  def size: Int
  def anomalySize: Int
  def source: Source
  def plan: OutlierPlan
  def algorithmControlBoundaries: Map[Symbol, Seq[ControlBoundary]]

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
    s"""${getClass.safeSimpleName}:[${plan.name}][${topic}][source:[${source.size}] interval:[${source.interval getOrElse "No Interval"}]"""
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
    algorithmControlBoundaries: Map[Symbol, Seq[ControlBoundary]]
  ): Valid[Outliers] = {
    (
      checkAlgorithms(algorithms, plan)
      |@| checkSeriesSource(source, plan)
      |@| checkOutliers(outliers, source)
      |@| checkControlBoundaries( algorithms, algorithmControlBoundaries )
    ) { (a, s, o, c) =>
      if ( o.isEmpty ) NoOutliers( algorithms = a, source = s, plan = plan, algorithmControlBoundaries = c )
      else SeriesOutliers( algorithms = a, source = s, plan = plan, outliers = o, algorithmControlBoundaries = c )
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

  def checkControlBoundaries(
    algorithms: Set[Symbol],
    algorithmControlBoundaries: Map[Symbol, Seq[ControlBoundary]]
  ): Valid[Map[Symbol, Seq[ControlBoundary]]] = {
    val boundaryAlgorithms = algorithmControlBoundaries.keySet
    if ( boundaryAlgorithms == boundaryAlgorithms.intersect( algorithms ) ) algorithmControlBoundaries.successNel
    else Validation.failureNel( ControlBoundaryAlgorithmMismatchError(boundaryAlgorithms, algorithms) )
  }

  final case class PlanAlgorithmsMismatchError private[outlier]( algorithms: Set[Symbol], plan: OutlierPlan )
    extends IllegalArgumentException( s"""cannot create Outliers for algorithms[${algorithms.mkString(",")}] not included in plan [$plan]""" )


  final case class PlanSourceMismatchError private[outlier]( source: TimeSeriesBase, plan: OutlierPlan )
    extends IllegalArgumentException( s"""cannot create Outliers since plan [$plan] does not apply to source[${source.topic}]""" )


  final case class SourceOutliersMismatchError private[outlier]( outliers: Seq[DataPoint], source: TimeSeriesBase )
    extends IllegalArgumentException( s"""cannot create Outliers for outliers[${outliers.mkString(",")}] not included in source [${source}]""" )

  final case class ControlBoundaryAlgorithmMismatchError private[outlier](
    boundaryAlgorithms: Set[Symbol],
    algorithms: Set[Symbol]
  ) extends IllegalArgumentException(
    s"""control algorithms [${boundaryAlgorithms.mkString(",")}] don't match plan algorithms [${algorithms.mkString(",")}]"""
  )
}

case class NoOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeriesBase,
  override val plan: OutlierPlan,
  override val algorithmControlBoundaries: Map[Symbol, Seq[ControlBoundary]] = Map.empty[Symbol, Seq[ControlBoundary]]
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
  override val algorithmControlBoundaries: Map[Symbol, Seq[ControlBoundary]] = Map.empty[Symbol, Seq[ControlBoundary]]
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


  override def toString: String = super.toString + s"""[outliers[${outliers.size}]:[${outliers.mkString(",")}]]"""
}


case class CohortOutliers(
  override val algorithms: Set[Symbol],
  override val source: TimeSeriesCohort,
  override val plan: OutlierPlan,
  outliers: Set[TimeSeries],
  override val algorithmControlBoundaries: Map[Symbol, Seq[ControlBoundary]] = Map.empty[Symbol, Seq[ControlBoundary]]
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

  override def toString: String = super.toString + s"""[outliers:[${outliers.mkString(",")}]"""
}
