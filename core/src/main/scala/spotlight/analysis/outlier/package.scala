package spotlight.analysis

import akka.actor.ActorRef

import scalaz._
import Scalaz._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance}
import peds.commons.Valid
import peds.commons.math.MahalanobisDistance
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase, TimeSeriesCohort, Topic}


/**
 * Created by rolfsd on 10/4/15.
 */
package object outlier {
  sealed trait OutlierDetectionMessage {
    def topic: Topic
    type Source <: TimeSeriesBase
    def source: Source
    def plan: OutlierPlan
  }

  object OutlierDetectionMessage {
    def apply( ts: TimeSeriesBase, plan: OutlierPlan ): Valid[OutlierDetectionMessage] = {
      checkPlan(plan, ts) map { p =>
        ts match {
          case s: TimeSeries => DetectOutliersInSeries( s, p )
          case c: TimeSeriesCohort => DetectOutliersInCohort( c, p )
        }
      }
    }

    def unapply( m: OutlierDetectionMessage ): Option[(OutlierPlan, Topic, m.Source)] = Some( (m.plan, m.topic, m.source) )

    def checkPlan( plan: OutlierPlan, ts: TimeSeriesBase ): Valid[OutlierPlan] = {
      if ( plan appliesTo ts ) plan.successNel else Validation.failureNel( PlanMismatchError( plan, ts ) )
    }
  }


  /**
    * Type class that determines circumstance when a distance measure is valid to use.
 *
    * @tparam D
    */
  trait DistanceMeasureValidity[D <: DistanceMeasure] {
    def isApplicable(distance: D, history: HistoricalStatistics ): Boolean
  }

  /**
    * Mahalanobis distance should not be applied when the historical covariance matrix has a determinant of 0.0
    */
  implicit val mahalanobisValidity = new DistanceMeasureValidity[MahalanobisDistance] {
    override def isApplicable(distance: MahalanobisDistance, history: HistoricalStatistics ): Boolean = {
      import org.apache.commons.math3.linear.EigenDecomposition
      val determinant = new EigenDecomposition( history.covariance ).getDeterminant
      determinant != 0.0
    }
  }

  /**
    * Euclidean distance can always be applied.
    */
  implicit val euclideanValidity = new DistanceMeasureValidity[EuclideanDistance] {
    override def isApplicable(distance: EuclideanDistance, history: HistoricalStatistics ): Boolean = true
  }


  case class HistoryKey( plan: OutlierPlan, topic: Topic ) {
    override val toString: String = s"${plan.name}:${topic}"
  }

  case class MomentBinKey( dayOfWeek: DayOfWeek, hourOfDay: Int ) {
    def id: String = s"${dayOfWeek.label}:${hourOfDay}"
  }

  import org.joda.{ time => joda }

  sealed trait DayOfWeek {
    def label: String
    def jodaKey: Int
  }

  object DayOfWeek {
    def fromJodaKey( key: Int ): Valid[DayOfWeek] = Validation.fromTryCatchNonFatal{ JodaDays(key) }.toValidationNel

    val JodaDays: Map[Int, DayOfWeek] = Map(
      Seq( Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday ).map{ d => d.jodaKey -> d }:_*
    )
  }

  case object Sunday extends DayOfWeek {
    override val label: String = "Sunday"
    override val jodaKey: Int = joda.DateTimeConstants.SUNDAY
  }
  case object Monday extends DayOfWeek {
    override val label: String = "Monday"
    override val jodaKey: Int = joda.DateTimeConstants.MONDAY
  }
  case object Tuesday extends DayOfWeek {
    override val label: String = "Tuesday"
    override val jodaKey: Int = joda.DateTimeConstants.TUESDAY
  }
  case object Wednesday extends DayOfWeek {
    override val label: String = "Wednesday"
    override val jodaKey: Int = joda.DateTimeConstants.WEDNESDAY
  }
  case object Thursday extends DayOfWeek {
    override val label: String = "Thursday"
    override val jodaKey: Int = joda.DateTimeConstants.THURSDAY
  }
  case object Friday extends DayOfWeek {
    override val label: String = "Friday"
    override val jodaKey: Int = joda.DateTimeConstants.FRIDAY
  }
  case object Saturday extends DayOfWeek {
    override val label: String = "Saturday"
    override val jodaKey: Int = joda.DateTimeConstants.SATURDAY
  }


  final case class PlanMismatchError private[outlier]( plan: OutlierPlan, timeseries: TimeSeriesBase )
  extends IllegalStateException( s"plan [${plan.name}:${plan.id}] improperly associated with time series [${timeseries.topic}]" )


  final case class DetectOutliersInSeries private[outlier](
    override val source: TimeSeries,
    override val plan: OutlierPlan
  ) extends OutlierDetectionMessage {
    override def topic: Topic = source.topic
    override type Source = TimeSeries
  }

  final case class DetectOutliersInCohort private[outlier](
    override val source: TimeSeriesCohort,
    override val plan: OutlierPlan
  ) extends OutlierDetectionMessage {
    override def topic: Topic = source.topic
    override type Source = TimeSeriesCohort
  }


  final case class DetectUsing private[outlier](
    algorithm: Symbol,
    aggregator: ActorRef,
    payload: OutlierDetectionMessage,
    history: HistoricalStatistics,
    properties: Config = ConfigFactory.empty()
  ) extends OutlierDetectionMessage {
    override def topic: Topic = payload.topic
    override type Source = payload.Source
    override def source: Source = payload.source
    override val plan: OutlierPlan = payload.plan
  }


  final case class UnrecognizedPayload private[outlier](
    algorithm: Symbol,
    request: DetectUsing
  ) extends OutlierDetectionMessage {
    override def topic: Topic = request.topic
    override type Source = request.Source
    override def source: Source = request.source
    override def plan: OutlierPlan = request.plan
  }
}
