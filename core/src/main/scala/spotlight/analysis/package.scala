package spotlight

import scala.concurrent.duration._
import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.config.Config
import org.apache.commons.math3.ml.distance.{ DistanceMeasure, EuclideanDistance }
import peds.commons.math.MahalanobisDistance
import spotlight.model.outlier.{ AnalysisPlan, Outliers }
import spotlight.model.timeseries.{ TimeSeries, TimeSeriesBase }

/** Created by rolfsd on 10/4/15.
  */
package object analysis {
  val BaseMetricName: String = "analysis"

  type DetectFlow = Flow[TimeSeries, Outliers, NotUsed]

  type TimeSeriesScope = ( TimeSeries, AnalysisPlan.Scope )

  def durationFrom( config: Config, path: String ): Option[Duration] = {
    if ( config hasPath path ) {
      if ( config.getString( path ).trim.compareToIgnoreCase( "Inf" ) == 0 ) Some( Duration.Inf )
      else Some( FiniteDuration( config.getDuration( path, NANOSECONDS ), NANOSECONDS ).toCoarsest )
    } else {
      None
    }
  }

  /** Type class that determines circumstance when a distance measure is valid to use.
    *
    * @tparam D
    */
  trait DistanceMeasureValidity[D <: DistanceMeasure] {
    def isApplicable( distance: D, history: HistoricalStatistics ): Boolean
  }

  /** Mahalanobis distance should not be applied when the historical covariance matrix has a determinant of 0.0
    */
  implicit val mahalanobisValidity = new DistanceMeasureValidity[MahalanobisDistance] {
    override def isApplicable( distance: MahalanobisDistance, history: HistoricalStatistics ): Boolean = {
      import org.apache.commons.math3.linear.EigenDecomposition
      val determinant = new EigenDecomposition( history.covariance ).getDeterminant
      determinant != 0.0
    }
  }

  /** Euclidean distance can always be applied.
    */
  implicit val euclideanValidity = new DistanceMeasureValidity[EuclideanDistance] {
    override def isApplicable( distance: EuclideanDistance, history: HistoricalStatistics ): Boolean = true
  }

  final case class PlanMismatchError private[analysis] ( plan: AnalysisPlan, timeseries: TimeSeriesBase )
    extends IllegalStateException( s"plan [${plan.name}:${plan.id}] improperly associated with time series [${timeseries.topic}]" )
}
