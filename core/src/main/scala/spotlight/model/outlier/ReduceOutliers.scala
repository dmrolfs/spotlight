package spotlight.model.outlier

import scala.concurrent.{ ExecutionContext, Future }
import spotlight.model.timeseries.TimeSeriesBase


trait ReduceOutliers {
  def apply(
    results: OutlierAlgorithmResults,
    source: TimeSeriesBase,
    plan: OutlierPlan
  )(
    implicit ec: ExecutionContext
  ): Future[Outliers]
}
