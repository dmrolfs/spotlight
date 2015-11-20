package lineup.model.outlier

import scala.concurrent.{ ExecutionContext, Future }
import lineup.model.timeseries.TimeSeriesBase


trait ReduceOutliers {
  def apply( results: SeriesOutlierResults, source: TimeSeriesBase )( implicit ec: ExecutionContext ): Future[Outliers]
}
