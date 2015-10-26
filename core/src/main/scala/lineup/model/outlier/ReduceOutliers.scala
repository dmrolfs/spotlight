package lineup.model.outlier

import lineup.model.timeseries.TimeSeriesBase


trait ReduceOutliers {
  def apply( results: SeriesOutlierResults, source: TimeSeriesBase ): Outliers
}
