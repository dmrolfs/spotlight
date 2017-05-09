package spotlight.analysis

import spotlight.model.timeseries.ThresholdBoundary

/** Created by rolfsd on 3/10/17.
  */
case class AnomalyScore( isOutlier: Boolean, threshold: ThresholdBoundary )
