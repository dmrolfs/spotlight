package spotlight.model.outlier

import peds.commons.V
import shapeless.syntax.typeable._
import spotlight.model.timeseries.{DataPoint, TimeSeriesBase}


trait ReduceOutliers {
  def apply(
    results: OutlierAlgorithmResults,
    source: TimeSeriesBase,
    plan: OutlierPlan
  ): V[Outliers]
}

object ReduceOutliers {
  val seriesIntersection = new ReduceOutliers {
    override def apply(
      results: OutlierAlgorithmResults,
      source: TimeSeriesBase,
      plan: OutlierPlan
    ): V[Outliers] = {
      val (outliers, control) = {
        results
        .values
        .map{ case o =>
          val cb = o.algorithmControlBoundaries
          val so = o.cast[SeriesOutliers].map{ _.outliers } getOrElse { Seq.empty[DataPoint] }
          ( so, cb )
        }
        .reduceLeft { (aoc, oc) =>
          val (accOutliers, accControl) = aoc
          val (o, c) = oc
          val io = accOutliers.filter{ o.contains }
          val ic = accControl ++ c
          (io, ic)
        }
      }

      Outliers.forSeries(
        algorithms = results.keySet,
        plan = plan,
        source = source,
        outliers = outliers,
        algorithmControlBoundaries = control
      )
      .disjunction
    }
  }

}