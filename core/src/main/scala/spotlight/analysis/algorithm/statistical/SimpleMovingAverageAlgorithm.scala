package spotlight.analysis.algorithm.statistical

import com.persist.logging._
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import spotlight.analysis.DetectUsing
import spotlight.analysis.algorithm.{ Algorithm, CommonContext }
import spotlight.model.timeseries._
import squants.information.{ Bytes, Information }

/** Created by rolfsd on 6/8/16.
  */
object SimpleMovingAverageAlgorithm extends Algorithm[SummaryStatistics]( label = "simple-moving-average" ) { algorithm ⇒
  override def prepareData( c: Context ): Seq[DoublePoint] = { c.tailAverage()( c.data ) }

  override def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[( Boolean, ThresholdBoundary )] = {
    val mean = shape.getMean
    val stddev = shape.getStandardDeviation
    val threshold = ThresholdBoundary.fromExpectedAndDistance(
      timestamp = point.timestamp.toLong,
      expected = mean,
      distance = c.tolerance * stddev
    )

    Some( ( threshold isOutlier point.value, threshold ) )
  }

  override type Context = CommonContext

  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )

  /** Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    * @return blended average size for the algorithm shape
    */
  override def estimatedAverageShapeSize: Option[Information] = Some( Bytes( 345 ) )
}
