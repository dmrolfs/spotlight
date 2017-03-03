package spotlight.analysis.algorithm.statistical

import com.persist.logging._
import spotlight.analysis.{ DetectUsing, Moment }
import spotlight.analysis.algorithm.{ Algorithm, CommonContext }
import spotlight.model.timeseries._
import squants.information.{ Bytes, Information }

/** Created by rolfsd on 11/12/16.
  */
object ExponentialMovingAverageAlgorithm extends Algorithm[Moment]( label = "ewma" ) { algorithm ⇒
  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )

  override def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[( Boolean, ThresholdBoundary )] = {
    shape.statistics map { stats ⇒
      log.debug(
        Map(
          "@msg" → "stepping",
          "point" → ( point.timestamp.toLong, point.value ).toString,
          "mean" → stats.ewma,
          "stdev" → stats.ewmsd,
          "tolerance" → c.tolerance
        )
      )

      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.dateTime,
        expected = stats.ewma,
        distance = math.abs( c.tolerance * stats.ewmsd )
      )

      ( threshold isOutlier point.value, threshold )
    }
  }

  /** Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    * @return blended average size for the algorithm shape
    */
  override val estimatedAverageShapeSize: Option[Information] = Some( Bytes( 354 ) )
}
