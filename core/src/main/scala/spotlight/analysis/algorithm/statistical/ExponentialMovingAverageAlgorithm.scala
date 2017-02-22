package spotlight.analysis.algorithm.statistical

import com.persist.logging._
import spotlight.analysis.{ DetectUsing, Moment }
import spotlight.analysis.algorithm.{ Algorithm, CommonContext }
import spotlight.model.timeseries._

/** Created by rolfsd on 11/12/16.
  */
object ExponentialMovingAverageAlgorithm extends Algorithm[Moment]( label = "ewma" ) { algorithm ⇒
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

  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )
}
