package spotlight.analysis.algorithm.statistical

import com.typesafe.config.Config

import scala.reflect.ClassTag
import scalaz.{ -\/, \/- }
import spotlight.analysis.{ DetectUsing, Moment }
import spotlight.analysis.algorithm.AlgorithmModule
import spotlight.analysis.algorithm.AlgorithmModule.ShapeCompanion
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
import spotlight.model.timeseries._

/** Created by rolfsd on 11/12/16.
  */
object ExponentialMovingAverageAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer ⇒
  /** Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  override type Shape = Moment
  object Shape extends ShapeCompanion[Shape] {
    val AlphaPath = "alpha"

    override def zero( configuration: Option[Config] ): Shape = {
      val alpha = valueFrom( configuration, AlphaPath ) { _ getDouble AlphaPath } getOrElse 0.05
      Moment.withAlpha( alpha ).disjunction match {
        case \/-( m ) ⇒ m
        case -\/( exs ) ⇒ {
          exs foreach { ex ⇒ logger.error( "failed to create moment shape", ex ) }
          throw exs.head
        }
      }
    }

    override def advance( original: Shape, advanced: Advanced ): Shape = original :+ advanced.point.value
  }

  override val evShape: ClassTag[Shape] = ClassTag( classOf[Moment] )
  override val shapeCompanion: ShapeCompanion[Shape] = Shape

  override def algorithm: Algorithm = new Algorithm {
    override val label: String = "ewma"
    override def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[( Boolean, ThresholdBoundary )] = {
      shape.statistics map { stats ⇒
        logger.debug(
          "pt:[{}] - Stddev from exponential moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]",
          ( point.timestamp.toLong, point.value ),
          stats.ewma.toString,
          stats.ewmsd.toString,
          c.tolerance.toString
        )

        val threshold = ThresholdBoundary.fromExpectedAndDistance(
          timestamp = point.dateTime,
          expected = stats.ewma,
          distance = math.abs( c.tolerance * stats.ewmsd )
        )

        ( threshold isOutlier point.value, threshold )
      }
    }
  }

  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )
}
