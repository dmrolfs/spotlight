package spotlight.analysis.outlier.algorithm.statistical

import scala.reflect.ClassTag
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import spotlight.analysis.outlier.DetectUsing
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmModule.ShapeCompanion
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/8/16.
  */
object SimpleMovingAverageAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  /**
    * Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  override type Shape = SummaryStatistics
  object Shape extends ShapeCompanion[Shape] {
    override def zero( configuration: Option[Config] ): Shape = new SummaryStatistics()
    override def advance( original: Shape, advanced: Advanced ): Shape = {
      val result = original.copy()
      result addValue advanced.point.value
      result
    }
  }

  override val evShape: ClassTag[Shape] = ClassTag( classOf[SummaryStatistics] )
  override val shapeCompanion: ShapeCompanion[Shape] = Shape

  override def algorithm: Algorithm = new Algorithm {
    override val label: Symbol = Symbol( "simple-moving-average" )

    override def prepareData( c: Context ): Seq[DoublePoint] = { c.tailAverage()( c.data ) }

    override def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)] = {
      val mean = shape.getMean
      val stddev = shape.getStandardDeviation
      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.timestamp.toLong,
        expected = mean,
        distance = c.tolerance * stddev
      )

      Some( (threshold isOutlier point.value, threshold) )
    }
  }

  override type Context = CommonContext

  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )
}
