package spotlight.analysis.outlier.algorithm.statistical

import org.apache.commons.math3.ml.clustering.DoublePoint
import spotlight.analysis.outlier.DetectUsing
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.model.timeseries.{PointT, ThresholdBoundary}


/**
  * Created by rolfsd on 11/12/16.
  */
object ExponentialMovingAverageAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  import AlgorithmModule.{ AnalysisState, StrictSelf }

  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = 'EWMA

    override def prepareData( algorithmContext: Context ): Seq[DoublePoint] = algorithmContext.data

    override def step( point: PointT )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)] = ???
  }


  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )


  case class State(
                  override val id: TID, WORK HERE
                  ) extends AnalysisState with StrictSelf[State]
}
