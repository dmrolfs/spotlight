//package spotlight.analysis.outlier.algorithm.skyline
//
//import org.apache.commons.math3.ml.clustering.DoublePoint
//import peds.commons.TryV
//import spotlight.analysis.outlier.{DetectUsing, HistoricalStatistics}
//import spotlight.analysis.outlier.algorithm.AlgorithmModule
//import spotlight.model.timeseries.{PointT, ThresholdBoundary}
//
///**
//  * Created by rolfsd on 10/3/16.
//  */
//object TimeWindowAverageModule extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
//  override lazy val algorithm: Algorithm = new Algorithm {
//    override val label: Symbol = Symbol( "TimeWindowAverage" )
//    override def prepareData( algorithmContext: Context ): TryV[Seq[DoublePoint]] = {
//      algorithmContext.tailAverage()( algorithmContext.data ).right
//    }
//
//    override def step( point: PointT )( implicit state: State, algorithmContext: Context ): (Boolean, ThresholdBoundary) = ???
//  }
//
//  override type Context = this.type
//  case class Context( ) extends AlgorithmContext {
//    override def message: DetectUsing = ???
//
//    override def history: HistoricalStatistics = ???
//
//    override def data: Seq[DoublePoint] = ???
//  }
//
//  override def makeContext(message: DetectUsing): TimeWindowAverageModule = ???
//
//  override type State = this.type
//
//  override implicit def evState: ClassManifest[TimeWindowAverageModule.type] = ???
//
//  override val analysisStateCompanion: _root_.spotlight.analysis.outlier.algorithm.skyline.TimeWindowAverageModule.AnalysisStateCompanion = _
//}
