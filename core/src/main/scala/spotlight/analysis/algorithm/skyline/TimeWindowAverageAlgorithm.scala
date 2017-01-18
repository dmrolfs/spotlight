//package spotlight.analysis.algorithm.skyline
//
//import com.typesafe.config.Config
//import org.apache.commons.lang3.ClassUtils
//import org.apache.commons.math3.ml.clustering.DoublePoint
//import peds.commons.{TryV, Valid}
//import shapeless.Lens
//import spotlight.analysis.{DetectUsing, HistoricalStatistics, RecentHistory}
//import spotlight.analysis.algorithm.AlgorithmModule
//import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
//import spotlight.analysis.algorithm.skyline.TimeWindowAverageModule.State
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
//  case class Context( override val message: DetectUsing ) extends CommonContext( message ) {
//
//  }
//
//  object Context {
//    /**
//      * Identifies whether the evaluated time window is 'static' or 'tumbling' or 'sliding'
//      */
//    val WindowPath = "window"
//
//    val StaticWindowPath = WindowPath + ".static"
//    val StaticWindowStartPath = StaticWindowPath + ".start"
//    val StaticWindowFinishPath = StaticWindowPath + ".finish"
//
//    val TumblingWindowPath = WindowPath + ".tumbling"
//    val tumblingWindowLengthPath = TumblingWindowPath + ".length"
//
//  }
//
//  override def makeContext( message: DetectUsing ): Context = {
//    checkDetectionProperties( message.properties ) map { props =>
//
//    }
//  }
//
//  def checkDetectionProperties( properties: Config ): Valid[Config] = {
//
//  }
//
//  case class State(
//    override val id: TID,
//    override val name: String,
//    override val thresholds: Seq[ThresholdBoundary] = Seq.empty[ThresholdBoundary]
//  ) extends AlgorithmModule.AnalysisState with AlgorithmModule.StrictSelf[State] {
//    override type Self = State
//
//    override def algorithm: Symbol = outer.algorithm.label
//    override def addThreshold( threshold: ThresholdBoundary ): Self = this.copy( thresholds = this.thresholds :+ threshold )
//
//    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
//
//    override def toString: String = {
//      s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
//        s"id:[${id}]; "+
//        s"tolerance:[${tolerance}]; "+
//        s"shape:[${shape}]; "+
//        s"""thresholds:[${thresholds.mkString(",")}]""" +
//        " )"
//    }
//  }
//
//  object State extends AnalysisStateCompanion {
//    override def zero(id: State#TID): State = ???
//
//    override type Shape = this.type
//
//    override def updateShape(
//                                shape: State,
//                                event: Advanced
//                              ): State = ???
//
//    override def shapeLens: Lens[State, State.type] = ???
//
//    override def thresholdLens: Lens[State, Seq[ThresholdBoundary]] = ???
//  }
//
//  override implicit def evState: ClassManifest[TimeWindowAverageModule.type] = ???
//
//  override val analysisStateCompanion: _root_.spotlight.analysis.outlier.algorithm.skyline.TimeWindowAverageModule.AnalysisStateCompanion = _
//}
