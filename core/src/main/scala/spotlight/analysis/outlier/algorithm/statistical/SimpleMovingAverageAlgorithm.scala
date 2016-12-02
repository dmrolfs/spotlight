package spotlight.analysis.outlier.algorithm.statistical

import scala.reflect.ClassTag
import scalaz.syntax.validation._
import shapeless.{Lens, lens}
import com.typesafe.config.Config
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import peds.commons.Valid
import peds.commons.log.Trace
import spotlight.analysis.outlier.DetectUsing
import spotlight.analysis.outlier.algorithm.{AlgorithmModule, AlgorithmProtocol}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/8/16.
  */
object SimpleMovingAverageAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = Symbol( "simple-moving-average" )

    override def prepareData( algorithmContext: Context ): Seq[DoublePoint] = {
      algorithmContext.tailAverage()( algorithmContext.data )
    }

    override def step( point: PointT )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)] = {
      val moving = s.statistics
      val mean = moving.getMean
      val stddev = moving.getStandardDeviation
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


  case class State(
    override val id: TID,
    override val name: String,
    statistics: Shape = makeShape() //,
  ) extends AlgorithmModule.AnalysisState with AlgorithmModule.StrictSelf[State] {
    override type Self = State

    override def algorithm: Symbol = outer.algorithm.label
    override def withConfiguration( configuration: Config ): Valid[State] = this.successNel

    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
    override def toString: String = {
      s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
      s"id:[${id}] " +
      s"statistics:[N=${statistics.getN} m=${statistics.getMean} sd=${statistics.getStandardDeviation} " +
      s"range:[${statistics.getMin} - ${statistics.getMax}}]]; "+
      ")"
    }
  }

  override val analysisStateCompanion: AnalysisStateCompanion = State
  override implicit def evState: ClassTag[State] = ClassTag( classOf[State] )

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]
    override def zero( id: State#TID ): State = State( id = id, name = "" )


    override def advanceShape( statistics: Shape, advanced: AlgorithmProtocol.Advanced ): Shape = trace.block( "advanceShape" ) {
      val newStats = statistics.copy()
      newStats addValue advanced.point.value
      newStats
    }

    override def shapeLens: Lens[State, Shape] = lens[State] >> 'statistics
  }


  override type Shape = SummaryStatistics
  def makeShape(): Shape = new SummaryStatistics()
}
