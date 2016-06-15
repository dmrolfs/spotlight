package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import scalaz.syntax.either._
import shapeless.{Lens, lens}
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.TryV
import demesne.InitializeAggregateRootClusterSharding
import org.apache.commons.lang3.ClassUtils
import spotlight.analysis.outlier.{DetectUsing, HistoricalStatistics}
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/8/16.
  */
object SimpleMovingAverageModule
extends AlgorithmModule
with AlgorithmModule.ModuleProvider
with InitializeAggregateRootClusterSharding {
  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = Symbol( "simple-moving-average" )

    override def prepareData( algorithmContext: Context ): TryV[Seq[DoublePoint]] = {
      algorithmContext.tailAverage()( algorithmContext.data ).right
    }

    override def step( point: PointT )( implicit state: State, algorithmContext: Context ): (Boolean, ThresholdBoundary) = {
      val moving = state.history.movingStatistics
      val mean = moving.getMean
      val stddev = moving.getStandardDeviation
      logger.debug(
        "Stddev from simple moving Average N[{}]: mean[{}]\tstdev[{}]\ttolerance[{}]",
        moving.getN.toString, mean.toString, stddev.toString, state.tolerance.toString
      )

      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.timestamp.toLong,
        expected = mean,
        distance = state.tolerance * stddev
      )

      ( threshold isOutlier point.value, threshold )
    }
  }

  case class Context(
    override val message: DetectUsing
  ) extends AlgorithmContext {
    override def history: HistoricalStatistics = message.history
    override def data: Seq[DoublePoint] = message.payload.source.points
  }

  override def makeContext( message: DetectUsing ): Context = Context( message )

  case class State(
    override val id: TID,
    override val name: String,
    history: State.History = State.History.empty,
    override val tolerance: Double = 3.0,
    override val thresholds: Seq[ThresholdBoundary] = Seq.empty[ThresholdBoundary]
  ) extends AnalysisState {
    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
    override def addThreshold( threshold: ThresholdBoundary ): State = copy( thresholds = thresholds :+ threshold )
    override def toString: String = {
      s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
        s"id:[${id}]; "+
        s"tolerance:[${tolerance}]; "+
        s"history:[${history}]; "+
        s"""thresholds:[${thresholds.mkString(",")}]""" +
      " )"
    }
  }

  override val analysisStateCompanion: AnalysisStateCompanion = State
  override implicit def evState: ClassTag[State] = ClassTag( classOf[State] )

  object State extends AnalysisStateCompanion {
    override def zero( id: State#TID ): State = State( id = id, name = "" )

    case class History( movingStatistics: DescriptiveStatistics ) extends Equals {
      override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[History]

      override def equals( rhs: Any ): Boolean = {
        rhs match {
          case that: History => {
            if ( this eq that ) true
            else {
              ( that.## == this.## ) &&
              ( that canEqual this ) &&
              ( this.movingStatistics.getN == that.movingStatistics.getN ) &&
              ( this.movingStatistics.getMean == that.movingStatistics.getMean ) &&
              ( this.movingStatistics.getStandardDeviation == that.movingStatistics.getStandardDeviation )
            }
          }

          case _ => false
        }
      }

      override def hashCode(): Int = {
        41 * (
          41 * (
            41 + movingStatistics.getN.##
          ) + movingStatistics.getMean.##
        ) + movingStatistics.getStandardDeviation.##
      }
    }

    object History {
      def statsLens: Lens[History, DescriptiveStatistics] = lens[History] >> 'movingStatistics
      def empty: History = History( new DescriptiveStatistics( AlgorithmModule.ApproximateDayWindow ) )
    }

    override def updateHistory( h: History, event: AnalysisState.Advanced ): History = {
      History.statsLens.modify( h ){ stats =>
        val ms = stats.copy
        ms addValue event.point.value
        ms
      }
    }

    override def historyLens: Lens[State, History] = lens[State] >> 'history
    override def thresholdLens: Lens[State, Seq[ThresholdBoundary]] = lens[State] >> 'thresholds
  }
}
