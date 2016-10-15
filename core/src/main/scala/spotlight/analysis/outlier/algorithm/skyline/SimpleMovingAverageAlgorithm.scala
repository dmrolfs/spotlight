package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import scalaz.syntax.either._
import shapeless.{Lens, lens}
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.lang3.ClassUtils
import peds.commons.TryV
import peds.commons.log.Trace
import spotlight.analysis.outlier.DetectUsing
import spotlight.analysis.outlier.algorithm.{AlgorithmModule, AlgorithmProtocol}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/8/16.
  */
object SimpleMovingAverageAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = Symbol( "SimpleMovingAverage" )

    override def prepareData( algorithmContext: Context ): TryV[Seq[DoublePoint]] = {
      algorithmContext.tailAverage()( algorithmContext.data ).right
    }

    override def step( point: PointT )( implicit state: State, algorithmContext: Context ): (Boolean, ThresholdBoundary) = {
      logger.debug( "TEST:step( {} ): state=[{}]", point, state )
      val moving = state.history.movingStatistics
      val mean = moving.getMean
      val stddev = moving.getStandardDeviation
      logger.debug(
        "Stddev from simple moving Average N[{}]: mean[{}]\tstdev[{}]\ttolerance[{}]",
        moving.getN.toString, mean.toString, stddev.toString, algorithmContext.tolerance.toString
      )

      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.timestamp.toLong,
        expected = mean,
        distance = algorithmContext.tolerance * stddev
      )

      ( threshold isOutlier point.value, threshold )
    }
  }


  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): TryV[Context] = new CommonContext( message ).right


  case class State(
    override val id: TID,
    override val name: String,
    history: State.Shape = State.makeShape(),
    override val thresholds: Seq[ThresholdBoundary] = Seq.empty[ThresholdBoundary]
  ) extends AlgorithmModule.AnalysisState with AlgorithmModule.StrictSelf[State] {
    override type Self = State

    override def algorithm: Symbol = outer.algorithm.label

    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
    override def addThreshold( threshold: ThresholdBoundary ): Self = copy( thresholds = thresholds :+ threshold )
    override def toString: String = {
      s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
        s"id:[${id}]; "+
        s"shape:[${history}]; "+
        s"""thresholds:[${thresholds.mkString(",")}]""" +
      " )"
    }
  }

  override val analysisStateCompanion: AnalysisStateCompanion = State
  override implicit def evState: ClassTag[State] = ClassTag( classOf[State] )

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]
    override def zero( id: State#TID ): State = State( id = id, name = "" )

    case class Shape( movingStatistics: DescriptiveStatistics ) extends Equals {
      override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[Shape]

      override def equals( rhs: Any ): Boolean = {
        rhs match {
          case that: Shape => {
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

    def makeShape(): Shape = Shape( new DescriptiveStatistics( AlgorithmModule.ApproximateDayWindow ) )

    object Shape {
      def statsLens: Lens[Shape, DescriptiveStatistics] = lens[Shape] >> 'movingStatistics
    }

    override def updateShape( h: Shape, event: AlgorithmProtocol.Advanced ): Shape = trace.block( "updateShape" ) {
      logger.debug( "TEST: history shape = [{}]", h )
      logger.debug( "TEST: advanced event  = [{}]", event )
      Shape.statsLens.modify( h ){ stats =>
        val ms = stats.copy()
        ms addValue event.point.value
        logger.debug( "TEST:" )
        ms
      }
    }

    override def shapeLens: Lens[State, Shape] = lens[State] >> 'history
    override def thresholdLens: Lens[State, Seq[ThresholdBoundary]] = lens[State] >> 'thresholds
  }
}
