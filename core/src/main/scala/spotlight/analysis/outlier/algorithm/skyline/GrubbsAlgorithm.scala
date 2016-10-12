package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import scalaz.{-\/, Validation, \/, \/-}
import scalaz.syntax.either._
import scalaz.syntax.validation._
import shapeless.{Lens, lens}
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.distribution.TDistribution
import peds.commons.log.Trace
import peds.commons.{TryV, Valid}
import spotlight.analysis.outlier.{DetectUsing, RecentHistory}
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 10/5/16.
  */
object GrubbsAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = 'Grubbs

    override def prepareData( algorithmContext: Context ): TryV[Seq[DoublePoint]] = {
      algorithmContext.tailAverage()( algorithmContext.data ).right
    }

    override def step( point: PointT )( implicit state: State, algorithmContext: Context ): (Boolean, ThresholdBoundary) = {
      // original version set expected at context stats mean and stddev
      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.timestamp.toLong,
        expected = state.movingStatistics.getMean,
        distance = algorithmContext.tolerance * algorithmContext.grubbsScore * state.movingStatistics.getStandardDeviation
      )

      ( threshold isOutlier point.value, threshold )
    }

  }


  case class Context( override val message: DetectUsing, grubbsScore: Double ) extends CommonContext( message )

  object GrubbsContext {
    val AlphaPath = "alpha"
    def checkAlpha( conf: Config ): Valid[Double] = {
      val alpha = if ( conf hasPath AlphaPath ) conf getDouble AlphaPath else 0.05
      alpha.successNel
    }

    def criticalValueFor( alpha: Double, size: Long ): Valid[Double] = {
      Validation
      .fromTryCatchNonFatal {
        val degreesOfFreedom = math.max( size - 2, 1 ) //todo: not a great idea but for now avoiding error if size <= 2
        new TDistribution( degreesOfFreedom ).inverseCumulativeProbability( alpha / (2.0 * size) )
      }
      .toValidationNel
    }

    def grubbsScore( critical: Double, statistics: DescriptiveStatistics ): Double = {
      val size = statistics.getN
      val mean = statistics.getMean
      val stddev = statistics.getStandardDeviation
      logger.debug( "Grubbs movingStatistics: N:[{}] mean:[{}] stddev:[{}]", size.toString, mean.toString, stddev.toString )

      val thresholdSquared = math.pow( critical, 2 )
      logger.debug( "Grubbs threshold^2:[{}]", thresholdSquared )
      ((size - 1) / math.sqrt(size)) * math.sqrt( thresholdSquared / (size - 2 + thresholdSquared) )
    }
  }

  override def makeContext( message: DetectUsing, state: Option[State] ): TryV[Context] = {
    val statistics = state map { _.movingStatistics } getOrElse State.makeStatistics()
    val context = {
      for {
        alpha <- GrubbsContext.checkAlpha( message.properties ).disjunction
        critical <- GrubbsContext.criticalValueFor( alpha, statistics.getN ).disjunction
      } yield {
        Context( message, grubbsScore = GrubbsContext.grubbsScore(critical, statistics) )
      }
    }

    context.leftMap { exs =>
      exs foreach { ex => logger.error( "failed to make Grubbs algorithm context", ex ) }
      exs.head
    }
  }


  case class State(
    override val id: TID,
    override val name: String,
    movingStatistics: DescriptiveStatistics,
    override val thresholds: Seq[ThresholdBoundary] = Seq.empty[ThresholdBoundary]
  ) extends AlgorithmModule.AnalysisState with AlgorithmModule.StrictSelf[State] {
    override type Self = State

    override def algorithm: Symbol = outer.algorithm.label
    override def addThreshold( threshold: ThresholdBoundary ): State = this.copy( thresholds = this.thresholds :+ threshold )

    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
    override def toString: String = {
      s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
        s"id:[${id}]; "+
        s"movingStatistics:[mean:${movingStatistics.getMean} stddev:${movingStatistics.getStandardDeviation}]; " +
        s"""thresholds:[${thresholds.mkString(",")}]""" +
        " )"
    }
  }

  override val analysisStateCompanion: AnalysisStateCompanion = State
  override implicit def evState: ClassTag[State] = ClassTag( classOf[State] )

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]

    override type Shape = DescriptiveStatistics

    override def zero( id: State#TID ): State = State( id, name = "", movingStatistics = makeStatistics() )

    def makeStatistics(): DescriptiveStatistics = new DescriptiveStatistics( RecentHistory.LastN )

    override def updateShape( statistics: Shape, event: Advanced ): Shape = trace.block( "State.updateShape" ) {
      logger.debug( "TEST: statistics shape = [{}]", statistics )
      logger.debug( "TEST: advanced event  = [{}]", event )
      val newStats = statistics.copy()
      newStats addValue event.point.value
      newStats
    }

    override def shapeLens: Lens[State, Shape] = lens[State] >> 'movingStatistics
    override def thresholdLens: Lens[State, Seq[ThresholdBoundary]] = lens[State] >> 'thresholds
  }
}
