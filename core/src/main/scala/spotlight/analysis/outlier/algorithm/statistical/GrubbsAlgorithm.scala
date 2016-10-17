package spotlight.analysis.outlier.algorithm.statistical

import com.typesafe.config.Config
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.TryV
import peds.commons.log.Trace
import shapeless.{Lens, lens}
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.outlier.{DetectUsing, RecentHistory}
import spotlight.model.timeseries._

import scala.reflect.ClassTag
import scalaz.syntax.either._
import scalaz.{-\/, \/, \/-}


/**
  * Grubbs' test is used to detect a single outlier in a univariate data set that follows an approximately normal distribution.
  * This implementation applies Grubbs to each point individually considering its prior neighbors.
  *
  * Created by rolfsd on 10/5/16.
  */
object GrubbsAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  private val trace = Trace[GrubbsAlgorithm.type]

  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = 'Grubbs

    override def prepareData( algorithmContext: Context ): Seq[DoublePoint] = {
      algorithmContext.tailAverage()( algorithmContext.data )
    }

    override def step( point: PointT )( implicit state: State, algorithmContext: Context ): (Boolean, ThresholdBoundary) = trace.block( s"step($point)" ) {
      // original version set expected at context stats mean and stddev
      val result = {
        logger.debug( "TEST: state=[{}]", state )
        Option( state )
        .map { s =>
          s
          .grubbsScore
          .map { grubbs =>
            val threshold = ThresholdBoundary.fromExpectedAndDistance(
              timestamp = point.timestamp.toLong,
              expected = state.movingStatistics.getMean,
              distance = algorithmContext.tolerance * grubbs * state.movingStatistics.getStandardDeviation
            )

            ( threshold isOutlier point.value, threshold )
          }
        }
      }

      result match {
        case Some( \/-(r) ) => r

        case None => {
          import spotlight.model.timeseries._
          logger.debug( s"skipping point[{}] until history is established", point)
          ( false, ThresholdBoundary empty point.timestamp.toLong )
        }

        case Some( -\/( ex: AlgorithmModule.InsufficientDataSize ) ) => {
          import spotlight.model.timeseries._
          logger.debug( "skipping point[{}]: {}", point, ex.getMessage )
          ( false, ThresholdBoundary empty point.timestamp.toLong )
        }

        case Some( -\/(ex) ) => {
          logger.warn( "issue in step calculation", ex )
          throw ex
        }
      }
    }
  }


  case class Context( override val message: DetectUsing, alpha: Double ) extends CommonContext( message )

  object GrubbsContext {
    val AlphaPath = "alpha"
//    val SampleSizePath = "sample-size"
//    def getSamplesize( conf: Config ): TryV[Int] = {
//      logger.debug( "configuration[{}] getLong= [{}]", SampleSizePath, conf.getLong(SampleSizePath).toString )
//      val sampleSize = if ( conf hasPath SampleSizePath ) conf getInt SampleSizePath else RecentHistory.LastN
//      sampleSize.right
//    }

    def getAlpha( conf: Config ): TryV[Double] = {
      logger.debug( "configuration[{}] getDouble = [{}]", AlphaPath, conf.getDouble(AlphaPath).toString )
      val alpha = if ( conf hasPath AlphaPath ) conf getDouble AlphaPath else 0.05
      alpha.right
    }
  }

  override def makeContext( message: DetectUsing, state: Option[State] ): Context = {
    val context = GrubbsContext.getAlpha( message.properties ) map { alpha => Context( message, alpha ) }
    context match {
      case \/-( c ) => c
      case -\/( ex ) => throw ex
    }
  }


  override type Shape = DescriptiveStatistics

  case class State(
    override val id: TID,
    override val name: String,
    movingStatistics: DescriptiveStatistics //,
//    override val thresholds: Seq[ThresholdBoundary] = Seq.empty[ThresholdBoundary]
  ) extends AlgorithmModule.AnalysisState with AlgorithmModule.StrictSelf[State] {
    override type Self = State

    override def algorithm: Symbol = outer.algorithm.label
//    override def addThreshold( threshold: ThresholdBoundary ): State = this.copy( thresholds = this.thresholds :+ threshold )

    def grubbsScore( implicit context: Context ): TryV[Double] = trace.block( "grubbsScore" ) {
      for {
        size <- checkSize( movingStatistics.getN )
        critical <- criticalValue( context.alpha )
      } yield {
        val mean = movingStatistics.getMean
        val sd = movingStatistics.getStandardDeviation
        logger.debug( "Grubbs movingStatistics: N:[{}] mean:[{}] stddev:[{}]", size.toString, mean.toString, sd.toString )

        val thresholdSquared = math.pow( critical, 2 )
        logger.debug( "Grubbs threshold^2:[{}]", thresholdSquared )
        ((size - 1) / math.sqrt(size)) * math.sqrt( thresholdSquared / (size - 2 + thresholdSquared) )
      }
    }

    def criticalValue( alpha: Double ): TryV[Double] = trace.briefBlock( s"criticalValue($alpha)" ) {
      def calculateCritical( size: Long ): TryV[Double] = {
        \/ fromTryCatchNonFatal {
          val degreesOfFreedom = math.max( size - 2, 1 ) //todo: not a great idea but for now avoiding error if size <= 2
          new TDistribution( degreesOfFreedom ).inverseCumulativeProbability( alpha / (2.0 * size) )
        }
      }

      for {
        size <- checkSize( movingStatistics.getN )
        critical <- calculateCritical( size )
      } yield critical
    }


    /**
      * The test should not be used for sample sizes of six or fewer since it frequently tags most of the points as outliers.
      * @param size
      * @return
      */
    private def checkSize( size: Long ): TryV[Long] = {
      val MinimumDataPoints = 7
      size match {
        case s if s < MinimumDataPoints => AlgorithmModule.InsufficientDataSize( algorithm, s, MinimumDataPoints ).left
        case s => s.right
      }
    }

    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
    override def toString: String = {
      s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
        s"id:[${id}]; "+
        s"movingStatistics:[N:${movingStatistics.getN} mean:${movingStatistics.getMean} stddev:${movingStatistics.getStandardDeviation}]; " +
//        s"""thresholds:[${thresholds.mkString(",")}]""" +
        " )"
    }
  }

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]

    override def zero( id: State#TID ): State = {
      State( id, name = "", movingStatistics = makeShape() )
    }

    def makeShape(): Shape = new DescriptiveStatistics( RecentHistory.LastN ) //todo: move sample size to repository config?

    override def advanceShape( statistics: Shape, advanced: Advanced ): Shape = trace.block( "State.advanceShape" ) {
      logger.debug( "TEST: statistics shape = [{}]", statistics )
      logger.debug( "TEST: advanced event  = [{}]", advanced )
      val newStats = statistics.copy()
      newStats addValue advanced.point.value
      newStats
    }

    override def shapeLens: Lens[State, Shape] = lens[State] >> 'movingStatistics
//    override def thresholdLens: Lens[State, Seq[ThresholdBoundary]] = lens[State] >> 'thresholds
  }

  override val analysisStateCompanion: AnalysisStateCompanion = State
  override implicit val evState: ClassTag[State] = ClassTag( classOf[State] )
}
