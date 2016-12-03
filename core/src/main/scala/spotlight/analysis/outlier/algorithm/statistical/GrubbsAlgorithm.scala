package spotlight.analysis.outlier.algorithm.statistical

import scala.reflect.ClassTag
import scalaz._, Scalaz._
import com.typesafe.config.Config
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.{TryV, Valid}
import peds.commons.log.Trace
import shapeless.{Lens, lens}
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.outlier.{DetectUsing, RecentHistory}
import spotlight.model.timeseries._
import spotlight.analysis.outlier.algorithm.AlgorithmModule.RedundantAlgorithmConfiguration


/**
  * Grubbs' test is used to detect a single outlier in a univariate data set that follows an approximately normal distribution.
  * This implementation applies Grubbs to each point individually considering its prior neighbors.
  *
  * Created by rolfsd on 10/5/16.
  */
object GrubbsAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  private val trace = Trace[GrubbsAlgorithm.type]

  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = 'grubbs

    override def prepareData( algorithmContext: Context ): Seq[DoublePoint] = {
      algorithmContext.tailAverage()( algorithmContext.data )
    }

    override def step( point: PointT )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)] = {
      val result = s.grubbsScore map { grubbs =>
        val threshold = ThresholdBoundary.fromExpectedAndDistance(
          timestamp = point.timestamp.toLong,
          expected = s.movingStatistics.getMean,
          distance = c.tolerance * grubbs * s.movingStatistics.getStandardDeviation
        )

        ( threshold isOutlier point.value, threshold )
      }

      result match {
        case \/-(r) => Some( r )

        case -\/( ex: AlgorithmModule.InsufficientDataSize ) => {
          import spotlight.model.timeseries._
          logger.debug( "skipping point[{}] until sufficient history is established: {}", point, ex.getMessage )
          Some( (false, ThresholdBoundary empty point.timestamp.toLong) )
        }

        case -\/(ex) => {
          logger.warn( s"exception raised in ${algorithm.label} step calculation", ex )
          throw ex
        }
      }
    }
  }


  case class Context( override val message: DetectUsing, alpha: Double ) extends CommonContext( message )

  object GrubbsContext {
    val AlphaPath = "alpha"

    def getAlpha( conf: Config ): TryV[Double] = {
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
    movingStatistics: DescriptiveStatistics,
    sampleSize: Int = RecentHistory.LastN
  ) extends AlgorithmModule.AnalysisState with AlgorithmModule.StrictSelf[State] {
    override type Self = State

    override def algorithm: Symbol = outer.algorithm.label

    def grubbsScore( implicit context: Context ): TryV[Double] = {
      for {
        size <- checkSize( movingStatistics.getN )
        critical <- criticalValue( context.alpha )
      } yield {
        val mean = movingStatistics.getMean
        val sd = movingStatistics.getStandardDeviation
        val thresholdSquared = math.pow( critical, 2 )
        ((size - 1) / math.sqrt(size)) * math.sqrt( thresholdSquared / (size - 2 + thresholdSquared) )
      }
    }

    def criticalValue( alpha: Double ): TryV[Double] = {
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
      import State.MinimumDataPoints
      size match {
        case s if s < MinimumDataPoints => AlgorithmModule.InsufficientDataSize( algorithm, s, MinimumDataPoints ).left
        case s => s.right
      }
    }

    override def withConfiguration( configuration: Config ): Valid[State] = {
      State.getSampleSize( configuration ) match {
        case scalaz.Success( newSampleSize ) if newSampleSize != sampleSize => {
          val newStats = new DescriptiveStatistics( newSampleSize )
          DescriptiveStatistics.copy( this.movingStatistics, newStats )
          copy( sampleSize = newSampleSize, movingStatistics = newStats ).successNel
        }

        case scalaz.Success( duplicateSampleSize ) => {
          logger.debug( "ignoring duplicate configuration: sample-size:[{}]", sampleSize )
          Validation.failureNel( RedundantAlgorithmConfiguration(id, path = State.SampleSizePath, value = duplicateSampleSize) )
        }

        case scalaz.Failure( exs ) => {
          exs foreach { ex =>
            logger.error(
              s"ignoring configuration provided without properties relevant to ${algorithm.name} algorithm: [${configuration}]",
              ex
            )
          }

          exs.failure
        }
      }
    }

    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]

    override def equals( rhs: Any ): Boolean = {
      def optionalNaN( n: Double ): Option[Double] = if ( n.isNaN ) None else Some( n )

      rhs match {
        case that: State => {
          super.equals( that ) &&
          ( this.movingStatistics.getN == that.movingStatistics.getN ) &&
          ( optionalNaN(this.movingStatistics.getMean) == optionalNaN(that.movingStatistics.getMean) ) &&
          ( optionalNaN(this.movingStatistics.getStandardDeviation) == optionalNaN(that.movingStatistics.getStandardDeviation) ) &&
          ( this.sampleSize == that.sampleSize )
        }

        case _ => false
      }
    }

    override def hashCode: Int = {
      41 * (
        41 * (
          41 * (
            41 * (
              41 + super.hashCode
            ) + movingStatistics.getN.##
          )  + movingStatistics.getMean.##
        ) + movingStatistics.getStandardDeviation.##
      ) + sampleSize.##
    }

    override def toString: String = {
      s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
      s"id:[${id}] "+
      s"sampleSize:[${sampleSize}] " +
      s"movingStatistics:[N:${movingStatistics.getN} m:${movingStatistics.getMean} sd:${movingStatistics.getStandardDeviation} " +
      s"range:[${movingStatistics.getMin} - ${movingStatistics.getMax}]] " +
      ")"
    }
  }

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]

    val MinimumDataPoints = 7

    override def zero( id: State#TID ): State = {
      State( id, name = "", movingStatistics = makeShape() )
    }

    def makeShape(): Shape = new DescriptiveStatistics( RecentHistory.LastN ) //todo: move sample size to repository config?

    override def advanceShape( statistics: Shape, advanced: Advanced ): Shape = {
      val newStats = statistics.copy()
      newStats addValue advanced.point.value
      newStats
    }

    override def shapeLens: Lens[State, Shape] = lens[State] >> 'movingStatistics

    val RootPath = algorithm.label.name
    val SampleSizePath = RootPath + ".sample-size"
    def getSampleSize( conf: Config ): Valid[Int] = {
      val sampleSize = if ( conf hasPath SampleSizePath ) conf getInt SampleSizePath else RecentHistory.LastN
      if ( MinimumDataPoints <= sampleSize ) sampleSize.successNel
      else {
        Validation.failureNel(
          AlgorithmModule.InvalidAlgorithmConfiguration(
            algorithm.label,
            SampleSizePath,
            s"integer greater than or equals to ${MinimumDataPoints}"
          )
        )
      }
    }
  }

  override val analysisStateCompanion: AnalysisStateCompanion = State
  override implicit val evState: ClassTag[State] = ClassTag( classOf[State] )
}
