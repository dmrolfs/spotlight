package spotlight.analysis.outlier.algorithm.statistical

import scala.reflect.ClassTag
import scalaz._
import Scalaz._
import com.typesafe.config.Config
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.TryV
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.outlier.{DetectUsing, RecentHistory}
import spotlight.model.timeseries._
import spotlight.analysis.outlier.algorithm.AlgorithmModule.ShapeCompanion


/**
  * Grubbs' test is used to detect a single outlier in a univariate data set that follows an approximately normal distribution.
  * This implementation applies Grubbs to each point individually considering its prior neighbors.
  *
  * Created by rolfsd on 10/5/16.
  */
case class GrubbsShape( movingStatistics: DescriptiveStatistics, sampleSize: Int = RecentHistory.LastN ) extends Equals {
  override def canEqual( that: Any ): Boolean = that.isInstanceOf[GrubbsShape]

  override def equals( rhs: Any ): Boolean = {
    def optionalNaN( d: Double ): Option[Double] = if ( d.isNaN ) None else Some( d )

    rhs match {
      case that: GrubbsShape => {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
          ( that canEqual this ) &&
          ( this.movingStatistics.getN == that.movingStatistics.getN ) &&
          ( optionalNaN(this.movingStatistics.getMean) == optionalNaN(that.movingStatistics.getMean) ) &&
          ( optionalNaN(this.movingStatistics.getStandardDeviation) == optionalNaN(that.movingStatistics.getStandardDeviation) ) &&
          ( this.sampleSize == that.sampleSize )
        }
      }

      case _ => false
    }
  }

  override def hashCode: Int = {
    41 * (
      41 * (
        41 * (
          41 + sampleSize.##
        ) + movingStatistics.getN.##
      )  + movingStatistics.getMean.##
    ) + movingStatistics.getStandardDeviation.##
  }

  override def toString: String = {
    s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
    s"sampleSize:[${sampleSize}] " +
    s"movingStatistics:[N:${movingStatistics.getN} m:${movingStatistics.getMean} sd:${movingStatistics.getStandardDeviation} " +
    s"range:[${movingStatistics.getMin} - ${movingStatistics.getMax}]] " +
    ")"
  }
}

object GrubbsShape extends ShapeCompanion[GrubbsShape] {
  val SampleSizePath = "sample-size"

  override def zero( configuration: Option[Config] ): GrubbsShape = {
    val sampleSize = valueFrom( configuration, SampleSizePath ){ _.getInt( SampleSizePath ) } getOrElse RecentHistory.LastN
    GrubbsShape( new DescriptiveStatistics(sampleSize), sampleSize )
  }

  override def advance( original: GrubbsShape, advanced: Advanced ): GrubbsShape = {
    val newStats = original.movingStatistics.copy()
    newStats addValue advanced.point.value
    original.copy( movingStatistics = newStats )
  }
}


object GrubbsAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  /**
    * Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  override type Shape = GrubbsShape
  override val evShape: ClassTag[Shape] = ClassTag( classOf[GrubbsShape] )
  override val shapeCompanion: ShapeCompanion[Shape] = GrubbsShape

  override def algorithm: Algorithm = new Algorithm {
    override val label: Symbol = 'grubbs
    val minimumDataPoints: Int = 7

    override def prepareData( c: Context ): Seq[DoublePoint] = c.tailAverage()( c.data )

    override def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)] = {
      val result = grubbsScore( shape ) map { grubbs =>
        val threshold = ThresholdBoundary.fromExpectedAndDistance(
          timestamp = point.timestamp.toLong,
          expected = shape.movingStatistics.getMean,
          distance = c.tolerance * grubbs * shape.movingStatistics.getStandardDeviation
        )

        ( threshold isOutlier point.value, threshold )
      }

      result match {
        case \/-( r ) => Some( r )

        case -\/( ex: AlgorithmModule.InsufficientDataSize ) => {
          import spotlight.model.timeseries._
          logger.debug( "skipping point[{}] until sufficient history is established: {}", point, ex.getMessage )
          Some( (false, ThresholdBoundary empty point.timestamp.toLong) )
        }

        case -\/( ex ) => {
          logger.warn( s"exception raised in ${algorithm.label} step calculation", ex )
          throw ex
        }
      }
    }

    def grubbsScore( shape: Shape )( implicit s: State, c: Context ): TryV[Double] = {
      for {
        size <- checkSize( shape.movingStatistics.getN )
        critical <- criticalValue( shape )
      } yield {
        val mean = shape.movingStatistics.getMean
        val sd = shape.movingStatistics.getStandardDeviation
        val thresholdSquared = math.pow( critical, 2 )
        ((size - 1) / math.sqrt(size)) * math.sqrt( thresholdSquared / (size - 2 + thresholdSquared) )
      }
    }

    def criticalValue( shape: Shape)( implicit c: Context ): TryV[Double] = {
      def calculateCritical( size: Long ): TryV[Double] = {
        \/ fromTryCatchNonFatal {
          val degreesOfFreedom = math.max( size - 2, 1 ) //todo: not a great idea but for now avoiding error if size <= 2
          new TDistribution( degreesOfFreedom ).inverseCumulativeProbability( c.alpha / (2.0 * size) )
        }
      }

      for {
        size <- checkSize( shape.movingStatistics.getN )
        critical <- calculateCritical( size )
      } yield critical
    }

    /**
      * The test should not be used for sample sizes of six or fewer since it frequently tags most of the points as outliers.
      * @param size
      * @return
      */
    private def checkSize( size: Long ): TryV[Long] = {
      size match {
        case s if s < minimumDataPoints => AlgorithmModule.InsufficientDataSize( label, s, minimumDataPoints ).left
        case s => s.right
      }
    }
  }

  case class Context( override val message: DetectUsing, alpha: Double ) extends CommonContext( message )

  object Context {
    val AlphaPath = "alpha"
    def getAlpha( c: Config ): TryV[Double] = {
      val alpha = if ( c hasPath AlphaPath ) c getDouble AlphaPath else 0.05
      alpha.right
    }
  }

  override def makeContext( message: DetectUsing, state: Option[State] ): Context = {
    val context = Context.getAlpha( message.properties ) map { alpha => Context( message, alpha ) }
    context match {
      case \/-( c ) => c
      case -\/( ex ) => throw ex
    }
  }
}
