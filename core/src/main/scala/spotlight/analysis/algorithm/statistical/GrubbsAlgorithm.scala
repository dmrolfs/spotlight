package spotlight.analysis.algorithm.statistical

import com.google.common.collect.EvictingQueue

import scalaz._
import Scalaz._
import com.typesafe.config.Config
import com.persist.logging._
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.DoublePoint
import omnibus.commons.TryV
import spotlight.analysis.algorithm.{ Advancing, Algorithm, CommonContext }
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.DetectUsing
import spotlight.model.timeseries._
import squants.information.{ Bytes, Information }

/** Grubbs' test is used to detect a single outlier in a univariate data set that follows an approximately normal distribution.
  * This implementation applies Grubbs to each point individually considering its prior neighbors.
  *
  * This shape is not immutable as defined.
  * Created by rolfsd on 10/5/16.
  */
case class GrubbsShape( sampleSize: Int = GrubbsShape.DefaultSampleSize ) extends Equals {
  override def canEqual( that: Any ): Boolean = that.isInstanceOf[GrubbsShape]

  import scala.collection.JavaConverters._

  val window: EvictingQueue[Double] = EvictingQueue.create[Double]( sampleSize )
  def N: Int = window.size
  def mean: Double = if ( 0 < N ) window.iterator().asScala.sum / N else Double.NaN
  def standardDeviation: Double = {
    val mu = mean
    val parts = window.iterator().asScala.map { n ⇒ math.pow( ( n - mu ), 2.0 ) }
    if ( 0 < N ) math.sqrt( 1.0 / N * parts.sum ) else Double.NaN
  }

  def minimum: Double = if ( 0 < N ) window.iterator.asScala.min else Double.NaN
  def maximum: Double = if ( 0 < N ) window.iterator.asScala.max else Double.NaN

  override def equals( rhs: Any ): Boolean = {
    def optionalNaN( d: Double ): Option[Double] = if ( d.isNaN ) None else Some( d )

    rhs match {
      case that: GrubbsShape ⇒ {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.sampleSize == that.sampleSize ) &&
            ( this.window == that.window )
        }
      }

      case _ ⇒ false
    }
  }

  override def hashCode: Int = {
    41 * (
      41 + sampleSize.##
    ) + window.##
  }

  override def toString: String = {
    s"${ClassUtils.getAbbreviatedName( getClass, 15 )}(" +
      s"sampleSize:[${sampleSize}] " +
      s"movingStatistics:[N:${N}" +
      ( if ( 0 < N ) " mean:${mean} stddev:${standardDeviation} range:[${minimum} - ${maximum}]] " else "" ) +
      "])"
  }
}

object GrubbsShape {
  val SampleSizePath = "sample-size"
  val DefaultSampleSize: Int = 60

  implicit val advancing = new Advancing[GrubbsShape] {
    override def zero( configuration: Option[Config] ): GrubbsShape = {
      val sampleSize = valueFrom( configuration, SampleSizePath ) { _.getInt( SampleSizePath ) } getOrElse DefaultSampleSize
      GrubbsShape( sampleSize )
    }

    override def advance( original: GrubbsShape, advanced: Advanced ): GrubbsShape = {
      original.window.add( advanced.point.value )
      original
      //      val newStats = original.movingStatistics.copy()
      //      newStats addValue advanced.point.value
      //      original.copy( movingStatistics = newStats )
    }
  }
}

object GrubbsAlgorithm extends Algorithm[GrubbsShape]( label = "grubbs" ) { algorithm ⇒
  val minimumDataPoints: Int = 7

  override def prepareData( c: Context ): Seq[DoublePoint] = c.tailAverage()( c.data )

  override def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[( Boolean, ThresholdBoundary )] = {
    val result = grubbsScore( shape ) map { grubbs ⇒
      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.timestamp.toLong,
        expected = shape.mean,
        distance = c.tolerance * grubbs * shape.standardDeviation
      )

      ( threshold isOutlier point.value, threshold )
    }

    result match {
      case \/-( r ) ⇒ Some( r )

      case -\/( ex: Algorithm.InsufficientDataSize ) ⇒ {
        import spotlight.model.timeseries._
        log.info( Map( "@msg" → "skipping point until sufficient history is establish", "point" → point ), ex )
        Some( ( false, ThresholdBoundary empty point.timestamp.toLong ) )
      }

      case -\/( ex ) ⇒ {
        log.warn( Map( "@msg" → "exception raised in algorithm step calculation", "algorithm" → label ), ex )
        throw ex
      }
    }
  }

  def grubbsScore( shape: Shape )( implicit c: Context ): TryV[Double] = {
    for {
      size ← checkSize( shape.N )
      critical ← criticalValue( shape )
    } yield {
      val mean = shape.mean
      val sd = shape.standardDeviation
      val thresholdSquared = math.pow( critical, 2 )
      ( ( size - 1 ) / math.sqrt( size ) ) * math.sqrt( thresholdSquared / ( size - 2 + thresholdSquared ) )
    }
  }

  def criticalValue( shape: Shape )( implicit c: Context ): TryV[Double] = {
    def calculateCritical( size: Long ): TryV[Double] = {
      \/ fromTryCatchNonFatal {
        val degreesOfFreedom = math.max( size - 2, 1 ) //todo: not a great idea but for now avoiding error if size <= 2
        new TDistribution( degreesOfFreedom ).inverseCumulativeProbability( c.alpha / ( 2.0 * size ) )
      }
    }

    for {
      size ← checkSize( shape.N )
      critical ← calculateCritical( size )
    } yield critical
  }

  /** The test should not be used for sample sizes of six or fewer since it frequently tags most of the points as outliers.
    * @param size
    * @return
    */
  private def checkSize( size: Long ): TryV[Long] = {
    size match {
      case s if s < minimumDataPoints ⇒ Algorithm.InsufficientDataSize( label, s, minimumDataPoints ).left
      case s ⇒ s.right
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
    TryV unsafeGet { Context.getAlpha( message.properties ) map { alpha ⇒ Context( message, alpha ) } }
  }

  /** Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    *
    * @return blended average size for the algorithm shape
    */
  override val estimatedAverageShapeSize: Option[Information] = Some( Bytes( 1194 ) )
}
