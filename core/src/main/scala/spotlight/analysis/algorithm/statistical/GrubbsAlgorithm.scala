package spotlight.analysis.algorithm.statistical

import scalaz._
import Scalaz._
import com.typesafe.config.Config
import com.persist.logging._
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.distribution.TDistribution
import omnibus.commons.TryV
import spotlight.analysis.algorithm.{ Advancing, Algorithm, CommonContext }
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.DetectUsing
import spotlight.model.statistics.MovingStatistics
import spotlight.model.timeseries._
import squants.information.{ Bytes, Information }

/** Grubbs' test is used to detect a single outlier in a univariate data set that follows an approximately normal distribution.
  * This implementation applies Grubbs to each point individually considering its prior neighbors.
  *
  * This shape is not immutable as defined.
  * Created by rolfsd on 10/5/16.
  */
case class GrubbsShape( underlying: MovingStatistics ) extends Equals {
  override def canEqual( that: Any ): Boolean = that.isInstanceOf[GrubbsShape]

  def :+( value: Double ): GrubbsShape = copy( underlying = underlying :+ value )
  def width: Int = underlying.width
  def N: Long = underlying.N
  def mean: Double = underlying.mean
  def standardDeviation: Double = underlying.standardDeviation
  def minimum: Double = underlying.minimum
  def maximum: Double = underlying.maximum

  override def equals( rhs: Any ): Boolean = {
    def optionalNaN( d: Double ): Option[Double] = if ( d.isNaN ) None else Some( d )

    rhs match {
      case that: GrubbsShape ⇒ {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.underlying == that.underlying )
        }
      }

      case _ ⇒ false
    }
  }

  override def hashCode: Int = { 41 + underlying.## }
  override def toString: String = s"${ClassUtils.getAbbreviatedName( getClass, 15 )}( ${underlying} )"
}

object GrubbsShape extends ClassLogging {
  def apply( width: Int = DefaultSlidingWindow ): GrubbsShape = GrubbsShape( MovingStatistics( width ) )

  val SlidingWindowPath = "sliding-window"
  val DefaultSlidingWindow: Int = 60

  implicit val advancing = new Advancing[GrubbsShape] {
    override def zero( configuration: Option[Config] ): GrubbsShape = {
      val width = valueFrom( configuration, SlidingWindowPath ) { c =>
        val sliding = c getInt SlidingWindowPath
        if ( 0 < sliding ) sliding else DefaultSlidingWindow
      } getOrElse {
        DefaultSlidingWindow
      }
      GrubbsShape( width )
    }

    override def N( shape: GrubbsShape ): Long = shape.N
    override def advance( original: GrubbsShape, advanced: Advanced ): GrubbsShape = { original :+ advanced.point.value }
    override def copy( shape: GrubbsShape ): GrubbsShape = shape.copy( underlying = shape.underlying.copy() )
  }
}

object GrubbsAlgorithm extends Algorithm[GrubbsShape]( label = "grubbs" ) { algorithm ⇒
  import Algorithm.ConfigurationProvider.MinimalPopulationPath

  def minimumDataPoints( implicit c: Context ): Int = {
    val grubbsRequirement = 7

    if ( c.properties hasPath MinimalPopulationPath ) {
      math.max( grubbsRequirement, c.properties getInt MinimalPopulationPath )
    } else {
      grubbsRequirement
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
  private def checkSize( size: Long )( implicit c: Context ): TryV[Long] = {
    size match {
      case s if s < minimumDataPoints ⇒ Algorithm.InsufficientDataSize( label, s, minimumDataPoints ).left
      case s ⇒ s.right
    }
  }

  /** Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    *
    * @return blended average size for the algorithm shape
    */
  override val estimatedAverageShapeSize: Option[Information] = Some( Bytes( 1318 ) )
}
