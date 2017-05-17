package spotlight.analysis.algorithm.statistical

import scala.math
import cats.syntax.either._

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import com.persist.logging._
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.distribution.TDistribution
import omnibus.commons._
import spotlight.analysis.algorithm.{ Advancing, Algorithm, CommonContext, InsufficientDataSize }
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.{ AnomalyScore, DetectUsing }
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
    override def zero( configuration: Option[Config] ): GrubbsShape = GrubbsShape( slidingWindowFrom( configuration ) )
    override def N( shape: GrubbsShape ): Long = shape.N
    override def advance( original: GrubbsShape, advanced: Advanced ): GrubbsShape = { original :+ advanced.point.value }
    override def copy( shape: GrubbsShape ): GrubbsShape = shape.copy( underlying = shape.underlying.copy() )
  }

  def slidingWindowFrom( configuration: Option[Config] ): Int = {
    val sliding = for {
      c ← configuration
      s ← c.as[Option[Int]]( SlidingWindowPath ) if 0 < s
    } yield s

    sliding getOrElse DefaultSlidingWindow
  }
}

object GrubbsAlgorithm extends Algorithm[GrubbsShape]( label = "grubbs" ) { algorithm ⇒
  import Algorithm.ConfigurationProvider.MinimalPopulationPath

  def minimumDataPoints( implicit c: Context ): Int = {
    val grubbsRequirement = 7

    c.properties
      .as[Option[Int]]( MinimalPopulationPath )
      .map { mpp ⇒ math.max( grubbsRequirement, mpp ) }
      .getOrElse { grubbsRequirement }
  }

  case class Context( override val message: DetectUsing, alpha: Double ) extends CommonContext( message )

  object Context {
    val AlphaPath = "alpha"
    def getAlpha( c: Config ): ErrorOr[Double] = {
      val alpha = c.as[Option[Double]]( AlphaPath ) getOrElse 0.05
      alpha.asRight
    }
  }

  override def makeContext( message: DetectUsing, state: Option[State] ): Context = {
    Context
      .getAlpha( message.properties )
      .map { alpha ⇒ Context( message, alpha ) }
      .unsafeGet
  }

  override def score( point: PointT, shape: Shape )( implicit c: Context ): Option[AnomalyScore] = {
    val result = grubbsScore( shape ) map { grubbs ⇒
      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.timestamp.toLong,
        expected = shape.mean,
        distance = c.tolerance * grubbs * shape.standardDeviation
      )

      AnomalyScore( threshold isOutlier point.value, threshold )
    }

    result match {
      case Right( r ) ⇒ Some( r )

      case Left( ex: InsufficientDataSize ) ⇒ {
        import spotlight.model.timeseries._
        log.info( Map( "@msg" → "skipping point until sufficient history is establish", "point" → point ), ex )
        Some( AnomalyScore( isOutlier = false, threshold = ThresholdBoundary empty point.timestamp.toLong ) )
      }

      case Left( ex ) ⇒ {
        log.warn( Map( "@msg" → "exception raised in algorithm step calculation", "algorithm" → label ), ex )
        throw ex
      }
    }
  }

  def grubbsScore( shape: Shape )( implicit c: Context ): ErrorOr[Double] = {
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

  def criticalValue( shape: Shape )( implicit c: Context ): ErrorOr[Double] = {
    def calculateCritical( size: Long ): ErrorOr[Double] = {
      Either catchNonFatal {
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
  private def checkSize( size: Long )( implicit c: Context ): ErrorOr[Long] = {
    size match {
      case s if s < minimumDataPoints ⇒ InsufficientDataSize( label, s, minimumDataPoints ).asLeft
      case s ⇒ s.asRight
    }
  }

  /** Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    *
    * @return blended average size for the algorithm shape
    */
  override def estimatedAverageShapeSize( properties: Option[Config] ): Option[Information] = {
    // these numbers were taken from SMA -- actuals differ
    GrubbsShape.slidingWindowFrom( properties ) match {
      case GrubbsShape.DefaultSlidingWindow ⇒ Some( Bytes( 1589.0 ) )
      case window if window <= 32 ⇒ Some( Bytes( 716 + ( window * 13 ) ) ) // identified through observation
      case window if window <= 42 ⇒ Some( Bytes( 798 + ( window * 13 ) ) )
      case window if window <= 73 ⇒ Some( Bytes( 839 + ( window * 13 ) ) )
      case window if window <= 105 ⇒ Some( Bytes( 880 + ( window * 13 ) ) )
      case _ ⇒ None
    }

  }

}
