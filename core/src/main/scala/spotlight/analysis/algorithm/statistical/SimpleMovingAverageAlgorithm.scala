package spotlight.analysis.algorithm.statistical

import com.persist.logging._
import com.typesafe.config.Config
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.stat.descriptive.{ StatisticalSummary, SummaryStatistics }
import squants.information.{ Bytes, Information }
import omnibus.commons.util._
import spotlight.analysis.{ AnomalyScore, DetectUsing }
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.algorithm.{ Advancing, Algorithm, AlgorithmShapeCompanion, CommonContext }
import spotlight.model.statistics
import spotlight.model.statistics.MovingStatistics
import spotlight.model.timeseries._

case class SimpleMovingAverageShape( underlying: StatisticalSummary ) extends Equals {
  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[SimpleMovingAverageShape]

  def :+( value: Double ): SimpleMovingAverageShape = {
    underlying match {
      case u: MovingStatistics ⇒ copy( underlying = u :+ value )

      case s: SummaryStatistics ⇒ {
        val newUnderlying = s.copy()
        newUnderlying addValue value
        copy( underlying = newUnderlying )
      }
    }
  }

  def N: Long = underlying.getN
  def mean: Double = underlying.getMean
  def standardDeviation: Double = underlying.getStandardDeviation
  def minimum: Double = underlying.getMin
  def maximum: Double = underlying.getMax

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
  override def toString: String = {
    s"${ClassUtils.getAbbreviatedName( getClass, 15 )}( " +
      s"underlying-type:${underlying.getClass.safeSimpleName} " +
      s"N:${N} mean:${mean} stdev:${standardDeviation} range:[${minimum} : ${maximum}] " +
      s"): underlying:[${underlying}]"
  }
}

object SimpleMovingAverageShape extends AlgorithmShapeCompanion {
  def apply( width: Int ): SimpleMovingAverageShape = {
    val u = width match {
      case Int.MaxValue ⇒ new SummaryStatistics()
      case w ⇒ statistics.MovingStatistics( w )
    }

    SimpleMovingAverageShape( underlying = u )
  }

  val SlidingWindowPath = "sliding-window"
  val DefaultSlidingWindow = Int.MaxValue
  def slidingWindowFrom( configuration: Option[Config] ): Int = {
    valueFrom( configuration, SlidingWindowPath, DefaultSlidingWindow ) { _ getInt _ }
  }

  implicit val advancing = new Advancing[SimpleMovingAverageShape] {
    override def zero( configuration: Option[Config] ): SimpleMovingAverageShape = {
      val window = slidingWindowFrom( configuration )
      log.debug( Map( "@msg" → "creating zero shape", "window" → window, "is-inf" → ( Int.MaxValue == window ) ) )
      SimpleMovingAverageShape( window )
    }

    override def N( shape: SimpleMovingAverageShape ): Long = shape.N

    override def advance( original: SimpleMovingAverageShape, advanced: Advanced ): SimpleMovingAverageShape = {
      original :+ advanced.point.value
    }

    override def copy( shape: SimpleMovingAverageShape ): SimpleMovingAverageShape = {
      val newUnderlying = shape.underlying match {
        case s: SummaryStatistics ⇒ s.copy()
        case s: MovingStatistics ⇒ s.copy()
      }
      shape.copy( underlying = newUnderlying )
    }
  }
}

/** Created by rolfsd on 6/8/16.
  */
object SimpleMovingAverageAlgorithm extends Algorithm[SimpleMovingAverageShape]( label = "simple-moving-average" ) { algorithm ⇒
  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )

  override def score( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[AnomalyScore] = {
    val mean = shape.mean
    val stddev = shape.standardDeviation
    val threshold = ThresholdBoundary.fromExpectedAndDistance(
      timestamp = point.timestamp.toLong,
      expected = mean,
      distance = c.tolerance * stddev
    )

    def property( path: String ): String = if ( c.properties hasPath path ) c.properties getString path else "-nil-"

    //    log.debug(
    //      Map(
    //        "@msg" → "Step",
    //        "config" → Map( "tail" → property( "tail-average" ), "tolerance" → property( "tolerance" ), "minimum-population" → property( "minimum-population" ) ),
    //        "stats" → Map( "mean" → f"${mean}%2.5f", "standard-deviation" → f"${stddev}%2.5f", "distance" → f"${c.tolerance * stddev}%2.5f" ),
    //        "point" → Map( "timestamp" → point.dateTime.toString, "value" → f"${point.value}%2.5f" ),
    //        "threshold" → Map( "1_floor" → threshold.floor.toString, "2_expected" → threshold.expected.toString, "3_ceiling" → threshold.ceiling.toString ),
    //        "is-anomaly" → threshold.isOutlier( point.value )
    //      )
    //    )

    Some( AnomalyScore( threshold isOutlier point.value, threshold ) )
  }

  /** Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    * @return blended average size for the algorithm shape
    */
  override def estimatedAverageShapeSize( properties: Option[Config] ): Option[Information] = {
    SimpleMovingAverageShape.slidingWindowFrom( properties ) match {
      case SimpleMovingAverageShape.DefaultSlidingWindow ⇒ Some( Bytes( 2230 ) )
      case window if window <= 32 ⇒ Some( Bytes( 716 + ( window * 13 ) ) ) // identified through observation
      case window if window <= 42 ⇒ Some( Bytes( 798 + ( window * 13 ) ) )
      case window if window <= 73 ⇒ Some( Bytes( 839 + ( window * 13 ) ) )
      case window if window <= 105 ⇒ Some( Bytes( 880 + ( window * 13 ) ) )
      case _ ⇒ None
    }
  }
}
