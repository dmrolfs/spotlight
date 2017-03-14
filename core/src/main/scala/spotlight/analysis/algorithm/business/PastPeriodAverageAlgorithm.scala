package spotlight.analysis.algorithm.business

import com.github.nscala_time.time.Imports._
import com.persist.logging._
import com.typesafe.config.Config
import org.apache.commons.math3.stat.descriptive.StatisticalSummary
import org.joda.{ time ⇒ joda }
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.algorithm.{ Advancing, Algorithm, AlgorithmShapeCompanion, CommonContext }
import spotlight.analysis.{ AnomalyScore, DetectUsing }
import spotlight.model.statistics.{ CircularBuffer, MovingStatistics }
import spotlight.model.timeseries._
import squants.information.{ Bytes, Information }

object PastPeriod extends ClassLogging {
  sealed abstract class Period extends Equals {
    def qualifier: Int
    def timestamp: joda.DateTime

    override def hashCode(): Int = 41 * ( 41 + qualifier.## )

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: Period ⇒ {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
              ( that canEqual this ) &&
              ( this.qualifier == that.qualifier )
          }
        }

        case _ ⇒ false
      }
    }
  }

  type PeriodValue = ( Period, Double )

  object Period {
    def fromDataPoint( dp: DataPoint ): PeriodValue = ( Period.fromTimestamp( dp.timestamp ), dp.value )
    def fromTimestamp( timestamp: joda.DateTime ): Period = PeriodImpl( toQualifier( timestamp.toLocalDate ), timestamp )
    def fromLocalDate( date: joda.LocalDate ): Period = PeriodImpl( toQualifier( date ), date.toDateTimeAtStartOfDay )

    private[algorithm] def toQualifier( date: joda.LocalDate ): Int = date.year.get * 100 + date.month.get

    def isCandidateMoreRecent( p: Period, candidate: Period ): Boolean = {
      //      log.debug(
      //        Map(
      //          "@msg" → "#TEST isCandidateMoreRecent",
      //          "qualifiers" → Map( "p" → p.qualifier, "candidate" → candidate.qualifier, "is-same" → ( p.qualifier == candidate.qualifier ) ),
      //          "timestamps" → Map( "p" → p.timestamp, "candidate" → candidate.timestamp, "more-recent" → ( p.timestamp < candidate.timestamp ) )
      //        )
      //      )
      ( p.qualifier == candidate.qualifier ) && ( p.timestamp < candidate.timestamp )
    }

    implicit val periodOrdering: Ordering[Period] = new Ordering[Period] {
      override def compare( lhs: Period, rhs: Period ): Int = {
        if ( lhs.qualifier != rhs.qualifier ) lhs.qualifier - rhs.qualifier
        else ( lhs.timestamp.getMillis - rhs.timestamp.getMillis ).toInt
      }
    }

    final case class PeriodImpl private[PastPeriod] (
        override val qualifier: Int,
        override val timestamp: joda.DateTime
    ) extends Period {
      override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[PeriodImpl]
      override def toString: String = s"Period(${qualifier}: ${timestamp}[${timestamp.getMillis}])"
    }
  }

  /** Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  case class Shape(
      window: Int,
      history: Int,
      N: Long = 0L,
      periods: CircularBuffer[PeriodValue] = CircularBuffer.empty[PeriodValue]
  ) {
    //    private val addPeriod = CircularBuffer.addTo[Period]( window )

    def pastPeriodsFromNow: Seq[PeriodValue] = pastPeriodsFromTimestamp( joda.DateTime.now )
    def pastPeriodsFromLocalDate( date: joda.LocalDate ): Seq[PeriodValue] = pastPeriodsFrom( Period fromLocalDate date )
    def pastPeriodsFromTimestamp( timestamp: joda.DateTime ): Seq[PeriodValue] = pastPeriodsFrom( Period fromTimestamp timestamp )

    def pastPeriodsFrom( period: Period ): Seq[PeriodValue] = {
      val current = currentPeriodValue map { _._1.qualifier } getOrElse Int.MaxValue
      val past = periods.takeWhile { case ( p, _ ) ⇒ p.qualifier < period.qualifier && p.qualifier < current }
      val r = past.drop( past.size - window )
      //      log.debug(
      //        Map(
      //          "@msg" → "pastPeriodsFrom",
      //          "period" → period.toString,
      //          "periods" → periods.toString,
      //          "current" → current,
      //          "past" → past.toString,
      //          "result" → r.toString
      //        )
      //      )
      r
    }

    def currentPeriodValue: Option[PeriodValue] = {
      periods.lastOption flatMap { pv ⇒ if ( inCurrentPeriod( pv._1 ) ) Some( pv ) else None }
    }

    def inCurrentPeriod( ts: joda.DateTime ): Boolean = ts.getMonthOfYear == joda.DateTime.now.getMonthOfYear
    def inCurrentPeriod( period: Period ): Boolean = period.qualifier == Period.toQualifier( joda.LocalDate.now )

    def addPeriodValue( period: Period, value: Double ): Shape = {
      val index = periods indexWhere { case ( p, _ ) ⇒ p.qualifier == period.qualifier }
      //      log.debug( Map( "@msg" → "#TEST: period index", "period" → period.toString, "index" → index ) )
      index match {
        case -1 ⇒ addPeriod( period, value )
        case i if ( Period.isCandidateMoreRecent( periods( i )._1, period ) ) ⇒ updatePeriod( period, value, i )
        case _ ⇒ copy( N = this.N + 1 )
      }
    }

    private val wholeWindow = window + 1 // to include current period at end of buffer

    private val addToPeriods = CircularBuffer.addTo( wholeWindow )( periods, _: PeriodValue )
    private[this] def addPeriod( p: Period, v: Double ): Shape = this.copy( N = this.N + 1, periods = addToPeriods( p, v ) )

    private[this] def updatePeriod( p: Period, v: Double, index: Int ): Shape = {
      val newPeriods = periods.updated( index, ( p, v ) )
      //      log.debug(
      //        Map(
      //          "@msg" → "#TEST: updatePeriod",
      //          "prior" → periods.toString,
      //          "new" → newPeriods.mkString( "[", ", ", "]" )
      //        )
      //      )

      this.copy( N = this.N + 1, periods = newPeriods )
    }

    private[this] def statsFrom( timestamp: joda.DateTime ): Option[StatisticalSummary] = {
      val values = pastPeriodsFrom( Period fromTimestamp timestamp ) map { _._2 }
      if ( values.isEmpty ) None else Option( MovingStatistics( window, values: _* ) )
    }

    def meanFrom( timestamp: joda.DateTime ): Option[Double] = statsFrom( timestamp ) map { _.getMean }
    def standardDeviationFrom( timestamp: joda.DateTime ): Option[Double] = statsFrom( timestamp ) map { _.getStandardDeviation }

    override def toString: String = {
      "PastPeriodAverageAlgorithm.Shape( " +
        s"N:[${N}] " +
        s"current-mean:[${meanFrom( joda.DateTime.now )}] current-stddev:[${standardDeviationFrom( joda.DateTime.now )}] " +
        s"current:[${currentPeriodValue}] " +
        s"""pastPeriods:[${periods.mkString( ", " )}] """ +
        ")"
    }
  }

  object Shape extends AlgorithmShapeCompanion {
    val WindowPath = "window"
    val DefaultWindow: Int = 3
    def windowFrom( configuration: Option[Config] ): Int = valueFrom( configuration, WindowPath, DefaultWindow ) { _ getInt _ }

    val HistoryPath = "history"
    val DefaultHistory: Int = DefaultWindow * 10
    def historyFrom( configuration: Option[Config] ): Int = valueFrom( configuration, HistoryPath, DefaultHistory ) { _ getInt _ }

    //    def applyWindow[T]( window: Int )( periods: List[T] ): List[T] = periods drop ( periods.size - window )

    implicit val advancing = new Advancing[Shape] {
      override def zero( configuration: Option[Config] ): Shape = {
        val window = valueFrom( configuration, WindowPath ) { _ getInt WindowPath } getOrElse 3
        val history = valueFrom( configuration, HistoryPath ) { _ getInt HistoryPath } getOrElse { 10 * window }
        Shape( window = window, history = history )
      }

      override def N( shape: Shape ): Long = shape.N

      override def advance( original: Shape, advanced: Advanced ): Shape = {
        val ( p, v ) = Period fromDataPoint advanced.point
        //        log.debug( Map( "@msg" → "#TEST ASSIGNING point to period", "timestamp" → advanced.point.timestamp, "period" → p ) )
        original.addPeriodValue( p, v )
      }

      override def copy( shape: Shape ): Shape = shape
    }
  }
}

/** Created by rolfsd on 10/14/16.
  */
object PastPeriodAverageAlgorithm extends Algorithm[PastPeriod.Shape]( label = "past-period" ) { algorithm ⇒
  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )

  override def score( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[AnomalyScore] = {
    for {
      m ← shape.meanFrom( point.dateTime )
      sd ← shape.standardDeviationFrom( point.dateTime )
    } yield {
      //      if ( sd == 0.0 || sd == Double.NaN ) throw InsufficientVariance( algorithm.label, shape.N )

      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.timestamp.toLong,
        expected = m,
        distance = c.tolerance * sd
      )

      AnomalyScore( threshold isOutlier point.value, threshold )
    }
  }

  /** Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    * @return blended average size for the algorithm shape
    */
  override def estimatedAverageShapeSize( properties: Option[Config] ): Option[Information] = {
    ( PastPeriod.Shape.windowFrom( properties ), PastPeriod.Shape.historyFrom( properties ) ) match {
      case ( PastPeriod.Shape.DefaultWindow, PastPeriod.Shape.DefaultHistory ) ⇒ Some( Bytes( 1763 ) )
      //      case window if window <= 32 ⇒ Some( Bytes( 716 + ( window * 13 ) ) ) // identified through observation
      //      case window if window <= 42 ⇒ Some( Bytes( 798 + ( window * 13 ) ) )
      //      case window if window <= 73 ⇒ Some( Bytes( 839 + ( window * 13 ) ) )
      //      case window if window <= 105 ⇒ Some( Bytes( 880 + ( window * 13 ) ) )
      case _ ⇒ None
    }
  }

}
