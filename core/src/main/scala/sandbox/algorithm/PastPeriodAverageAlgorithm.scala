package sandbox.algorithm

import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.joda.{ time ⇒ joda }
import com.github.nscala_time.time.Imports._
import com.persist.logging._
import omnibus.commons.identifier.Identifying
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import spotlight.analysis.DetectUsing
import spotlight.analysis.algorithm.{ Advancing, Algorithm, AlgorithmState, CommonContext }
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
import spotlight.model.timeseries._

object PastPeriod extends ClassLogging {
  sealed abstract class Period extends Equals {
    def descriptor: Int
    def timestamp: joda.DateTime

    override def hashCode(): Int = 41 * ( 41 + descriptor.## )

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: Period ⇒ {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
              ( that canEqual this ) &&
              ( this.descriptor == that.descriptor )
          }
        }

        case _ ⇒ false
      }
    }
  }

  type PeriodValue = ( Period, Double )

  object Period {
    def assign( dp: DataPoint ): PeriodValue = ( Period.assign( dp.timestamp ), dp.value )
    def assign( timestamp: joda.DateTime ): Period = PeriodImpl( descriptor = timestamp.getMonthOfYear, timestamp )

    def isCandidateMoreRecent( p: Period, candidate: Period ): Boolean = {
      log.debug(
        Map(
          "@msg" → "#TEST isCandidateMoreRecent",
          "descriptors" → Map( "p" → p.descriptor, "candidate" → candidate.descriptor, "is-same" → ( p.descriptor == candidate.descriptor ) ),
          "timestamps" → Map( "p" → p.timestamp, "candidate" → candidate.timestamp, "more-recent" → ( p.timestamp < candidate.timestamp ) )
        )
      )
      ( p.descriptor == candidate.descriptor ) && ( p.timestamp < candidate.timestamp )
    }

    implicit val periodOrdering: Ordering[Period] = Ordering by { _.descriptor }

    final case class PeriodImpl private[PastPeriod] (
        override val descriptor: Int,
        override val timestamp: joda.DateTime
    ) extends Period {
      override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[PeriodImpl]
      override def toString: String = s"Period(${descriptor}: ${timestamp}:${timestamp.getMillis})"
    }
  }

  /** Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  case class Shape(
      currentPeriodValue: Option[PeriodValue] = None,
      priorPeriods: List[PeriodValue] = List.empty[PeriodValue],
      resize: List[PeriodValue] ⇒ List[PeriodValue]
  ) {
    def inCurrentPeriod( ts: joda.DateTime ): Boolean = ts.getMonthOfYear == joda.DateTime.now.getMonthOfYear
    def inCurrentPeriod( period: Period ): Boolean = period.descriptor == joda.DateTime.now.getMonthOfYear

    def withPeriodValue( period: Period, value: Double ): Shape = {
      if ( inCurrentPeriod( period ) ) withCurrentPeriod( period, value )
      else {
        val i = priorPeriods indexWhere { case ( p, _ ) ⇒ p.descriptor == period.descriptor }
        log.debug( Map( "@msg" → "#TEST: prior period index", "period" → period.toString, "index" → i ) )
        if ( i == -1 ) this.withNewPriorPeriod( period, value )
        else if ( Period.isCandidateMoreRecent( priorPeriods( i )._1, period ) ) this.withUpdatedPriorPeriod( period, value, i )
        else this
      }
    }

    def withCurrentPeriod( p: Period, v: Double ): Shape = {
      currentPeriodValue
        .map {
          case ( current, _ ) ⇒ {
            if ( Period.isCandidateMoreRecent( current, p ) ) this.copy( currentPeriodValue = Some( ( p, v ) ) ) else this
          }
        }
        .getOrElse { this.copy( currentPeriodValue = Some( ( p, v ) ) ) }
    }

    def withUpdatedPriorPeriod( p: Period, v: Double, index: Int ): Shape = {
      val ( h, c :: t ) = priorPeriods splitAt index
      val newPriors = h ::: ( ( p, v ) :: t )
      log.debug(
        Map(
          "@msg" → "#TEST: withUpdatedPriorPeriod",
          "prior" → priorPeriods.toString,
          "new" → newPriors.mkString( "[", ", ", "]" )
        )
      )

      this.copy( priorPeriods = newPriors )
    }

    def withNewPriorPeriod( p: Period, v: Double ): Shape = {
      val newPriors = resize( ( ( p, v ) :: priorPeriods ) sortBy { _._1 } )
      this.copy( priorPeriods = newPriors )
    }

    private val stats: Option[DescriptiveStatistics] = {
      val values = resize( priorPeriods ) map { _._2 }
      if ( values.isEmpty ) None else Option( new DescriptiveStatistics( values.toArray ) )
    }

    val mean: Option[Double] = stats map { _.getMean }
    val standardDeviation: Option[Double] = stats map { _.getStandardDeviation }

    override def toString: String = {
      "PastPeriodAverageAlgorithm.Shape( " +
        s"mean:[${mean}] stddev:[${standardDeviation}] " +
        s"current:[${currentPeriodValue}] " +
        s"pastPeriods[${stats.map { _.getN }.getOrElse( 0 )}]:[${priorPeriods}] " +
        ")"
    }
  }

  object Shape {
    val WindowPath = "window"
    def applyWindow[T]( window: Int )( periods: List[T] ): List[T] = periods drop ( periods.size - window )

    implicit val advancing = new Advancing[Shape] {
      override def zero( configuration: Option[Config] ): Shape = {
        val window = valueFrom( configuration, WindowPath ) { _ getInt WindowPath } getOrElse 3
        Shape( resize = applyWindow( window ) )
      }

      override def advance( original: Shape, advanced: Advanced ): Shape = {
        val ( p, v ) = Period assign advanced.point
        log.debug( Map( "@msg" → "#TEST ASSIGNING point to period", "timestamp" → advanced.point.timestamp, "period" → p ) )
        original.withPeriodValue( p, v )
      }
    }
  }
}

/** Created by rolfsd on 10/14/16.
  */
object PastPeriodAverageAlgorithm extends Algorithm[PastPeriod.Shape] { algorithm ⇒
  override val label: String = "past-period"

  override def prepareData( c: Context ): Seq[DoublePoint] = c.tailAverage()( c.data )

  override def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[( Boolean, ThresholdBoundary )] = {
    for {
      m ← shape.mean
      sd ← shape.standardDeviation
    } yield {
      val threshold = ThresholdBoundary.fromExpectedAndDistance(
        timestamp = point.timestamp.toLong,
        expected = m,
        distance = c.tolerance * sd
      )

      val isOutlier = shape.inCurrentPeriod( point.dateTime ) && threshold.isOutlier( point.value )
      ( isOutlier, threshold )
    }
  }

  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )
}
