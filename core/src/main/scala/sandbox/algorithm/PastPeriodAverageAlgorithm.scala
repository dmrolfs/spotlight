package sandbox.algorithm

import scala.reflect.ClassTag
import scalaz.Validation
import scalaz.syntax.validation._
import shapeless.{Lens, lens}
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.joda.{time => joda}
import com.github.nscala_time.time.Imports._
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.Valid
import peds.commons.log.Trace
import spotlight.analysis.outlier.DetectUsing
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmModule.RedundantAlgorithmConfiguration
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 10/14/16.
  */
object PastPeriodAverageAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  private val trace = Trace[PastPeriodAverageAlgorithm.type]

  override def algorithm: Algorithm = new Algorithm {
    override val label: Symbol = Symbol( "past-period" )

    override def prepareData( algorithmContext: Context ): Seq[DoublePoint] = {
      algorithmContext.tailAverage()( algorithmContext.data )
    }

    override def step( point: PointT )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)] = {
      for {
        m <- s.shape.mean
        sd <- s.shape.standardDeviation
      } yield {
        val threshold = ThresholdBoundary.fromExpectedAndDistance(
          timestamp = point.timestamp.toLong,
          expected = m,
          distance = c.tolerance * sd
        )

        val isOutlier = s.shape.inCurrentPeriod( point.dateTime ) && threshold.isOutlier( point.value )
        ( isOutlier, threshold )
      }
    }
  }

  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )


  sealed abstract class Period extends Equals {
    def descriptor: Int
    def timestamp: joda.DateTime

    override def hashCode(): Int = 41 * ( 41 + descriptor.## )

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: Period => {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.descriptor == that.descriptor )
          }
        }

        case _ => false
      }
    }
  }

  object Period {
    def assign( dp: DataPoint ): PeriodValue = ( Period.assign(dp.timestamp), dp.value )
    def assign( timestamp: joda.DateTime ): Period = PeriodImpl( descriptor = timestamp.getMonthOfYear, timestamp )

    implicit val periodOrdering: Ordering[Period] = Ordering by { _.descriptor }

    final case class PeriodImpl private[PastPeriodAverageAlgorithm](
      override val descriptor: Int,
      override val timestamp: joda.DateTime
    ) extends Period {
      override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[PeriodImpl]
      override def toString: String = s"Period(${descriptor}: ${timestamp}:${timestamp.getMillis})"
    }
  }


  type PeriodValue = (Period, Double)

  /**
    * Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  case class Shape(
    currentPeriodValue: Option[PeriodValue] = None,
    priorPeriods: List[PeriodValue] = List.empty[PeriodValue],
    resize: List[PeriodValue] => List[PeriodValue]
  ) {
    def inCurrentPeriod( ts: joda.DateTime ): Boolean = ts.getMonthOfYear == joda.DateTime.now.getMonthOfYear
    def inCurrentPeriod( period: Period ): Boolean = period.descriptor == joda.DateTime.now.getMonthOfYear

    def withPeriodValue( period: Period, value: Double ): Shape = trace.block( "withPeriodValue" ) {
      if ( inCurrentPeriod(period) ) withCurrentPeriod( period, value )
      else {
        val i = priorPeriods indexWhere { case (p, _) => p.descriptor == period.descriptor }
        logger.debug( "TEST: prior period index for [{}] = [{}]", period, i.toString )
        if ( i == -1 ) this.withNewPriorPeriod( period, value )
        else if ( Shape.isCandidateMoreRecent(priorPeriods(i)._1, period) ) this.withUpdatedPriorPeriod(period, value, i)
        else this
      }
    }

    def withCurrentPeriod( p: Period, v: Double ): Shape = trace.block( "withCurrentPeriod" ) {
      currentPeriodValue
      .map { case (current, _) =>
        if ( Shape.isCandidateMoreRecent( current, p ) ) this.copy( currentPeriodValue = Some( (p, v) ) ) else this
      }
      .getOrElse { this.copy( currentPeriodValue = Some( (p, v) ) ) }
    }

    def withUpdatedPriorPeriod( p: Period, v: Double, index: Int ): Shape = trace.block( "withUpdatedPriorPeriod" ) {
      val (h, c :: t) = priorPeriods splitAt index
      val newPriors = h ::: ( (p, v) :: t )
      logger.debug( "TEST: priorPeriods=[{}] newPriors:[{}]", priorPeriods, newPriors )
      this.copy( priorPeriods = newPriors )
    }

    def withNewPriorPeriod( p: Period, v: Double ): Shape = trace.block("withNewPriorPeriod") {
      val newPriors = resize( ( (p, v) :: priorPeriods ) sortBy { _._1 } )
      this.copy( priorPeriods = newPriors )
    }

    private val stats: Option[DescriptiveStatistics] = {
      val values = resize( priorPeriods ) map {_._2 }
      if ( values.isEmpty ) None else Option( new DescriptiveStatistics(values.toArray) )
    }

    val mean: Option[Double] = stats map { _.getMean }
    val standardDeviation: Option[Double] = stats map { _.getStandardDeviation }

    override def toString: String = {
      "PastPeriodAverageAlgorithm.Shape( " +
      s"mean:[${mean}] stddev:[${standardDeviation}] " +
      s"current:[${currentPeriodValue}] " +
      s"pastPeriods[${stats.map{_.getN}.getOrElse(0)}]:[${priorPeriods}] " +
      ")"
    }
  }

  object Shape {
    def isCandidateMoreRecent( p: Period, candidate: Period ): Boolean = trace.block("isCandidateMoreRecent") {
      logger.debug( "TEST: p.desciptor[{}] == candidate.descriptor[{}]: {}", p.descriptor.toString, candidate.descriptor.toString, (p.descriptor == candidate.descriptor).toString)
      logger.debug( "TEST: p.timestamp[{}] < candidate.timestamp[{}]: {}", p.timestamp, candidate.timestamp, (p.timestamp < candidate.timestamp).toString)
      ( p.descriptor == candidate.descriptor ) && ( p.timestamp < candidate.timestamp )
    }

    def sizeToWindow[T]( window: Int )( periods: List[T] ) = periods drop ( periods.size - window )
  }


  import AlgorithmModule.{ AnalysisState, StrictSelf }

  case class State(
    override val id: TID,
    override val name: String,
    shape: Shape,
    window: Int
  ) extends AnalysisState with StrictSelf[State] {
    override type Self = State
    override def algorithm: Symbol = outer.algorithm.label

    override def withConfiguration( configuration: Config ): Valid[State] = {
      State.getWindow( configuration ) match {
        case scalaz.Success( newWindow ) if newWindow != window => State.windowLens.set( this )( newWindow ).successNel

        case scalaz.Success( dup ) => {
          logger.debug( "ignoring duplicate configuration: window:[{}]", window )
          Validation.failureNel( RedundantAlgorithmConfiguration(id, path = State.WindowPath, value = dup) )
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

    override def toString: String = {
      s"PastPeriodAverageAlgorithm.State( id:[${id}] window:[${window}] shape:[${shape}] )"
    }
  }

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]

    override def zero( id: State#TID ): State = {
      State( id, name = "", Shape( resize = Shape sizeToWindow DefaultWindow ), DefaultWindow )
    }

    override def advanceShape( shape: Shape, advanced: Advanced ): Shape = {
      val (p, v) = Period assign advanced.point
      logger.debug( "TEST: ASSIGNING point:[{}] to period:{}", advanced.point.timestamp, p )
      shape.withPeriodValue( p, v )
    }

    override val shapeLens: Lens[State, Shape] = lens[State] >> 'shape

    val windowLens: Lens[State, Int] = new Lens[State, Int] {
      override def get( s: State ): Int = s.window
      override def set( s: State )( w: Int ): State = {
        if ( w == s.window ) s
        else s.copy( window = w, shape = s.shape.copy( resize = Shape sizeToWindow w ) )
      }
    }

    val DefaultWindow: Int = 3

    val RootPath = algorithm.label.name
    val WindowPath = RootPath + ".window"
    def getWindow( conf: Config ): Valid[Int] = {
      val window = if ( conf hasPath WindowPath ) conf getInt WindowPath else DefaultWindow
      if ( window > 0 ) window.successNel
      else {
        Validation.failureNel(
          AlgorithmModule.InvalidAlgorithmConfiguration( algorithm.label, WindowPath, "positive integer value" )
        )
      }
    }
  }

  override implicit def evState: ClassTag[State] = ClassTag( classOf[State] )

  override val analysisStateCompanion: AnalysisStateCompanion = State
}
