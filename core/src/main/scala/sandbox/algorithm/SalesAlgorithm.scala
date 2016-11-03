package sandbox.algorithm

import scala.reflect.ClassTag
import scalaz.{-\/, \/-}
import scalaz.syntax.either._
import shapeless.{Lens, lens}
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.joda.{time => joda}
import com.github.nscala_time.time.Imports._
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.TryV
import peds.commons.log.Trace
import spotlight.analysis.outlier.DetectUsing
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 10/14/16.
  */
object SalesAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  private val trace = Trace[SalesAlgorithm.type]

  override def algorithm: Algorithm = new Algorithm {
    override val label: Symbol = 'MonthlySales

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

        ( threshold isOutlier point.value, threshold )
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

    final case class PeriodImpl private[SalesAlgorithm](
      override val descriptor: Int,
      override val timestamp: joda.DateTime
    ) extends Period {
      override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[PeriodImpl]
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
    def withPeriodValue( period: Period, value: Double ): Shape = {
      if ( period.descriptor == joda.DateTime.now.getMonthOfYear ) withCurrentPeriod( period, value )
      else {
        val i = priorPeriods indexWhere { case (p, _) => Shape.isCandidateMoreRecentForPeriod( p, period ) }
        if ( i == -1 ) withNewPriorPeriod( period, value )
        else withUpdatedPriorPeriod( period, value, i )
      }
    }

    def withCurrentPeriod( p: Period, v: Double ): Shape = {
      currentPeriodValue
      .map { case (current, _) =>
        if ( Shape.isCandidateMoreRecentForPeriod( current, p ) ) this.copy( currentPeriodValue = Some( (p, v) ) ) else this
      }
      .getOrElse { this }
    }

    def withUpdatedPriorPeriod( p: Period, v: Double, index: Int ): Shape = {
      val (h, c :: t) = priorPeriods splitAt index
      val newPriors = h ::: ( (p, v) :: t )
      this.copy( priorPeriods = newPriors )
    }

    def withNewPriorPeriod( p: Period, v: Double ): Shape = {
      val newPriors = resize( ( (p, v) :: priorPeriods ) sortBy { _._1 } )
      this.copy( priorPeriods = newPriors )
    }

    private val stats: Option[DescriptiveStatistics] = {
      val values = resize( priorPeriods ) map {_._2 }
      if ( values.isEmpty ) None else Option( new DescriptiveStatistics(values.toArray) )
    }

    val mean: Option[Double] = stats map { _.getMean }
    val standardDeviation: Option[Double] = stats map { _.getStandardDeviation }
  }

  object Shape {
    def isCandidateMoreRecentForPeriod( p: Period, candidate: Period ): Boolean = {
      p == candidate && p.timestamp < candidate.timestamp
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

    override def withConfiguration( configuration: Config ): Option[State] = {
      State.getWindow( configuration ) match {
        case \/-( newWindow ) if newWindow != window => Some( State.windowLens.set(this)(newWindow) )

        case \/-( _ ) => {
          logger.debug( "ignoring duplicate configuration: window:[{}]", window )
          None
        }

        case -\/( ex ) => {
          logger.info(
            "ignoring configuration provided without properties relevant to {} algorithm: [{}]",
            outer.algorithm.label,
            configuration
          )
          None
        }
      }
    }

    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
  }

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]

    override def zero( id: State#TID ): State = {
      State( id, name = "", Shape( resize = Shape sizeToWindow DefaultWindow ), DefaultWindow )
    }

    override def advanceShape( shape: Shape, advanced: Advanced ): Shape = {
      val (p, v) = Period assign advanced.point
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
    def getWindow( conf: Config ): TryV[Int] = {
      logger.debug( "configuration[{}] getInt=[{}]", WindowPath, conf.getInt(WindowPath).toString )
      val window = if ( conf hasPath WindowPath ) conf getInt WindowPath else DefaultWindow
      if ( window > 0 ) window.right
      else AlgorithmModule.InvalidAlgorithmConfiguration( algorithm.label, WindowPath, "positive integer value" ).left
    }
  }

  override implicit def evState: ClassTag[State] = ClassTag( classOf[State] )

  override val analysisStateCompanion: AnalysisStateCompanion = State
}
