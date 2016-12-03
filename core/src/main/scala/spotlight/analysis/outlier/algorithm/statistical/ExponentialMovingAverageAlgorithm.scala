package spotlight.analysis.outlier.algorithm.statistical

import scala.reflect.ClassTag
import scalaz.{-\/, \/-}
import scalaz.syntax.validation._
import shapeless.{Lens, lens}
import com.typesafe.config.Config
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.ml.clustering.DoublePoint
import peds.commons.Valid
import peds.commons.log.Trace
import spotlight.analysis.outlier.{DetectUsing, Moment}
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 11/12/16.
  */
object ExponentialMovingAverageAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  import AlgorithmModule.{ AnalysisState, StrictSelf }

  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = 'ewma

    override def prepareData( algorithmContext: Context ): Seq[DoublePoint] = algorithmContext.data

    override def step( point: PointT )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)] = {
      s.moment.statistics map { stats =>
        logger.debug(
          "pt:[{}] - Stddev from exponential moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]",
          (point.timestamp.toLong, point.value),
          stats.ewma.toString,
          stats.ewmsd.toString,
          c.tolerance.toString
        )

        val threshold = ThresholdBoundary.fromExpectedAndDistance(
          timestamp = point.dateTime,
          expected = stats.ewma,
          distance = math.abs( c.tolerance * stats.ewmsd )
        )

        ( threshold isOutlier point.value, threshold )
      }
    }
  }


  override type Context = CommonContext
  override def makeContext( message: DetectUsing, state: Option[State] ): Context = new CommonContext( message )


  case class State(
    override val id: TID,
    override val name: String,
    moment: Shape = makeShape()
  ) extends AnalysisState with StrictSelf[State] {
    override type Self = State

    override def algorithm: Symbol = outer.algorithm.label  //todo remove need by having module: AlgorithmModule ref in AnalysisState?
    override def withConfiguration( configuration: Config ): Valid[State] = this.successNel

    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
    override def toString: String = s"${ClassUtils.getAbbreviatedName(getClass, 15)}( id:[${id}] moment:[${moment}] )"
  }

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]
    override def zero( id: State#TID ): State = State( id = id, name = "" )
    override def advanceShape( moment: Shape, advanced: Advanced ): Shape = moment :+ advanced.point.value
    override def shapeLens: Lens[State, Shape] = lens[State] >> 'moment
  }

  override implicit def evState: ClassTag[State] = ClassTag( classOf[State] )
  override val analysisStateCompanion: AnalysisStateCompanion = State

  override type Shape = Moment
  def makeShape(): Shape = {
    Moment.withAlpha( 0.05 ).disjunction match {
      case \/-( m ) => m
      case -\/( exs ) => {
        exs foreach { ex => logger.error( "failed to create moment shape", ex ) }
        throw exs.head
      }
    }
  }
}
