package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.ask
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.Valid
import peds.commons.util._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, TryV}
import spotlight.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{ControlBoundary, Point2D, TimeSeriesBase}


/**
  * Created by rolfsd on 2/25/16.
  */
object MedianAbsoluteDeviationAnalyzer {
  val Algorithm = Symbol( "median-absolute-deviation" )

  def props( router: ActorRef ): Props = Props { new MedianAbsoluteDeviationAnalyzer( router ) }

  final case class Context private[skyline](
    override val underlying: AlgorithmContext,
    movingStatistics: DescriptiveStatistics,
    deviationStatistics: DescriptiveStatistics
  ) extends SkylineContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[SkylineContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addControlBoundary( control: ControlBoundary ): That = copy(underlying = underlying.addControlBoundary(control))

    override def toString: String = {
      s"""${getClass.safeSimpleName}(moving-stats:[${movingStatistics}] deviation-stats:[${deviationStatistics}])"""
    }
  }

}

class MedianAbsoluteDeviationAnalyzer( override val router: ActorRef )
extends SkylineAnalyzer[MedianAbsoluteDeviationAnalyzer.Context] {
  import MedianAbsoluteDeviationAnalyzer._
  import SkylineAnalyzer.ApproximateDayWindow

  type Context = MedianAbsoluteDeviationAnalyzer.Context

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = MedianAbsoluteDeviationAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = {
    ( makeMovingStatistics(c) |@| makeDeviationStatistics(c) ) { (m, d) =>
      Context( underlying = c, movingStatistics = m, deviationStatistics = d )
    }
  }

  def makeMovingStatistics( context: AlgorithmContext ): Valid[DescriptiveStatistics] = {
    new DescriptiveStatistics( ApproximateDayWindow ).successNel
  }

  def makeDeviationStatistics( context: AlgorithmContext ): Valid[DescriptiveStatistics] = {
    new DescriptiveStatistics( ApproximateDayWindow ).successNel
  }

  /**
    * A timeseries is anomalous if the deviation of its latest datapoint with
    * respect to the median is [tolerance] times larger than the median of deviations.
    */
  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, AlgorithmContext]
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
    } yield {
      val tol = tolerance getOrElse 3D  // skyline source uses 6.0 - admittedly arbitrary?

      def deviation( value: Double, ctx: Context ): Double = {
        val movingMedian = ctx.movingStatistics getPercentile 50
        log.debug( "medianAbsoluteDeviation: N:[{}] movingMedian:[{}]", ctx.deviationStatistics.getN, movingMedian )
        math.abs( value - movingMedian )
      }

      collectOutlierPoints(
        points = context.source.pointsAsPairs,
        context = context,
        evaluateOutlier = (p: Point2D, ctx: Context) => {
          val (ts, v) = p
          val control = ControlBoundary.fromExpectedAndDistance(
            timestamp = ts.toLong,
            expected = ctx.movingStatistics.getPercentile( 50 ),
            distance = tol * ctx.deviationStatistics.getPercentile( 50 )
          )
          ( control isOutlier v, control )
//          val d = deviation( v, ctx )
//          val deviationMedian = ctx.deviationStatistics getPercentile 50
//          log.debug( "medianAbsoluteDeviation: N:[{}] deviation:[{}] deviationMedian:[{}]", ctx.deviationStatistics.getN, d, deviationMedian )
//          d > ( tol * deviationMedian )
        },
        update = (ctx: Context, pt: Point2D) => {
          val (_, v) = pt
          ctx.movingStatistics addValue v
          ctx.deviationStatistics addValue math.abs( v - ctx.movingStatistics.getPercentile(50) )
//          ctx.deviationStatistics addValue deviation( v, ctx )
          ctx
        }
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
