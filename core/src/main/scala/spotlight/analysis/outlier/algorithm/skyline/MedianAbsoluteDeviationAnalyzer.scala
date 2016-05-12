package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.ask
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.{KOp, TryV, Valid}
import peds.commons.util._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._


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
  ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addThresholdBoundary( threshold: ThresholdBoundary ): That = {
      copy( underlying = underlying.addThresholdBoundary( threshold ) )
    }

    override def toString: String = {
      s"""${getClass.safeSimpleName}(moving-stats:[${movingStatistics}] deviation-stats:[${deviationStatistics}])"""
    }
  }

}

class MedianAbsoluteDeviationAnalyzer( override val router: ActorRef )
extends CommonAnalyzer[MedianAbsoluteDeviationAnalyzer.Context] {
  import MedianAbsoluteDeviationAnalyzer._
  import CommonAnalyzer.ApproximateDayWindow

  type Context = MedianAbsoluteDeviationAnalyzer.Context

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = MedianAbsoluteDeviationAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
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
  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      ctx <- toConcreteContextK
      tolerance <- tolerance
    } yield {
      val tol = tolerance getOrElse 3D  // skyline source uses 6.0 - admittedly arbitrary?

      def deviation( v: Double, c: Context ): Double = {
        val movingMedian = c.movingStatistics getPercentile 50
        log.debug( "medianAbsoluteDeviation: N:[{}] movingMedian:[{}]", c.deviationStatistics.getN, movingMedian )
        math.abs( v - movingMedian )
      }

      collectOutlierPoints(
        points = ctx.source.points,
        analysisContext = ctx,
        evaluateOutlier = (p: PointT, c: Context) => {
          val threshold = ThresholdBoundary.fromExpectedAndDistance(
            timestamp = p.timestamp.toLong,
            expected = c.movingStatistics.getPercentile( 50 ),
            distance = tol * c.deviationStatistics.getPercentile( 50 )
          )

          ( threshold isOutlier p.value, threshold )
//          val d = deviation( v, ctx )
//          val deviationMedian = ctx.deviationStatistics getPercentile 50
//          log.debug( "medianAbsoluteDeviation: N:[{}] deviation:[{}] deviationMedian:[{}]", ctx.deviationStatistics.getN, d, deviationMedian )
//          d > ( tol * deviationMedian )
        },
        update = (c: Context, p: PointT) => {
          c.movingStatistics addValue p.value
          c.deviationStatistics addValue math.abs( p.value - c.movingStatistics.getPercentile(50) )
//          ctx.deviationStatistics addValue deviation( v, ctx )
          c
        }
      )
    }

    makeOutliersK( outliers )
  }
}
