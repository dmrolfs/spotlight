package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ ActorRef, Props }
import scalaz._, Scalaz._
import scalaz.Kleisli.ask
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.Valid
import peds.commons.util._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{ AlgorithmContext, Op, Point2D, TryV }
import spotlight.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.DataPoint


/**
  * Created by rolfsd on 2/25/16.
  */
object SimpleMovingAverageAnalyzer {
  val Algorithm = 'stddev_from_simple_moving_average

  def props( router: ActorRef ): Props = Props { new SimpleMovingAverageAnalyzer( router ) }


  final case class Context private[skyline](
    override val underlying: AlgorithmContext,
    movingStatistics: DescriptiveStatistics
  ) extends SkylineContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[SkylineContext] = copy( underlying = ctx ).successNel

    override def toString: String = {
      s"""${getClass.safeSimpleName}(moving-stats:[${movingStatistics}])"""
    }
  }
}

class SimpleMovingAverageAnalyzer( override val router: ActorRef ) extends SkylineAnalyzer[SimpleMovingAverageAnalyzer.Context] {
  import SimpleMovingAverageAnalyzer._
  import SkylineAnalyzer.ApproximateDayWindow

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = SimpleMovingAverageAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = {
    makeStatistics( c ) map { movingStats => Context( underlying = c, movingStatistics = movingStats ) }
  }

  def makeStatistics( context: AlgorithmContext ): Valid[DescriptiveStatistics] = {
    new DescriptiveStatistics( ApproximateDayWindow ).successNel
  }


  /**
    * A timeseries is anomalous if the absolute value of the average of the latest
    * three datapoint minus the moving average is greater than tolerance * standard
    * deviations of the average. This does not exponentially weight the moving average
    * and so is better for detecting anomalies with respect to the entire series.
    */
  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, AlgorithmContext]
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
      taverages <- tailAverage <=< ask[TryV, AlgorithmContext]
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = taverages,
        context = context,
        isOutlier = (p: Point2D, ctx: Context) => {
          val (_, v) = p
          val mean = ctx.movingStatistics.getMean
          val stddev = ctx.movingStatistics.getStandardDeviation
          log.debug( "Stddev from simple moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]", mean, stddev, tol )
          math.abs( v - mean ) > ( tol * stddev)
        },
        update = (ctx: Context, pt: DataPoint) => {
          ctx.movingStatistics addValue pt.value
          ctx
        }
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
