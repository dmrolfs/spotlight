package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.{KOp, Valid}
import peds.commons.util._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 2/25/16.
  */
object SimpleMovingAverageAnalyzer {
  val Algorithm = Symbol( "simple-moving-average" )

  def props( router: ActorRef ): Props = Props { new SimpleMovingAverageAnalyzer( router ) }


  final case class Context private[skyline](
    override val underlying: AlgorithmContext,
    movingStatistics: DescriptiveStatistics
  ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addControlBoundary( control: ControlBoundary ): That = copy(underlying = underlying.addControlBoundary(control))

    override def toString: String = {
      s"""${getClass.safeSimpleName}(moving-stats:[${movingStatistics}])"""
    }
  }
}

class SimpleMovingAverageAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[SimpleMovingAverageAnalyzer.Context] {
  import SimpleMovingAverageAnalyzer._
  import CommonAnalyzer.ApproximateDayWindow

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = SimpleMovingAverageAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
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
  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      ctx <- toConcreteContextK
      tolerance <- tolerance
      taverages <- tailAverage( ctx.data )
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = taverages,
        analysisContext = ctx,
        evaluateOutlier = (p: PointT, c: Context) => {
          val mean = c.movingStatistics.getMean
          val stddev = c.movingStatistics.getStandardDeviation
          log.debug(
            "Stddev from simple moving Average N[{}]: mean[{}]\tstdev[{}]\ttolerance[{}]",
            c.movingStatistics.getN, mean, stddev, tol
          )

          val control = ControlBoundary.fromExpectedAndDistance(
            timestamp = p.timestamp.toLong,
            expected = mean,
            distance = tol * stddev
          )
          ( control isOutlier p.value, control )
        },
        update = (c: Context, p: PointT) => {
          c.movingStatistics addValue p.value
          c
        }
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
