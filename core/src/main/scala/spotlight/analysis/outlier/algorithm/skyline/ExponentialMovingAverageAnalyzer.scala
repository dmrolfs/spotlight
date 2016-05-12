package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import peds.commons.{KOp, Valid}
import peds.commons.util._
import spotlight.analysis.outlier.Moment
import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 2/25/16.
  */
object ExponentialMovingAverageAnalyzer {
  val Algorithm = 'ewma

  def props( router: ActorRef ): Props = Props { new ExponentialMovingAverageAnalyzer( router ) }


  final case class Context private[skyline]( override val underlying: AlgorithmContext, moment: Moment ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addThresholdBoundary(threshold: ThresholdBoundary ): That = {
      copy( underlying = underlying.addThresholdBoundary( threshold ) )
    }

    override def toString: String = s"""${getClass.safeSimpleName}(momentAt:[${moment}])"""
  }
}

class ExponentialMovingAverageAnalyzer(
  override val router: ActorRef
) extends CommonAnalyzer[ExponentialMovingAverageAnalyzer.Context] {
  import ExponentialMovingAverageAnalyzer._

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = ExponentialMovingAverageAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
    makeStatistics( c ) map { moment => Context( underlying = c, moment = moment ) }
  }

  def makeStatistics( ctx: AlgorithmContext ): Valid[Moment] = {
    Moment.withAlpha( id = ctx.historyKey.toString, alpha = 0.05 )
  }


  /**
    * A timeseries is anomalous if the absolute value of the average of the latest
    * three datapoint minus the moving average is greater than tolerance * standard
    * deviations of the exponential moving average. This is better for finding anomalies
    * with respect to the short term trends.
    */
  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      ctx <- toConcreteContextK
      tolerance <- tolerance
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = ctx.source.points,
        analysisContext = ctx,
        evaluateOutlier = (pt: PointT, c: Context) => {
          c.moment.statistics map { stats =>
            log.debug(
              "pt:[{}] - Stddev from exponential moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]",
              (pt.timestamp.toLong, pt.value),
              stats.ewma,
              stats.ewmsd,
              tol
            )
            //            math.abs( v - stats.ewma ) > ( tol * stats.ewmsd )
            val threshold = ThresholdBoundary.fromExpectedAndDistance(
              timestamp = pt.timestamp.toLong,
              expected = stats.ewma,
              distance = math.abs( tol * stats.ewmsd )
            )

            ( threshold isOutlier pt.value, threshold )
          } getOrElse {
            ( false, ThresholdBoundary.empty( pt.timestamp.toLong ) )
          }
        },
        update = (c: Context, pt: PointT) => {
          log.debug(
            "stddevFromMovingAverage: adding point ({}, {}) to historical momentAt: [{}]",
            pt.timestamp.toLong,
            pt.value,
            c.moment.statistics
          )
          c.copy( moment = c.moment :+ pt.value )
        }
      )
    }

    makeOutliersK( outliers )
  }
}
