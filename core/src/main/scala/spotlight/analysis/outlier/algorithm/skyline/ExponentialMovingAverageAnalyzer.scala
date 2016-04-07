package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._, Scalaz._
import scalaz.Kleisli.ask
import peds.commons.Valid
import peds.commons.util._
import spotlight.analysis.outlier.Moment
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, TryV}
import spotlight.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{ControlBoundary, Point2D, TimeSeriesBase}


/**
  * Created by rolfsd on 2/25/16.
  */
object ExponentialMovingAverageAnalyzer {
  val Algorithm = 'ewma

  def props( router: ActorRef ): Props = Props { new ExponentialMovingAverageAnalyzer( router ) }


  final case class Context private[skyline]( override val underlying: AlgorithmContext, moment: Moment ) extends SkylineContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[SkylineContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addControlBoundary( control: ControlBoundary ): That = copy(underlying = underlying.addControlBoundary(control))

    override def toString: String = s"""${getClass.safeSimpleName}(momentAt:[${moment}])"""
  }
}

class ExponentialMovingAverageAnalyzer(
  override val router: ActorRef
) extends SkylineAnalyzer[ExponentialMovingAverageAnalyzer.Context] {
  import ExponentialMovingAverageAnalyzer._

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = ExponentialMovingAverageAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = {
    makeStatistics( c ) map { moment => Context( underlying = c, moment = moment ) }
  }

  def makeStatistics( context: AlgorithmContext ): Valid[Moment] = {
    Moment.withAlpha( id = context.historyKey.toString, alpha = 0.05 )
  }


  /**
    * A timeseries is anomalous if the absolute value of the average of the latest
    * three datapoint minus the moving average is greater than tolerance * standard
    * deviations of the exponential moving average. This is better for finding anomalies
    * with respect to the short term trends.
    */
  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, AlgorithmContext]
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = context.source.pointsAsPairs,
        context = context,
        evaluateOutlier = (p: Point2D, ctx: Context) => {
          ctx.moment.statistics map { stats =>
            val (ts, v) = p
            log.debug( "pt:[{}] - Stddev from exponential moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]", (ts.toLong, v), stats.ewma, stats.ewmsd, tol )
            //            math.abs( v - stats.ewma ) > ( tol * stats.ewmsd )
            val control = ControlBoundary.fromExpectedAndDistance(
              timestamp = ts.toLong,
              expected = stats.ewma,
              distance = math.abs( tol * stats.ewmsd )
            )

            ( control.isOutlier(v), control )
          } getOrElse {
            ( false, ControlBoundary.empty(p._1.toLong) )
          }
        },
        update = (ctx: Context, pt: Point2D) => {
          val (ts, v) = pt
          log.debug( "stddevFromMovingAverage: adding point ({}, {}) to historical momentAt: [{}]", ts.toLong, v, context.moment.statistics )
          ctx.copy( moment = ctx.moment :+ v )
        }
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
