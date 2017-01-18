package spotlight.analysis.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import org.joda.{time => joda}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import peds.commons.{KOp, Valid}
import peds.commons.util._
import spotlight.analysis.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 2/25/16.
  */
object FirstHourAverageAnalyzer {
  val Algorithm = Symbol( "first-hour-average" )

  def props( router: ActorRef ): Props = Props { new FirstHourAverageAnalyzer( router ) }

  val DebugTopic = Topic( "prod-las.em.authz-proxy.1.proxy.p95" )

  object Context {
    import com.github.nscala_time.time.Imports._
    val FirstHour: joda.Interval = new joda.Interval( joda.DateTime.now, 1.hour.toDuration )
  }

  final case class Context private[skyline](
    override val underlying: AlgorithmContext,
    firstHour: SummaryStatistics
  ) extends WrappingContext with LazyLogging {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addThresholdBoundary(threshold: ThresholdBoundary ): That = {
      copy( underlying = underlying.addThresholdBoundary( threshold ) )
    }

    def withPoints( points: Seq[DoublePoint] ): Context = {
      val firstHourPoints = points collect { case p if Context.FirstHour contains p.timestamp.toLong => p.value }
      if ( firstHourPoints.nonEmpty ) {
        if ( underlying.source.topic == DebugTopic ) {
          logger.info( "context: first-hour:[{}] context source points: [{}]", Context.FirstHour, source.points.mkString(",") )
          logger.info( s"""context: adding values to first hour: [${firstHourPoints.mkString(",")}]"""  )
        }
        val updated = firstHourPoints.foldLeft( firstHour.copy ) { (s, v) => s.addValue( v ); s }
        if ( underlying.source.topic == DebugTopic ) {
          logger.info( s"context: updated first-hour statistics: mean=[${updated.getMean}] stddev=[${updated.getStandardDeviation}]" )
        }
        copy( firstHour = updated )
      } else {
        this
      }
    }

    override def toString: String = {
      s"${getClass.safeSimpleName}(firstHour-mean:[${firstHour.getMean}] firstHour-StdDev:[${firstHour.getStandardDeviation}])"
    }
  }
}

class FirstHourAverageAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[FirstHourAverageAnalyzer.Context] {
  import FirstHourAverageAnalyzer._

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = FirstHourAverageAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
//todo: bad recreating summary statistics for each pt!!!  need to retain and build incrementally (is this the case in other algos?)
    makeFirstHourStatistics( c ) map { firstHour => Context( underlying = c, firstHour = firstHour ) }
  }

  def makeFirstHourStatistics(
    context: AlgorithmContext,
    initialStatistics: Option[SummaryStatistics] = None
  ): Valid[SummaryStatistics] = {
    val firstHourStats = initialStatistics getOrElse { new SummaryStatistics }
    firstHourStats.successNel
  }

  val contextWithFirstHourStats: KOp[AlgorithmContext, Context] = toConcreteContextK map { c => c withPoints c.source.points }

  /**
    * Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    * A timeseries is anomalous if the average of the last three datapoints
    * are outside of three standard deviations of this value.
    */
  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      ctx <- contextWithFirstHourStats
      tolerance <- tolerance
      taverages <- tailAverage( ctx.data )
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = taverages,
        analysisContext = ctx,
        evaluateOutlier = (p: PointT, c: Context) => {
          val threshold = ThresholdBoundary.fromExpectedAndDistance(
            timestamp = p.timestamp.toLong,
            expected = c.firstHour.getMean,
            distance = math.abs( tol * c.firstHour.getStandardDeviation )
          )

          if ( c.source.topic == DebugTopic ) {
            log.info(
              "find: first hour[{}] mean[{}] stdev[{}] tolerance[{}]",
              c.firstHour.getN,
              c.firstHour.getMean,
              c.firstHour.getStandardDeviation,
              tol
            )
            log.info(
              "find: first hour[{}] [{}] is-outlier:{} threshold = [{}]",
              algorithm.name,
              p.value,
              threshold.isOutlier(p.value),
              threshold
            )
          }

          ( threshold isOutlier p.value, threshold )
        },
        update = (c: Context, p: PointT) => c
      )
    }

    makeOutliersK( outliers )
  }
}
