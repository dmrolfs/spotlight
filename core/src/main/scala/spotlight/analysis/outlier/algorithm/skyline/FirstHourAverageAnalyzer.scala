package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.ask
import org.joda.{time => joda}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import peds.commons.Valid
import peds.commons.util._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, TryV}
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{ControlBoundary, Point2D, TimeSeriesBase}


/**
  * Created by rolfsd on 2/25/16.
  */
object FirstHourAverageAnalyzer {
  val Algorithm = Symbol( "first-hour-average" )

  def props( router: ActorRef ): Props = Props { new FirstHourAverageAnalyzer( router ) }


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

    override def addControlBoundary( control: ControlBoundary ): That = {
      copy( underlying = underlying.addControlBoundary(control) )
    }

    def withPoints( points: Seq[DoublePoint] ): Context = {
      val firstHourPoints = {
        points
        .map { _.getPoint }
//        .map { p =>
//          logger.info( "FirstHour.withPoint [{}] in first-hour:[{}] = {}", (new joda.DateTime(p(0).toLong), p(1)), Context.FirstHour, Context.FirstHour.contains(p(0).toLong).toString )
//          p
//        }
        .collect { case Array( ts, v ) if Context.FirstHour contains ts.toLong => v }
      }

      if ( firstHourPoints.nonEmpty ) {
        logger.debug( s"""adding values to first hour: [${firstHourPoints.mkString(",")}]"""  )
        val updated = firstHourPoints.foldLeft( firstHour.copy ) { (s, v) => s.addValue( v ); s }
        logger.debug( s"updated first-hour stats: mean=[${updated.getMean}] stddev=[${updated.getStandardDeviation}]" )
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
    makeFirstHourStatistics( c ) map { firstHour => Context( underlying = c, firstHour = firstHour ) }
  }

  def makeFirstHourStatistics(
    context: AlgorithmContext,
    initialStatistics: Option[SummaryStatistics] = None
  ): Valid[SummaryStatistics] = {
    val firstHourStats = initialStatistics getOrElse { new SummaryStatistics }
    firstHourStats.successNel
  }

  val contextWithFirstHourStats: Op[AlgorithmContext, Context] = toConcreteContextK map { c =>
    log.debug( "first-hour:[{}] context source points: [{}]", Context.FirstHour, c.source.points.mkString(",") )
    c withPoints c.source.points
  }

  /**
    * Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    * A timeseries is anomalous if the average of the last three datapoints
    * are outside of three standard deviations of this value.
    */
  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      ctx <- contextWithFirstHourStats
      tolerance <- tolerance
      taverages <- tailAverage
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = taverages,
        context = ctx,
        evaluateOutlier = (p: Point2D, c: Context) => {
          val (ts, v) = p
          val control = ControlBoundary.fromExpectedAndDistance(
            timestamp = ts.toLong,
            expected = c.firstHour.getMean,
            distance = math.abs( tol * c.firstHour.getStandardDeviation )
          )
          log.debug( "first hour[{}] mean[{}] stdev[{}] tolerance[{}]", c.firstHour.getN, c.firstHour.getMean, c.firstHour.getStandardDeviation, tol )
          log.debug( "first hour[{}] [{}] is-outlier:{} control = [{}]", algorithm.name, v, control.isOutlier(v), control )
          ( control.isOutlier(v), control )
        },
        update = (c: Context, pt: Point2D) => c
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
