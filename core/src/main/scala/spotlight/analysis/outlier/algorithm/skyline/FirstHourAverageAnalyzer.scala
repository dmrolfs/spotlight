package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ ActorRef, Props }
import scalaz._, Scalaz._
import scalaz.Kleisli.ask
import org.joda.{ time => joda }
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import peds.commons.Valid
import peds.commons.util._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{ AlgorithmContext, Op, Point2D, TryV }
import spotlight.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.DataPoint


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
  ) extends SkylineContext with LazyLogging {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[SkylineContext] = copy( underlying = ctx ).successNel

    def withPoints( points: Seq[DoublePoint] ): Context = {
      val firstHourPoints = {
        points
        .map { _.getPoint }
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

class FirstHourAverageAnalyzer( override val router: ActorRef ) extends SkylineAnalyzer[FirstHourAverageAnalyzer.Context] {
  import FirstHourAverageAnalyzer._

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = FirstHourAverageAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = {
    makeFirstHourStatistics( c ) map { firstHour => Context( underlying = c, firstHour = firstHour ) }
  }

  override def preStartContext( context: AlgorithmContext, priorContext: SkylineContext ): TryV[SkylineContext] = {
    for {
      sctx <- super.preStartContext( context, priorContext )
      fhctx <- toConcreteContext( sctx )
    } yield fhctx withPoints fhctx.data
  }

  def makeFirstHourStatistics(
    context: AlgorithmContext,
    initialStatistics: Option[SummaryStatistics] = None
  ): Valid[SummaryStatistics] = {
    val firstHourStats = initialStatistics getOrElse { new SummaryStatistics }

    context.data
    .map { _.getPoint }
    .filter { case Array(ts, v) => Context.FirstHour contains ts.toLong }
    .foreach { case Array(ts, v) =>
      log.debug( "adding points to first hour: [({}, {})]", ts.toLong, v )
      firstHourStats addValue v
    }

    firstHourStats.successNel
  }


  /**
    * Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    * A timeseries is anomalous if the average of the last three datapoints
    * are outside of three standard deviations of this value.
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
          val mean = ctx.firstHour.getMean
          val stddev = ctx.firstHour.getStandardDeviation
          log.debug( "first hour mean[{}] and stdev[{}]", mean, stddev )
          math.abs( v - mean ) > ( tol * stddev)
        },
        update = (ctx: Context, pt: DataPoint) => ctx  //todo update with point for first hour?
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
