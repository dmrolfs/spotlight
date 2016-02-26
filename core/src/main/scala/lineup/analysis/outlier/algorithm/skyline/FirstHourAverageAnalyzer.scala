package lineup.analysis.outlier.algorithm.skyline

//import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import org.joda.{time => joda}
import com.typesafe.scalalogging.LazyLogging
import lineup.analysis.outlier.algorithm.AlgorithmActor
import lineup.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, Point2D, TryV}
import lineup.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import lineup.model.outlier.Outliers
import lineup.model.timeseries.{DataPoint, Row}
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import peds.commons.Valid
import peds.commons.util._


/**
  * Created by rolfsd on 2/25/16.
  */
object FirstHourAverageAnalyzer {
  val Algorithm = 'first_hour_average

  def props( router: ActorRef ): Props = Props { new FirstHourAverageAnalyzer( router ) }

//  object SkylineContext extends LazyLogging {
//    import AlgorithmActor.Context
//
//
//    def fromAlgorithmContext( underlying: Context ): Valid[SkylineContext] = {
//      (
//        makeFirstHourStatistics( underlying )
//          |@| makeMovingStatistics( underlying )
//          |@| makeDeviationStatistics( underlying )
//          |@| makeHistoricalMoment( underlying )
//          |@| makeMomentHistogram( underlying )
//        ) { (firstHour, moving, deviations, history, histogram) =>
//        SkylineContext(
//                        underlying = underlying,
//                        firstHour = firstHour,
//                        movingStatistics = moving,
//                        deviationStatistics = deviations,
//                        historicalMoment = history,
//                        momentHistogram = histogram
//                      )
//      }
//    }
//
//
//    // window size = 1d @ 1 pt per 10s
//    val ApproximateDayWindow: Int = 6 * 60 * 24
//
//    def makeMovingStatistics( context: Context ): Valid[DescriptiveStatistics] = {
//      new DescriptiveStatistics( ApproximateDayWindow ).successNel
//    }
//
//    def makeDeviationStatistics( context: Context ): Valid[DescriptiveStatistics] = {
//      new DescriptiveStatistics( ApproximateDayWindow ).successNel
//    }
//
//    def makeHistoricalMoment( context: AlgorithmActor.Context ): Valid[Moment] = {
//      Moment.withAlpha( id = context.historyKey.toString, alpha = 0.05 )
//    }
//
//    def makeMomentHistogram( context: Context ): Valid[MomentHistogram] = {
//      val moments: List[TryV[(MomentBinKey, Moment)]] = for {
//        day <- DayOfWeek.JodaDays.values.toList
//        hour <- 0 to 23
//      } yield {
//        val mbkey = MomentBinKey( day, hour )
//        Moment.withAlpha( mbkey.id, alpha = 0.05 ).map{ (mbkey, _) }.disjunction.leftMap{ _.head }
//      }
//
//      moments.sequenceU.map{ ms => Map( ms:_* ) }.validationNel
//    }
//  }
//

  object FirstHourContext {
    import com.github.nscala_time.time.Imports._
    val FirstHour: joda.Interval = new joda.Interval( joda.DateTime.now, 1.hour.toDuration )
  }

  final case class FirstHourContext private[skyline](
    override val underlying: AlgorithmContext,
    firstHour: SummaryStatistics
  ) extends SkylineContext with LazyLogging {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[SkylineContext] = copy( underlying = ctx ).successNel

    def withPoints( points: Seq[DoublePoint] ): FirstHourContext = {
      val firstHourPoints = {
        points
        .map { _.getPoint }
        .collect { case Array( ts, v ) if FirstHourContext.FirstHour contains ts.toLong => v }
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
      s"""${getClass.safeSimpleName}(firstHour-mean:[${firstHour.getMean}] firstHour-StdDev:[${firstHour.getStandardDeviation}])"""
    }
  }
}

class FirstHourAverageAnalyzer(
  override val router: ActorRef
) extends SkylineAnalyzer[FirstHourAverageAnalyzer.FirstHourContext] {
  import SkylineAnalyzer._
  import FirstHourAverageAnalyzer._

  override implicit val contextClassTag: ClassTag[FirstHourContext] = ClassTag( classOf[FirstHourContext])

  override def algorithm: Symbol = FirstHourAverageAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = {
    makeFirstHourStatistics( c ) map { firstHour => FirstHourContext( underlying = c, firstHour = firstHour ) }
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
    .filter { case Array(ts, v) => FirstHourContext.FirstHour contains ts.toLong }
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
    val outliers: Op[AlgorithmContext, (Row[DataPoint], AlgorithmContext)] = for {
      context <- toSkylineContext <=< ask[TryV, AlgorithmContext]
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
      taverages <- tailAverage <=< ask[TryV, AlgorithmContext]
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = taverages,
        context = context,
        isOutlier = (p: Point2D, ctx: FirstHourContext) => {
          val (_, v) = p
          val mean = ctx.firstHour.getMean
          val stddev = ctx.firstHour.getStandardDeviation
          log.debug( "first hour mean[{}] and stdev[{}]", mean, stddev )
          math.abs( v - mean ) > ( tol * stddev)
        },
        update = (ctx: FirstHourContext, pt: DataPoint) => ctx  //todo update with point for first hour?
      )
    }

    makeOutliersK( algorithm, outliers )
  }



}





//  val meanSubtractionCumulation: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }
//
//  /**
//    * A timeseries is anomalous if the absolute value of the average of the latest
//    * three datapoint minus the moving average is greater than tolerance * standard
//    * deviations of the average. This does not exponentially weight the moving average
//    * and so is better for detecting anomalies with respect to the entire series.
//    */
//  val stddevFromSimpleMovingAverage: Op[Context, (Outliers, Context)] = {
//    val outliers = for {
//      context <- toSkylineContext <=< ask[TryV, Context]
//      tolerance <- tolerance <=< ask[TryV, Context]
//      taverages <- tailAverage <=< ask[TryV, Context]
//    } yield {
//      val tol = tolerance getOrElse 3D
//
//      collectOutlierPoints(
//        points = taverages,
//        context = context,
//        isOutlier = (p: Point2D, ctx: SkylineContext) => {
//          val (_, v) = p
//          val mean = ctx.movingStatistics.getMean
//          val stddev = ctx.movingStatistics.getStandardDeviation
//          log.debug( "Stddev from simple moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]", mean, stddev, tol )
//          math.abs( v - mean ) > ( tol * stddev)
//        },
//        update = (ctx: SkylineContext, pt: DataPoint) => {
//          ctx.movingStatistics addValue pt.value
//          ctx
//        }
//      )
//    }
//
//    makeOutliersK( StddevFromSimpleMovingAverageAlgorithm, outliers )
//  }
//
//  /**
//    * A timeseries is anomalous if the absolute value of the average of the latest
//    * three datapoint minus the moving average is greater than tolerance * standard
//    * deviations of the exponential moving average. This is better for finding anomalies
//    * with respect to the short term trends.
//    */
//  val stddevFromExponentialMovingAverage: Op[Context, (Outliers, Context)] = {
//    val outliers = for {
//      context <- toSkylineContext <=< ask[TryV, Context]
//      tolerance <- tolerance <=< ask[TryV, Context]
//      taverages <- tailAverage <=< ask[TryV, Context]
//    } yield {
//      val tol = tolerance getOrElse 3D
//
//      collectOutlierPoints(
//        points = taverages,
//        context = context,
//        isOutlier = (p: Point2D, ctx: SkylineContext) => {
//          ctx.historicalMoment.statistics map { stats =>
//            val (ts, v) = p
//            log.debug( "pt:[{}] - Stddev from exponential moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]", (ts.toLong, v), stats.ewma, stats.ewmsd, tol )
//            math.abs( v - stats.ewma ) > ( tol * stats.ewmsd )
//          } getOrElse {
//            false
//          }
//        },
//        update = (ctx: SkylineContext, pt: DataPoint) => {
//          log.debug( "stddevFromMovingAverage: adding point ({}, {}) to historical moment: [{}]", pt.timestamp.getMillis, pt.value, context.historicalMoment.statistics )
//          historicalMomentLens.set( ctx )( ctx.historicalMoment :+ pt.value )
//        }
//      )
//    }
//
//    makeOutliersK( StddevFromExponentialMovingAverageAlgorithm, outliers )
//  }
//
//  val leastSquares: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }
//
//  /**
//    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
//    */
//  val grubbs: Op[Context, (Outliers, Context)] = {
//    // background: http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
//    // background: http://graphpad.com/support/faqid/1598/
//    val outliers: Op[Context, (Row[DataPoint], Context)] = for {
//      context <- ask[TryV, Context]
//      taverages <- tailAverage <=< ask[TryV, Context]
//    } yield {
//      val data = taverages.map{ case (_, v) => v }.toArray
//      val stats = new DescriptiveStatistics( data )
//      val stddev = stats.getStandardDeviation
//      val mean = stats.getMean
//      val zScores = taverages map { case (ts, v) => ( ts, math.abs(v - mean) / stddev ) }
//      log.debug( "Skyline[Grubbs]: mean:[{}] stddev:[{}] zScores:[{}]", mean, stddev, zScores.mkString(",") )
//
//      val Alpha = 0.05
//      val threshold = new TDistribution( data.size - 2 ).inverseCumulativeProbability( Alpha / ( 2D * data.size ) )
//      val thresholdSquared = math.pow( threshold, 2 )
//      log.debug( "Skyline[Grubbs]: threshold^2:[{}]", thresholdSquared )
//      val grubbsScore = ((data.size - 1) / math.sqrt(data.size)) * math.sqrt( thresholdSquared / (data.size - 2 + thresholdSquared) )
//      log.debug( "Skyline[Grubbs]: Grubbs Score:[{}]", grubbsScore )
//
//      collectOutlierPoints(
//        points = zScores,
//        context = context,
//        isOutlier = (p: Point2D, ctx: Context) => { p._2 > grubbsScore },
//        update = (ctx: Context, pt: DataPoint) => { ctx }
//      )
//    }
//
//    makeOutliersK( GrubbsAlgorithm, outliers )
//  }
//
//  val histogramBins: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }
//
//  /**
//    * A timeseries is anomalous if the deviation of its latest datapoint with
//    * respect to the median is [tolerance] times larger than the median of deviations.
//    */
//  val medianAbsoluteDeviation: Op[Context, (Outliers, Context)] = {
//    val outliers = for {
//      context <- toSkylineContext <=< ask[TryV, Context]
//      tolerance <- tolerance <=< ask[TryV, Context]
//    } yield {
//      val tol = tolerance getOrElse 3D  // skyline source uses 6.0 - admittedly arbitrary?
//
//      def deviation( value: Double, ctx: SkylineContext ): Double = {
//        val movingMedian = ctx.movingStatistics getPercentile 50
//        log.debug( "medianAbsoluteDeviation: N:[{}] movingMedian:[{}]", ctx.deviationStatistics.getN, movingMedian )
//        math.abs( value - movingMedian )
//      }
//
//      collectOutlierPoints(
//        points = context.data.map{ _.getPoint }.map{ case Array(ts, v) => (ts, v) },
//        context = context,
//        isOutlier = (p: Point2D, ctx: SkylineContext) => {
//          val (ts, v) = p
//          val d = deviation( v, ctx )
//          val deviationMedian = ctx.deviationStatistics getPercentile 50
//          log.debug( "medianAbsoluteDeviation: N:[{}] deviation:[{}] deviationMedian:[{}]", ctx.deviationStatistics.getN, d, deviationMedian )
//          d > ( tol * deviationMedian )
//        },
//        update = (ctx: SkylineContext, dp: DataPoint) => {
//          ctx.movingStatistics addValue dp.value
//          ctx.deviationStatistics addValue deviation( dp.value, ctx )
//          ctx
//        }
//      )
//    }
//
//    makeOutliersK( MedianAbsoluteDeviationAlgorithm, outliers )
//  }
//
//  val ksTest: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }

