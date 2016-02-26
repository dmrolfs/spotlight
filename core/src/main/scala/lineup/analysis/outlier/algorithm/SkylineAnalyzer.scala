package lineup.analysis.outlier.algorithm

import scala.annotation.tailrec
import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import scalaz.{ Lens => _, _ }, Scalaz._
import scalaz.Kleisli.{ ask, kleisli }
import shapeless.{ :: => _, _ }
import shapeless.syntax.typeable._
import org.joda.{ time => joda }
import com.github.nscala_time.time.Imports._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.{DescriptiveStatistics, SummaryStatistics}
import peds.commons.Valid
import lineup.model.outlier.{OutlierPlan, Outliers}
import lineup.model.timeseries._
import lineup.analysis.outlier._
import lineup.analysis.outlier.algorithm.AlgorithmActor._


/**
  * Created by rolfsd on 2/12/16.
  */
object SkylineAnalyzer {
  val FirstHourAverageAlgorithm = 'first_hour_average
  val MeanSubtractionCumulationAlgorithm = 'mean_subtraction_cumulation
  val StddevFromSimpleMovingAverageAlgorithm = 'stddev_from_simple_moving_average
  val StddevFromExponentialMovingAverageAlgorithm = 'stddev_from_exponential_moving_average
  val LeastSquaresAlgorithm = 'least_squares
  val GrubbsAlgorithm = 'grubbs
  val HistogramBinsAlgorithm = 'histogram_bins
  val MedianAbsoluteDeviationAlgorithm = 'median_absolute_deviation
  val KsTestAlgorithm = 'ks_test

  def props( router: ActorRef ): Props = Props { new SkylineAnalyzer( router ) }

  type Op[I, O] = Kleisli[TryV, I, O]
  type Points = Seq[DoublePoint]
  type Point = Array[Double]
  type Point2D = (Double, Double)
  type MomentHistogram = Map[MomentBinKey, Moment]

  object SkylineContext extends LazyLogging {
    import AlgorithmActor.Context

    val firstHour: joda.Interval = new joda.Interval( joda.DateTime.now, 1.hour.toDuration )

    def fromAlgorithmContext( underlying: Context ): Valid[SkylineContext] = {
      (
        makeFirstHourStatistics( underlying )
        |@| makeMovingStatistics( underlying )
        |@| makeDeviationStatistics( underlying )
        |@| makeHistoricalMoment( underlying )
        |@| makeMomentHistogram( underlying )
      ) { (firstHour, moving, deviations, history, histogram) =>
        SkylineContext(
          underlying = underlying,
          firstHour = firstHour,
          movingStatistics = moving,
          deviationStatistics = deviations,
          historicalMoment = history,
          momentHistogram = histogram
        )
      }
    }

    def makeFirstHourStatistics(
      context: Context,
      initialStatistics: Option[SummaryStatistics] = None
    ): Valid[SummaryStatistics] = {
      val stats = initialStatistics getOrElse { new SummaryStatistics }

      context.data
      .map { _.getPoint }
      .filter { case Array(ts, v) => firstHour contains ts.toLong }
      .foreach { case Array(ts, v) =>
        logger.debug( s"adding points to first hour: [(${ts.toLong}, ${v})]" )
        stats addValue v
      }

      stats.successNel
    }

    // window size = 1d @ 1 pt per 10s
    val ApproximateDayWindow: Int = 6 * 60 * 24

    def makeMovingStatistics( context: Context ): Valid[DescriptiveStatistics] = {
      new DescriptiveStatistics( ApproximateDayWindow ).successNel
    }

    def makeDeviationStatistics( context: Context ): Valid[DescriptiveStatistics] = {
      new DescriptiveStatistics( ApproximateDayWindow ).successNel
    }

    def makeHistoricalMoment( context: AlgorithmActor.Context ): Valid[Moment] = {
      Moment.withAlpha( id = context.historyKey.toString, alpha = 0.05 )
    }

    def makeMomentHistogram( context: Context ): Valid[MomentHistogram] = {
      val moments: List[TryV[(MomentBinKey, Moment)]] = for {
        day <- DayOfWeek.JodaDays.values.toList
        hour <- 0 to 23
      } yield {
        val mbkey = MomentBinKey( day, hour )
        Moment.withAlpha( mbkey.id, alpha = 0.05 ).map{ (mbkey, _) }.disjunction.leftMap{ _.head }
      }

      moments.sequenceU.map{ ms => Map( ms:_* ) }.validationNel
    }
  }

  case class SkylineContext private[algorithm](
    val underlying: AlgorithmActor.Context,
    val firstHour: SummaryStatistics,
    val movingStatistics: DescriptiveStatistics,
    val deviationStatistics: DescriptiveStatistics,
    val historicalMoment: Moment,
    val momentHistogram: MomentHistogram
  ) extends AlgorithmActor.Context with LazyLogging {
    override def message: DetectUsing = underlying.message
    override def data: Seq[DoublePoint] = underlying.data
    override def algorithm: Symbol = underlying.algorithm
    override def topic: Topic = underlying.topic
    override def plan: OutlierPlan = underlying.plan
    override def historyKey: HistoryKey = underlying.historyKey
    override def history: HistoricalStatistics = underlying.history
    override def source: TimeSeriesBase = underlying.source
    override def messageConfig: Config = underlying.messageConfig
    override def distanceMeasure: TryV[DistanceMeasure] = underlying.distanceMeasure
    override def tolerance: TryV[Option[Double]] = underlying.tolerance

    def withPoints( points: Seq[DoublePoint] ): SkylineContext = {
      val firstHourPoints = {
        points
        .map { _.getPoint }
        .collect { case Array( ts, v ) if SkylineContext.firstHour contains ts.toLong => v }
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
  }


  val underlyingLens: Lens[SkylineContext, AlgorithmActor.Context] = lens[SkylineContext] >> 'underlying
  val firstHourLens: Lens[SkylineContext, SummaryStatistics] = lens[SkylineContext] >> 'firstHour
  val movingStatisticsLens: Lens[SkylineContext, DescriptiveStatistics] = lens[SkylineContext] >> 'movingStatistics
  val deviationStatisticsLens: Lens[SkylineContext, DescriptiveStatistics] = lens[SkylineContext] >> 'deviationStatistics
  val historicalMomentLens: Lens[SkylineContext, Moment] = lens[SkylineContext] >> 'historicalMoment
  val momentHistogramLens: Lens[SkylineContext, MomentHistogram] = lens[SkylineContext] >> 'momentHistogram


  final case class SkylineContextError private[algorithm]( context: AlgorithmActor.Context )
  extends IllegalStateException( s"Context was not extended for Skyline algorithms: [${context}]" )
}

class SkylineAnalyzer( override val router: ActorRef ) extends AlgorithmActor {
  import SkylineAnalyzer._

  override def preStart(): Unit = {
    context watch router
    supportedAlgorithms.keys foreach { a => router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( a, self ) }
  }

  override def receive: Receive = around( initializing( registered = Set.empty[Symbol], waiting = supportedAlgorithms.keySet ) )

  private[this] var _activeAlgorithm: Symbol = _
  override def algorithm: Symbol = _activeAlgorithm
  def algorithm_=( algorithm: Symbol ): Unit = _activeAlgorithm = algorithm

  def initializing( registered: Set[Symbol], waiting: Set[Symbol] ): Receive = LoggingReceive {
    case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if waiting contains a => {
      val (r, w) = ( registered + a, waiting - a )
      if ( w.isEmpty ) context become detect
      else context become initializing( r, w )
    }

    case m @ DetectUsing( algo, _, _: DetectOutliersInSeries, _, _ ) if registered contains algo => runDetection( m )
  }

  override def detect: Receive = LoggingReceive {
    case m @ DetectUsing( algo, _, _: DetectOutliersInSeries, _, _ ) if supportedAlgorithms contains algo => runDetection( m )
  }

  def runDetection( message: DetectUsing ): Unit = {
    val toOutliers = kleisli[TryV, (Outliers, Context), Outliers] { case (o, _) => o.right }

    \/ fromTryCatchNonFatal {
      supportedAlgorithms( message.algorithm )
    } flatMap { op =>
      ( algorithmContext >=> op >=> toOutliers ) run message
    } match {
      case \/-( r ) => message.aggregator ! r
      case -\/( ex ) => {
        log.error(
          ex,
          "failed [{}] analysis on [{}] @ [{}]",
          message.algorithm.name,
          message.payload.plan.name + "][" + message.payload.topic,
          message.payload.source.interval
        )
      }
    }
  }


  var _scopedContexts: Map[HistoryKey, SkylineContext] = Map.empty[HistoryKey, SkylineContext]

  def setScopedContext( c: SkylineContext ): Unit = { _scopedContexts += c.historyKey -> c }

  override val algorithmContext: Op[DetectUsing, Context] = {
    val toSkyline = kleisli[TryV, Context, Context] { c =>
      _scopedContexts
      .get( c.historyKey )
      .map { priorContext =>
        SkylineContext.makeFirstHourStatistics( c, Option(priorContext.firstHour) )
        .disjunction
        .leftMap{ _.head }
        .map { firstHour => priorContext.copy( underlying = c, firstHour = firstHour ) }
      }
      .getOrElse {
        val context = SkylineContext.fromAlgorithmContext( c )
        context foreach { setScopedContext }
        context.disjunction.leftMap{ _.head }
      }
    }

    super.algorithmContext >=> toSkyline
  }

  val toSkylineContext: Op[Context, SkylineContext] = {
    kleisli { context =>
      context match {
        case ctx: SkylineContext => {
          ctx.right
        }

        case ctx => SkylineContextError( ctx ).left
      }
    }
  }

  val tailAverage: Op[Context, Seq[Point2D]] = Kleisli[TryV, Context, Seq[Point2D]] { context =>
    val data = context.data.map{ _.getPoint.apply( 1 ) }
    val last = context.history.lastPoints.drop( context.history.lastPoints.size - 2 ) map { case Array(_, v) => v }
    log.debug( "tail-average: last=[{}]", last.mkString(",") )

    val TailLength = 3

    context.data
    .map{ _.getPoint.apply( 0 ) }
    .zipWithIndex
    .map { case (ts, i) =>
      val pointsToAverage = if ( i < TailLength ) {
        val all = last ++ data.take( i + 1 )
        all.drop( all.size - TailLength )
      } else {
        data.drop( i - TailLength + 1 ).take( TailLength )
      }

      ( ts, pointsToAverage )
    }
    .map { case (ts, pts) =>
      log.debug( "points to tail average ({}, [{}]) = {}", ts.toLong, pts.mkString(","), pts.sum / pts.size )
      (ts, pts.sum / pts.size)
    }
    .right
  }


  /**
    * Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    * A timeseries is anomalous if the average of the last three datapoints
    * are outside of three standard deviations of this value.
    */
  val firstHourAverage: Op[Context, (Outliers, Context)] = {
    val outliers: Op[Context, (Row[DataPoint], Context)] = for {
      context <- toSkylineContext <=< ask[TryV, Context]
      tolerance <- tolerance <=< ask[TryV, Context]
      taverages <- tailAverage <=< ask[TryV, Context]
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = taverages,
        context = context,
        isOutlier = (p: Point2D, ctx: SkylineContext) => {
          val (_, v) = p
          val mean = ctx.firstHour.getMean
          val stddev = ctx.firstHour.getStandardDeviation
          log.debug( "first hour mean[{}] and stdev[{}]", mean, stddev )
          math.abs( v - mean ) > ( tol * stddev)
        },
        update = (ctx: SkylineContext, pt: DataPoint) => ctx
      )
    }

    makeOutliersK( FirstHourAverageAlgorithm, outliers )
  }

  val meanSubtractionCumulation: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }

  /**
    * A timeseries is anomalous if the absolute value of the average of the latest
    * three datapoint minus the moving average is greater than tolerance * standard
    * deviations of the average. This does not exponentially weight the moving average
    * and so is better for detecting anomalies with respect to the entire series.
    */
  val stddevFromSimpleMovingAverage: Op[Context, (Outliers, Context)] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, Context]
      tolerance <- tolerance <=< ask[TryV, Context]
      taverages <- tailAverage <=< ask[TryV, Context]
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = taverages,
        context = context,
        isOutlier = (p: Point2D, ctx: SkylineContext) => {
          val (_, v) = p
          val mean = ctx.movingStatistics.getMean
          val stddev = ctx.movingStatistics.getStandardDeviation
          log.debug( "Stddev from simple moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]", mean, stddev, tol )
          math.abs( v - mean ) > ( tol * stddev)
        },
        update = (ctx: SkylineContext, pt: DataPoint) => {
          ctx.movingStatistics addValue pt.value
          ctx
        }
      )
    }

    makeOutliersK( StddevFromSimpleMovingAverageAlgorithm, outliers )
  }

  /**
    * A timeseries is anomalous if the absolute value of the average of the latest
    * three datapoint minus the moving average is greater than tolerance * standard
    * deviations of the exponential moving average. This is better for finding anomalies
    * with respect to the short term trends.
    */
  val stddevFromExponentialMovingAverage: Op[Context, (Outliers, Context)] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, Context]
      tolerance <- tolerance <=< ask[TryV, Context]
      taverages <- tailAverage <=< ask[TryV, Context]
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = taverages,
        context = context,
        isOutlier = (p: Point2D, ctx: SkylineContext) => {
          ctx.historicalMoment.statistics map { stats =>
            val (ts, v) = p
            log.debug( "pt:[{}] - Stddev from exponential moving Average: mean[{}]\tstdev[{}]\ttolerance[{}]", (ts.toLong, v), stats.ewma, stats.ewmsd, tol )
            math.abs( v - stats.ewma ) > ( tol * stats.ewmsd )
          } getOrElse {
            false
          }
        },
        update = (ctx: SkylineContext, pt: DataPoint) => {
          log.debug( "stddevFromMovingAverage: adding point ({}, {}) to historical moment: [{}]", pt.timestamp.getMillis, pt.value, context.historicalMoment.statistics )
          historicalMomentLens.set( ctx )( ctx.historicalMoment :+ pt.value )
        }
      )
    }

    makeOutliersK( StddevFromExponentialMovingAverageAlgorithm, outliers )
  }

  val leastSquares: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }

  /**
    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
    */
  val grubbs: Op[Context, (Outliers, Context)] = {
    // background: http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
    // background: http://graphpad.com/support/faqid/1598/
    val outliers: Op[Context, (Row[DataPoint], Context)] = for {
      context <- ask[TryV, Context]
      taverages <- tailAverage <=< ask[TryV, Context]
    } yield {
      val data = taverages.map{ case (_, v) => v }.toArray
      val stats = new DescriptiveStatistics( data )
      val stddev = stats.getStandardDeviation
      val mean = stats.getMean
      val zScores = taverages map { case (ts, v) => ( ts, math.abs(v - mean) / stddev ) }
      log.debug( "Skyline[Grubbs]: mean:[{}] stddev:[{}] zScores:[{}]", mean, stddev, zScores.mkString(",") )

      val Alpha = 0.05
      val threshold = new TDistribution( data.size - 2 ).inverseCumulativeProbability( Alpha / ( 2D * data.size ) )
      val thresholdSquared = math.pow( threshold, 2 )
      log.debug( "Skyline[Grubbs]: threshold^2:[{}]", thresholdSquared )
      val grubbsScore = ((data.size - 1) / math.sqrt(data.size)) * math.sqrt( thresholdSquared / (data.size - 2 + thresholdSquared) )
      log.debug( "Skyline[Grubbs]: Grubbs Score:[{}]", grubbsScore )

      collectOutlierPoints(
        points = zScores,
        context = context,
        isOutlier = (p: Point2D, ctx: Context) => { p._2 > grubbsScore },
        update = (ctx: Context, pt: DataPoint) => { ctx }
      )
    }

    makeOutliersK( GrubbsAlgorithm, outliers )
  }

  val histogramBins: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }

  /**
    * A timeseries is anomalous if the deviation of its latest datapoint with
    * respect to the median is [tolerance] times larger than the median of deviations.
    */
  val medianAbsoluteDeviation: Op[Context, (Outliers, Context)] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, Context]
      tolerance <- tolerance <=< ask[TryV, Context]
    } yield {
      val tol = tolerance getOrElse 3D  // skyline source uses 6.0 - admittedly arbitrary?

      def deviation( value: Double, ctx: SkylineContext ): Double = {
        val movingMedian = ctx.movingStatistics getPercentile 50
        log.debug( "medianAbsoluteDeviation: N:[{}] movingMedian:[{}]", ctx.deviationStatistics.getN, movingMedian )
        math.abs( value - movingMedian )
      }

      collectOutlierPoints(
        points = context.data.map{ _.getPoint }.map{ case Array(ts, v) => (ts, v) },
        context = context,
        isOutlier = (p: Point2D, ctx: SkylineContext) => {
          val (ts, v) = p
          val d = deviation( v, ctx )
          val deviationMedian = ctx.deviationStatistics getPercentile 50
          log.debug( "medianAbsoluteDeviation: N:[{}] deviation:[{}] deviationMedian:[{}]", ctx.deviationStatistics.getN, d, deviationMedian )
          d > ( tol * deviationMedian )
        },
        update = (ctx: SkylineContext, dp: DataPoint) => {
          ctx.movingStatistics addValue dp.value
          ctx.deviationStatistics addValue deviation( dp.value, ctx )
          ctx
        }
      )
    }

    makeOutliersK( MedianAbsoluteDeviationAlgorithm, outliers )
  }

  val ksTest: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }


  type UpdateContext[C <: Context] = (C, DataPoint) => C
  type IsOutlier[C <: Context] = (Point2D, C) => Boolean

  def collectOutlierPoints[C <: Context](
    points: Seq[Point2D],
    context: C,
    isOutlier: IsOutlier[C],
    update: UpdateContext[C]
  ): (Row[DataPoint], Context) = {
    @tailrec def loop( pts: List[Point2D], ctx: C, acc: Row[DataPoint] ): (Row[DataPoint], Context) = {
      ctx.cast[SkylineContext] foreach { setScopedContext }

      log.debug( "{} checking pt [{}] for outlier = {}", ctx.algorithm, pts.headOption.map{p=>(p._1.toLong, p._2)}, pts.headOption.map{ p => isOutlier(p,ctx) } )
      pts match {
        case Nil => ( acc, ctx )

        case h :: tail if isOutlier( h, ctx ) => {
          val (ts, _) = h
          val original = ctx.source.points find { _.timestamp.getMillis == ts.toLong }
          val (updatedContext, updatedAcc) = original map { orig =>
            ( update(ctx, orig), acc :+ orig  )
          } getOrElse {
            (ctx, acc)
          }

          log.debug( "LOOP-HIT[({})]: updated context-stats=[{}] acc=[{}]", (ts.toLong, h._2), updatedContext.asInstanceOf[SkylineContext].historicalMoment.statistics, updatedAcc )
          loop( tail, updatedContext, updatedAcc )
        }

        case h :: tail => {
          val (ts, _) = h
          val updatedContext = {
            ctx.source.points
            .find { _.timestamp.getMillis == ts.toLong }
            .map { orig => update( ctx, orig ) }
            .getOrElse { ctx }
          }

          log.debug( "LOOP-MISS[({})]: updated context-stats=[{}] acc=[{}]", (ts.toLong, h._2), updatedContext.asInstanceOf[SkylineContext].historicalMoment.statistics, acc )
          loop( tail, updatedContext, acc )
        }
      }
    }

    loop( points.toList, context, Row.empty[DataPoint] )
  }

  def makeOutliersK( algorithm: Symbol, outliers: Op[Context, (Row[DataPoint], Context)] ): Op[Context, (Outliers, Context)] = {
    def makeOutliers( os: Row[DataPoint] ): Op[Context, Outliers] = {
      kleisli[TryV, Context, Outliers] { context =>
        Outliers.forSeries(
          algorithms = Set( algorithm ),
          plan = context.plan,
          source = context.source,
          outliers = os
        )
        .disjunction
        .leftMap { _.head }
      }
    }

    for {
      outliersContext <- outliers
      (outlierPoints, context) = outliersContext
      result <- makeOutliers( outlierPoints )
    } yield (result, context)
  }

  val supportedAlgorithms: Map[Symbol, Op[Context, (Outliers, Context)]] = {
    Map(
      FirstHourAverageAlgorithm -> firstHourAverage,
      MeanSubtractionCumulationAlgorithm -> meanSubtractionCumulation,
      StddevFromSimpleMovingAverageAlgorithm -> stddevFromSimpleMovingAverage,
      StddevFromExponentialMovingAverageAlgorithm -> stddevFromExponentialMovingAverage,
      LeastSquaresAlgorithm -> leastSquares,
      GrubbsAlgorithm -> grubbs,
      HistogramBinsAlgorithm -> histogramBins,
      MedianAbsoluteDeviationAlgorithm -> medianAbsoluteDeviation,
      KsTestAlgorithm -> ksTest
    )
  }
}
