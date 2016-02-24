package lineup.analysis.outlier.algorithm

import scala.annotation.tailrec
import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import scalaz.{ Lens => _, _ }, Scalaz._
import scalaz.Kleisli.{ ask, kleisli }
import shapeless.Lens
import org.joda.{ time => joda }
import com.github.nscala_time.time.Imports._
import com.typesafe.config.Config
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.{DescriptiveStatistics, MultivariateSummaryStatistics, SummaryStatistics}
import lineup.model.outlier.{OutlierPlan, Outliers}
import lineup.model.timeseries._
import lineup.analysis.outlier._
import lineup.analysis.outlier.algorithm.AlgorithmActor.TryV


/**
  * Created by rolfsd on 2/12/16.
  */
object SkylineAnalyzer {
  val FirstHourAverageAlgorithm = 'first_hour_average
  val MeanSubtractionCumulationAlgorithm = 'mean_subtraction_cumulation
  val StddevFromAverageAlgorithm = 'stddev_from_average
  val StddevFromMovingAverageAlgorithm = 'stddev_from_moving_average
  val LeastSquaresAlgorithm = 'least_squares
  val GrubbsAlgorithm = 'grubbs
  val HistogramBinsAlgorithm = 'histogram_bins
  val MedianAbsoluteDeviationAlgorithm = 'median_absolute_deviation
  val KsTestAlgorithm = 'ks_test

  def props( router: ActorRef ): Props = Props { new SkylineAnalyzer( router ) }


  type MomentHistogram = Map[MomentBinKey, Moment]

  class SkylineContext(
    private[algorithm] val underlying: AlgorithmActor.Context,
    val firstHour: SummaryStatistics,
    val movingStatistics: DescriptiveStatistics,
    val historicalMoment: Moment,
    val momentHistogram: MomentHistogram
  ) extends AlgorithmActor.Context {
    override def message: DetectUsing = underlying.message
    override def data: Seq[DoublePoint] = underlying.data
    override def algorithm: Symbol = underlying.algorithm
    override def plan: OutlierPlan = underlying.plan
    override def history: HistoricalStatistics = underlying.history
    override def source: TimeSeriesBase = underlying.source
    override def messageConfig: Config = underlying.messageConfig
    override def distanceMeasure: TryV[DistanceMeasure] = underlying.distanceMeasure
    override def tolerance: TryV[Option[Double]] = underlying.tolerance
  }


  val historicalMomentLens: Lens[SkylineContext, Moment] = new Lens[SkylineContext, Moment] {
    override def get( c: SkylineContext ): Moment = c.historicalMoment
    override def set( c: SkylineContext )( m: Moment ): SkylineContext = {
      new SkylineContext(
        underlying = c.underlying,
        firstHour = c.firstHour,
        movingStatistics = c.movingStatistics,
        historicalMoment = m,
        momentHistogram = c.momentHistogram
      )
    }
  }


  final case class SkylineContextError private[algorithm]( context: AlgorithmActor.Context )
  extends IllegalStateException( s"Context was not extended for Skyline algorithms: [${context}]" )
}

class SkylineAnalyzer( override val router: ActorRef ) extends AlgorithmActor {
  import SkylineAnalyzer._
  import AlgorithmActor._

  type TryV[T] = \/[Throwable, T]
  type Op[I, O] = Kleisli[TryV, I, O]
  type Points = Seq[DoublePoint]
  type Point = Array[Double]
  type Point2D = (Double, Double)


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


  val firstHour: joda.Interval = new joda.Interval( joda.DateTime.now, 1.hour.toDuration )
  var _firstHourHistory: Map[HistoryKey, SummaryStatistics] = Map.empty[HistoryKey, SummaryStatistics]

  val updateFirstHourHistory: Op[Context, SummaryStatistics] = Kleisli[TryV, Context, SummaryStatistics] { context =>
    val key = HistoryKey( context.plan, context.source.topic )

    val points = context.data map { _.getPoint } collect { case Array(ts, v) if firstHour contains ts.toLong => v }
    val initial = _firstHourHistory get key map { _.copy } getOrElse { new SummaryStatistics() }

    val result = {
      if ( points.nonEmpty ) {
        log.debug( "adding timestamps to first hour: [{}]", points.mkString(",") )
        val updated = points.foldLeft( initial ) { (h, v) => h.addValue( v ); h }
        log.debug( "updated first-hour stats: mean=[{}] stddev=[{}]", updated.getMean, updated.getStandardDeviation )
        _firstHourHistory += key -> updated
        updated
      } else {
        initial
      }
    }

    result.copy.right
  }

  var _movingStatistics: Map[HistoryKey, DescriptiveStatistics] = Map.empty[HistoryKey, DescriptiveStatistics]
  val movingStatisticsK: Op[Context, DescriptiveStatistics] = Kleisli[TryV, Context, DescriptiveStatistics] { context =>
    movingStatistics( HistoryKey(context.plan, context.source.topic) ).right
  }

  def movingStatistics( key: HistoryKey ): DescriptiveStatistics = {
    _movingStatistics
    .get( key )
    .getOrElse{
      val stats = new DescriptiveStatistics( 6 * 60 * 24 ) // window size = 1d @ 1 pt per 10s
      _movingStatistics += key -> stats
      stats
    }
  }

  var _historicalMoments: Map[HistoryKey, Moment] = Map.empty[HistoryKey, Moment]

//  def historicalMoment( data: TimeSeriesBase, plan: OutlierPlan ): TryV[(HistoryKey, Moment)] = {
  def historicalMoment( data: TimeSeriesBase, plan: OutlierPlan ): TryV[Moment] = {
    val key = HistoryKey( plan, data.topic )
    _historicalMoments
    .get( key )
    .map{ _.right }
    .getOrElse {
      Moment.withAlpha( id = s"${plan.name}:${data.topic}", alpha = 0.05 ).disjunction.leftMap{ _.head }
    }
//    .map { (key, _) }
  }

//  def updateMoment( moment: Moment, points: Seq[DataPoint] ): Moment = points.foldLeft( moment ) { case (m, dp) => m :+ dp.value }

  var _momentHistograms: Map[HistoryKey, MomentHistogram] = Map.empty[HistoryKey, MomentHistogram]

  def momentHistogram( data: TimeSeriesBase, plan: OutlierPlan ): TryV[MomentHistogram] = {
    def initMomentHistogram( hkey: HistoryKey ): TryV[MomentHistogram] = {
      val moments: List[TryV[(MomentBinKey, Moment)]] = for {
        day <- DayOfWeek.JodaDays.values.toList
        hour <- 0 to 23
      } yield {
        val mbkey = MomentBinKey( day, hour )
        Moment.withAlpha( mbkey.id, alpha = 0.05 ).map{ (mbkey, _) }.disjunction.leftMap{ _.head }
      }

      moments.sequenceU map { ms => Map( ms:_* ) }
    }

    val hkey = HistoryKey( plan, data.topic )
    _momentHistograms.get( hkey ).map{ _.right }.getOrElse{ initMomentHistogram( hkey ) }
  }


  def groupPointsByMomentKey(data: TimeSeriesBase, plan: OutlierPlan ): TryV[Map[MomentBinKey, Seq[DataPoint]]] = {
    val groups = data.points.groupBy { case DataPoint(ts, v) =>
      DayOfWeek
      .fromJodaKey( ts.dayOfWeek.get ).disjunction.leftMap { _.head }
      .map { day => MomentBinKey( dayOfWeek = day, hourOfDay = ts.hourOfDay.get ) }
    }
    .toList
    .map { case (key, pts) => key map { k => (k, pts) } }
    .sequenceU

    groups map { gs => Map( gs:_* ) }
  }

  def dataBinKeys( binData: Map[MomentBinKey, Seq[DataPoint]] ): Map[DataPoint, MomentBinKey] = {
    Map( binData.toList.flatMap{ case (key, points) => points map { p => ( p, key ) } }:_* )
  }

  val momentInfoK: Op[Context, (Moment, MomentHistogram)] = kleisli[TryV, Context, (Moment, MomentHistogram)] { context =>
    momentThenUpdate( context.source, context.plan )
  }

  def momentThenUpdate( data: TimeSeriesBase, plan: OutlierPlan ): TryV[(Moment, MomentHistogram)] = {
    val hkey = HistoryKey( plan, data.topic )

    for {
      history <- historicalMoment( data, plan )
      histogram <- momentHistogram( data, plan )
//      groups <- groupPointsByMomentKey( data, plan )
//      dataKeys = dataBinKeys( groups )
    } yield {
//WORK HERE DMR
//      val (updatedHistory, updatedHistogram) = data.points.foldLeft( (history, histogram) ) { case ( (hist, gram), pt ) =>
//        val mbkey: MomentBinKey = dataKeys( pt )
//        val moment: Moment = gram( mbkey )
//        val updatedMoment = moment :+ pt.value
//        (
//          hist :+ pt.value,
//          gram + (mbkey -> updatedMoment)
//        )
//      }
//
//      _historicalMoments += hkey -> updatedHistory
//      _momentHistograms += hkey -> updatedHistogram

      ( history, histogram )
    }
  }


  override val algorithmContext: Op[DetectUsing, Context] = {
    for {
      context <- super.algorithmContext
      firstHourHistory <- updateFirstHourHistory <=< super.algorithmContext
      movingStats <- movingStatisticsK <=< super.algorithmContext
      momentInfo <- momentInfoK <=< super.algorithmContext
      (history, histogram) = momentInfo
    } yield {
      log.debug( "algorithmContext: first hour history: [{}]", firstHourHistory )
      log.debug( "algorithmContext: moving stats: [{}]", movingStats )
      log.debug( "algorithmContext: moment history: [{}]", history )
      log.debug( "algorithmContext: moment histogram: [{}]", histogram )

      new SkylineContext(
        underlying = context,
        firstHour = firstHourHistory,
        movingStatistics = movingStats,
        historicalMoment = history,
        momentHistogram = histogram
      )
    }
  }

  val toSkylineContext: Op[Context, SkylineContext] = {
    kleisli { context =>
      context match {
        case ctx: SkylineContext => ctx.right
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
      val setToAverage = if ( i < TailLength ) {
        val all = last ++ data.take( i + 1 )
        all.drop( all.size - TailLength )
      } else {
        data.drop( i - TailLength + 1 ).take( TailLength )
      }

      ( ts, setToAverage )
    }
    .map { case (ts, pts) =>
      log.debug( "sets to tail average ({}, [{}]) = {}", ts.toLong, pts.mkString(","), pts.sum / pts.size )
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
  val stddevFromAverage: Op[Context, (Outliers, Context)] = {
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

    makeOutliersK( StddevFromAverageAlgorithm, outliers )
  }

  /**
    * A timeseries is anomalous if the absolute value of the average of the latest
    * three datapoint minus the moving average is greater than tolerance * standard
    * deviations of the moving average. This is better for finding anomalies with
    * respect to the short term trends.
    */
  val stddevFromMovingAverage: Op[Context, (Outliers, Context)] = {
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

    makeOutliersK( StddevFromMovingAverageAlgorithm, outliers )
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
  val medianAbsoluteDeviation: Op[Context, (Outliers, Context)] = Kleisli[TryV, Context, (Outliers, Context)] { context => -\/( new IllegalStateException("tbd") ) }
//  val medianAbsoluteDeviation: Op[Context, (Outliers, Context)] = {
//    val outliers = for {
//      context <- toSkylineContext <=< ask[TryV, Context]
//      tolerance <- tolerance <=< ask[TryV, Context]
//    } yield {
//      val deviationStats = new DescriptiveStatistics( context.data.size )
//      val deviations = context.data map { dp =>
//        val Array( ts, v ) = dp.getPoint
//
//        val median = context.movingStatistics.getPercentile( 50 )
//
//        context.source.points
//        .find{ _.timestamp.getMillis == ts.toLong }
//        .foreach { orig =>
//          log.debug( "medianAbsoluteDeviation: adding point ({}, {}) to moving stats", orig.timestamp.getMillis, orig.value )
//          context.movingStatistics addValue orig.value
//        }
//
//        val dev = math.abs( v - median )
//        deviationStats addValue dev
//        ( ts, dev )
//      }
//
//      val deviationMedian = deviationStats getPercentile 50
//
//      val tol = tolerance getOrElse 3D // skyline source uses 6.0 - admittedly arbitrary?
//
//      if ( deviationMedian == 0D ) (Row.empty[DataPoint], context)
//      else collectOutlierPoints( deviations, context ) { case (ts, dev) =>
//        val test = dev > ( tol * deviationMedian )
//        log.debug( "medianAbsoluteDeviation: ({}, {}) > {} isOutlier={}", new DateTime(ts.toLong), dev, tol*deviationMedian, test )
//        test
//      }
//    }
//
//    makeOutliersK( MedianAbsoluteDeviationAlgorithm, outliers )
//  }

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
    for {
      outliersContext <- outliers
      (outlierPoints, context) = outliersContext
      result <- kleisli[TryV, Context, Outliers]{ context => makeOutliers(outlierPoints, algorithm, context) }
    } yield (result, context)
  }

  def makeOutliers( outliers: Row[DataPoint], algorithm: Symbol, context: Context ): TryV[Outliers] = {
    Outliers.forSeries(
      algorithms = Set( algorithm ),
      plan = context.plan,
      source = context.source,
      outliers = outliers
    )
    .disjunction
    .leftMap { exs => exs.head }
  }

  val supportedAlgorithms: Map[Symbol, Op[Context, (Outliers, Context)]] = {
    Map(
      FirstHourAverageAlgorithm -> firstHourAverage,
      MeanSubtractionCumulationAlgorithm -> meanSubtractionCumulation,
      StddevFromAverageAlgorithm -> stddevFromAverage,
      StddevFromMovingAverageAlgorithm -> stddevFromMovingAverage,
      LeastSquaresAlgorithm -> leastSquares,
      GrubbsAlgorithm -> grubbs,
      HistogramBinsAlgorithm -> histogramBins,
      MedianAbsoluteDeviationAlgorithm -> medianAbsoluteDeviation,
      KsTestAlgorithm -> ksTest
    )
  }
}
