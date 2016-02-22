package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import scalaz._, Scalaz._
import scalaz.Kleisli.{ ask, kleisli }
import org.joda.{ time => joda }
import com.github.nscala_time.time.Imports._
import com.typesafe.config.Config
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.{ MultivariateSummaryStatistics, SummaryStatistics, DescriptiveStatistics }
import peds.commons.Valid
import lineup.model.outlier.{ OutlierPlan, Outliers }
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
    underlying: AlgorithmActor.Context,
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
    \/ fromTryCatchNonFatal {
      supportedAlgorithms( message.algorithm )
    } flatMap { op =>
      ( algorithmContext >=> op ) run message
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

  def updateMoment( moment: Moment, points: Seq[DataPoint] ): Moment = points.foldLeft( moment ) { case (m, dp) => m :+ dp.value }

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

  val momentInfoK: Op[Context, (Moment, MomentHistogram)] = Kleisli[TryV, Context, (Moment, MomentHistogram)] { context =>
    momentThenUpdate( context.source, context.plan )
  }

  def momentThenUpdate( data: TimeSeriesBase, plan: OutlierPlan ): TryV[(Moment, MomentHistogram)] = {
    val hkey = HistoryKey( plan, data.topic )

    for {
      history <- historicalMoment( data, plan )
      histogram <- momentHistogram( data, plan )
      groups <- groupPointsByMomentKey( data, plan )
      dataKeys = dataBinKeys( groups )
    } yield {
      val (updatedHistory, updatedHistogram) = data.points.foldLeft( (history, histogram) ) { case ( (hist, gram), pt ) =>
        val mbkey: MomentBinKey = dataKeys( pt )
        val moment: Moment = gram( mbkey )
        val updatedMoment = moment :+ pt.value
        (
          hist :+ pt.value,
          gram + (mbkey -> updatedMoment)
        )
      }

      _historicalMoments += hkey -> updatedHistory
      _momentHistograms += hkey -> updatedHistogram

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
    val last = context.history.lastPoints.drop( context.history.lastPoints.size - 2 ) map { new DoublePoint( _ ) }
    log.debug( "tail-average: last=[{}]", last.mkString(",") )
    context.data                            // data
    .zip( last ++ context.data )            // (data, last_-2_-1 ::: data)
    .zip( last.drop(1) ++ context.data )    // ((data, last1_-2_-1 ::: data), last_-1 :: data)
    .map { case ((d, y), z) =>
      log.debug( "tail-average: tuple=[({}, {}, {})]", d.getPoint.apply(1), y.getPoint.apply(1), z.getPoint.apply(1) )
      val timestamp = d.getPoint.apply( 0 )
      val average = ( d.getPoint.apply(1) + y.getPoint.apply(1) + z.getPoint.apply(1) ) / 3D
      ( timestamp, average )
    }
    .right
  }


  /**
    * Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    * A timeseries is anomalous if the average of the last three datapoints
    * are outside of three standard deviations of this value.
    */
  val firstHourAverage: Op[Context, Outliers] = {
    val outliers: Op[Context, Row[DataPoint]] = for {
      context <- toSkylineContext <=< ask[TryV, Context]
      tolerance <- tolerance <=< ask[TryV, Context]
      taverages <- tailAverage <=< ask[TryV, Context]
    } yield {
      val mean = context.firstHour.getMean
      val stddev = context.firstHour.getStandardDeviation
      log.debug( "first hour mean[{}] and stdev[{}]", mean, stddev )
      val tol = tolerance getOrElse 3D
      collectOutlierPoints( taverages, context ) { case (ts, v) => math.abs( v - mean ) > ( tol * stddev) }
    }

    makeOutliersK( FirstHourAverageAlgorithm, outliers )
  }

  val meanSubtractionCumulation: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }

  /**
    * A timeseries is anomalous if the absolute value of the average of the latest
    * three datapoint minus the moving average is greater than tolerance * standard
    * deviations of the average. This does not exponentially weight the moving average
    * and so is better for detecting anomalies with respect to the entire series.
    */
  val stddevFromAverage: Op[Context, Outliers] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, Context]
      tolerance <- tolerance <=< ask[TryV, Context]
      taverages <- tailAverage <=< ask[TryV, Context]
    } yield {
      val source = context.source.points
      val tol = tolerance getOrElse 3D
      collectOutlierPoints( taverages, context ) { case (ts, v) =>
        val mean = context.movingStatistics.getMean
        val stddev = context.movingStatistics.getStandardDeviation
        log.debug( "mean[{}]\tstdev[{}]\ttolerance[{}]", mean, stddev, tol )
        val test = math.abs( v - mean ) > ( tol * stddev)
        val original = source.find{ dp => dp.timestamp.getMillis == ts.toLong }
        original foreach { orig => context.movingStatistics addValue orig.value }
        test
      }
    }

    makeOutliersK( StddevFromAverageAlgorithm, outliers )
  }

  /**
    * A timeseries is anomalous if the absolute value of the average of the latest
    * three datapoint minus the moving average is greater than tolerance * standard
    * deviations of the moving average. This is better for finding anomalies with
    * respect to the short term trends.
    */
  val stddevFromMovingAverage: Op[Context, Outliers] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, Context]
      tolerance <- tolerance <=< ask[TryV, Context]
      taverages <- tailAverage <=< ask[TryV, Context]
    } yield {
      val ewma = context.historicalMoment.statistics.ewma
      val ewmsd = context.historicalMoment.statistics.ewmsd
      val tol = tolerance getOrElse 3D
      collectOutlierPoints( taverages, context ) { case (ts, v) => math.abs( v - ewma ) > ( tol * ewmsd ) }
    }

    makeOutliersK( StddevFromMovingAverageAlgorithm, outliers )
  }

  val leastSquares: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }

  /**
    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
    */
  val grubbs: Op[Context, Outliers] = {
    // background: http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
    // background: http://graphpad.com/support/faqid/1598/
    val outliers: Op[Context, Row[DataPoint]] = for {
      context <- ask[TryV, Context]
      taverages <- tailAverage <=< ask[TryV, Context]
    } yield {
      val data = context.data.map{ _.getPoint.apply( 1 ) }.toArray
      val stats = new DescriptiveStatistics( data )
      val stddev: Double = stats.getStandardDeviation
      val mean: Double = stats.getMean
      val zScores = taverages map { case (ts, v) =>
        val z = ( v - mean ) / stddev
        ( ts, z )
      }
      log.debug( "Skyline[Grubbs]: moving-mean:[{}] moving-stddev:[{}] zScores:[{}]", mean, stddev, zScores.map{ case (z0, z1) => (z0, z1)}.mkString(",") )

      val Alpha = 0.05
      val threshold = new TDistribution( data.size - 2 ).inverseCumulativeProbability( Alpha / ( 2D * data.size ) )
      val tsq = threshold * threshold
      log.debug( "Skyline[Grubbs]: threshold^s:[{}]", tsq )
      val grubbsScore = ((data.size - 1) / math.sqrt(data.size)) * math.sqrt( tsq / (data.size - 2 + tsq) )
      log.debug( "Skyline[Grubbs]: Grubbs Score:[{}]", grubbsScore )
      collectOutlierPoints( zScores, context ) { case ( ts, z ) => z > grubbsScore }
    }

    makeOutliersK( GrubbsAlgorithm, outliers )
  }

  val histogramBins: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }

  /**
    * A timeseries is anomalous if the deviation of its latest datapoint with
    * respect to the median is [tolerance] times larger than the median of deviations.
    */
  val medianAbsoluteDeviation: Op[Context, Outliers] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, Context]
      tolerance <- tolerance <=< ask[TryV, Context]
    } yield {
      val median = context.movingStatistics.getPercentile( 50 )
      val deviationStats = new DescriptiveStatistics( context.data.size )
      val deviations = context.data map { dp =>
        val Array( ts, v ) = dp.getPoint
        val dev = math.abs( v - median )
        deviationStats addValue dev
        ( ts, dev )
      }

      val deviationMedian = deviationStats getPercentile 50

      val tol = tolerance getOrElse 3D // skyline source uses 6.0 - admittedly arbitrary?

      if ( deviationMedian == 0D ) Row.empty[DataPoint]
      else collectOutlierPoints( deviations, context ) { case (ts, dev) => dev > ( tol * deviationMedian ) }
    }

    makeOutliersK( MedianAbsoluteDeviationAlgorithm, outliers )
  }

  val ksTest: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }


  type IsOutlier = Point2D => Boolean

  def collectOutlierPoints( points: Seq[Point2D], context: Context )( isOutlier: IsOutlier ): Row[DataPoint] = {
    points
    .collect {
      case p @ (ts, v) if isOutlier( p ) => {
        log.error( "OUTLIER FOUND @ ({}, {})", new DateTime(ts.toLong), v )
        p
      }

      case p @ (ts, v) => {
        log.info( "no outlier found @ ({}, {})", new DateTime(ts.toLong), v )
        p
      }
    }
    .filter( isOutlier )
    .map { case (ts, v) => context.source.points find { _.timestamp.getMillis == ts.toLong } }
    .flatten
    .toIndexedSeq
  }

  def makeOutliersK( algorithm: Symbol, outliers: Op[Context, Row[DataPoint]] ): Op[Context, Outliers] = {
    for {
      o <- outliers
      result <- kleisli[TryV, Context, Outliers]( context => makeOutliers(o, algorithm, context) )
    } yield result
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

  val supportedAlgorithms: Map[Symbol, Op[Context, Outliers]] = {
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
