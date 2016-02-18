package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import com.typesafe.config.Config
import org.apache.commons.math3.ml.distance.DistanceMeasure
import scalaz._, Scalaz._
import scalaz.Kleisli.{ ask, kleisli }
import org.joda.{ time => joda }
import com.github.nscala_time.time.Imports._
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.{ SummaryStatistics, DescriptiveStatistics }
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


  class SkylineContext(
    underlying: AlgorithmActor.Context,
    val firstHour: SummaryStatistics
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

}

class SkylineAnalyzer( override val router: ActorRef ) extends AlgorithmActor {
  import SkylineAnalyzer._
  import AlgorithmActor._

  type TryV[T] = \/[Throwable, T]
  type Op[I, O] = Kleisli[TryV, I, O]
  type Points = Seq[DoublePoint]


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
      ( algorithmContext >==> op ) run message
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

  val tailAverage: Op[Context, Seq[DoublePoint]] = Kleisli[TryV, Context, Seq[DoublePoint]] { context =>
    val last = context.history.lastPoints.drop( context.history.lastPoints.size - 2 ) map { new DoublePoint( _ ) }
    context.data
    .zip( last ++ context.data )
    .zip( last.drop(1) ++ context.data )
    .map { case ((d, y), z) =>
      val timestamp = d.getPoint.apply( 0 )
      val average = ( d.getPoint.apply(1) + y.getPoint.apply(1) + z.getPoint.apply(1) ) / 3D
      new DoublePoint( Array(timestamp, average) )
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
      context <- ask[TryV, Context]
      tolerance <- tolerance <==< ask[TryV, Context]
      taverage <- tailAverage <==< ask[TryV, Context]
    } yield {
      val (mean, stddev) = context match {
        case ctx: SkylineContext => ( ctx.firstHour.getMean, ctx.firstHour.getStandardDeviation )
        case ctx => {
          log.warning(
            "using entire historical statistics since skyline context including first hour statistics not formed for [{}]:[{}]",
            ctx.algorithm,
            ctx.plan
          )

          ( ctx.history.mean(0), ctx.history.standardDeviation(1) )
        }
      }
      log.debug( "first hour mean[{}] and stdev[{}]", mean, stddev )

      val tol = tolerance getOrElse 3D

      taverage
      .map { _.getPoint }
      .collect {
        case Array(ts, v) if (v - mean) > (tol * stddev) => {
          log.warning( "FOUND OUTLIER @ ({}, {})", new DateTime(ts.toLong), v )
          context.source.points.find { _.timestamp.getMillis == ts.toLong }
        }

        case Array(ts, v) => {
          log.error( "no outlier @ ({}, {})", new DateTime(ts.toLong), v )
          None
        }
      }
      .flatten
      .toIndexedSeq
    }

    for {
      o <- outliers
      result <- kleisli[TryV, Context, Outliers]( context => makeOutliers(o, FirstHourAverageAlgorithm, context) )
    } yield result
  }

  val meanSubtractionCumulation: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }
  val stddevFromAverage: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }
  val stddevFromMovingAverage: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }
  val leastSquares: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }

  /**
    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
    */
  val grubbs: Op[Context, Outliers] = {
    // background: http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
    // background: http://graphpad.com/support/faqid/1598/
    val outliers: Op[Context, Row[DataPoint]] = for {
      context <- ask[TryV, Context]
      tailAvgs <- tailAverage <=< ask[TryV, Context]
    } yield {
      val data = context.data.map{ _.getPoint.apply( 1 ) }.toArray
      val stats = new DescriptiveStatistics( data )
      val stddev: Double = stats.getStandardDeviation
      val mean: Double = stats.getMean
      val zScores = tailAvgs map { dp =>
        val Array( ts, v ) = dp.getPoint
        val z = ( v - mean ) / stddev
        Array( ts, z )
      }
      log.debug( "Skyline[Grubbs]: moving-mean:[{}] moving-stddev:[{}] zScores:[{}]", mean, stddev, zScores.map{z => (z(0), z(1))}.mkString(",") )

      val Alpha = 0.05
      val threshold = new TDistribution( data.size - 2 ).inverseCumulativeProbability( Alpha / ( 2D * data.size ) )
      val tsq = threshold * threshold
      log.debug( "Skyline[Grubbs]: threshold^s:[{}]", tsq )
      val grubbsScore = ((data.size - 1) / math.sqrt(data.size)) * math.sqrt( tsq / (data.size - 2 + tsq) )
      log.debug( "Skyline[Grubbs]: Grubbs Score:[{}]", grubbsScore )

      zScores
      .collect {
        case Array( ts, z ) if z > grubbsScore => context.source.points.find{ _.timestamp.getMillis == ts.toLong }
      }
      .flatten
      .toIndexedSeq
    }

    for {
      o <- outliers
      result <- kleisli[TryV, Context, Outliers]( context => makeOutliers(o, GrubbsAlgorithm, context) )
    } yield result
  }

  val histogramBins: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }

  /**
    * A timeseries is anomalous if the deviation of its latest datapoint with
    * respect to the median is [tolerance] times larger than the median of deviations.
    */
  val medianAbsoluteDeviation: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context =>
    val median = context.history.movingPercentile( 50 )
    val deviationStats = new DescriptiveStatistics( context.data.size )
    val deviations = context.data map { dp =>
      val Array( ts, v ) = dp.getPoint
      val dev = math.abs( v - median )
      deviationStats addValue dev
      Array( ts, dev )
    }

    val medianDeviation = deviationStats getPercentile 50

    context.tolerance flatMap { t =>
      val tolerance = t getOrElse 6.0 // arbitrary tolerance based on Skyline source

      val outliers = if ( medianDeviation == 0D ) Row.empty[DataPoint]
      else {
        deviations
        .collect {
          case Array(ts, dev) if (dev / tolerance) > medianDeviation => {
            context.source.points find { _.timestamp.getMillis == ts.toLong }
          }
        }
        .flatten
        .toIndexedSeq
      }

      makeOutliers( outliers, MedianAbsoluteDeviationAlgorithm, context )
    }
  }

  val ksTest: Op[Context, Outliers] = Kleisli[TryV, Context, Outliers] { context => -\/( new IllegalStateException("tbd") ) }

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


  override def algorithmContext: Op[DetectUsing, Context] = {
    for {
      context <- super.algorithmContext
      firstHourHistory <- updateFirstHourHistory <==< super.algorithmContext
    } yield {
      new SkylineContext( underlying = context, firstHour = firstHourHistory )
    }
  }



  val firstHour: joda.Interval = new joda.Interval( joda.DateTime.now, 1.hour.toDuration )
  var _firstHourHistory: Map[HistoryKey, SummaryStatistics] = Map.empty[HistoryKey, SummaryStatistics]

  def updateFirstHourHistory: Op[Context, SummaryStatistics] = Kleisli[TryV, Context, SummaryStatistics] { context =>
    val key = HistoryKey( context.plan, context.source.topic )

    val points = context.data map { _.getPoint } collect { case Array(ts, v) if firstHour contains ts.toLong => v }
    val initial = _firstHourHistory get key map { _.copy } getOrElse { new SummaryStatistics() }

    val result = {
      if ( points.nonEmpty ) {
        val updated = points.foldLeft( initial ) { (h, v) => h.addValue( v ); h }
        _firstHourHistory += key -> updated
        updated
      } else {
        initial
      }
    }

    result.copy.right
  }
}
