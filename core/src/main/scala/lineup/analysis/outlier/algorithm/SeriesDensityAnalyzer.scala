package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import com.typesafe.config.Config
import lineup.analysis.outlier.algorithm.AlgorithmActor.TryV
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.ml.distance.DistanceMeasure
import scalaz._, Scalaz._
import scalaz.Kleisli.{ ask, kleisli }
import lineup.model.timeseries._
import lineup.model.outlier.{ OutlierPlan, Outliers, NoOutliers, SeriesOutliers }
import lineup.analysis.outlier.{ DetectUsing, HistoryKey, HistoricalStatistics }


/**
 * Created by rolfsd on 9/29/15.
 */
object SeriesDensityAnalyzer {
  val Algorithm: Symbol = 'dbscanSeries
  def props( router: ActorRef ): Props = Props { new SeriesDensityAnalyzer( router ) }


  class SeriesDensityContext(
    underlying: AlgorithmActor.Context,
    override val history: HistoricalStatistics
  ) extends AlgorithmActor.Context {
    override def message: DetectUsing = underlying.message
    override def algorithm: Symbol = underlying.algorithm
    override def plan: OutlierPlan = underlying.plan
    override def data: Seq[DoublePoint] = underlying.data
    override def source: TimeSeriesBase = underlying.source
    override def messageConfig: Config = underlying.messageConfig
    override def distanceMeasure: TryV[DistanceMeasure] = underlying.distanceMeasure
    override def tolerance: TryV[Option[Double]] = underlying.tolerance
  }
}

class SeriesDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  import AlgorithmActor._
  import SeriesDensityAnalyzer.SeriesDensityContext

  override val algorithm: Symbol = SeriesDensityAnalyzer.Algorithm

  override def algorithmContext: Op[DetectUsing, Context] = {
    val distanceHistoryArgs = for {
      context <- ask[TryV, Context]
      distance <- kleisli { ctx: Context => ctx.distanceMeasure }
    } yield ( context.plan, context.source, distance )

    for {
      context <- super.algorithmContext
      distanceHistory <- updateHistory <=< distanceHistoryArgs <=< super.algorithmContext
    } yield {
      new SeriesDensityContext( underlying = context, distanceHistory )
    }
  }

  override def findOutliers( source: TimeSeries ): Op[(Context, Clusters), Outliers] = {
    val pullContext = Kleisli[TryV, (Context, Clusters), Context] { case (ctx, _) => ctx.right }

    val outliers: Op[(Context, Clusters), Seq[DataPoint]] = {
      for {
        contextAndClusters <- Kleisli.ask[TryV, (Context, Clusters)]
        (context, clusters) = contextAndClusters
        distance <- kleisli { ctx: Context => ctx.distanceMeasure } <=< pullContext
        isOutlier = makeOutlierTest( clusters )
      } yield {
        for {
          dp <- context.data if isOutlier( dp )
          o <- source.points if o.timestamp.getMillis == dp.getPoint.head.toLong
        } yield o
      }
    }

    for {
      ctx <- pullContext
      os <- outliers
    } yield {
      if ( os.nonEmpty ) {
        SeriesOutliers(
          algorithms = Set(algorithm),
          source = source,
          outliers = os.toIndexedSeq,
          plan = ctx.message.plan
        )
      } else {
        NoOutliers( algorithms = Set(algorithm), source = source, plan = ctx.message.plan )
      }
    }
  }

  var _distanceHistories: Map[HistoryKey, HistoricalStatistics] = Map.empty[HistoryKey, HistoricalStatistics]
  val updateHistory: Op[(OutlierPlan, TimeSeriesBase, DistanceMeasure), HistoricalStatistics] = {
    Kleisli[TryV, (OutlierPlan, TimeSeriesBase, DistanceMeasure), HistoricalStatistics] { case (plan, data, distance) =>
      val key = HistoryKey( plan, data.topic )
      val initialHistory = _distanceHistories get key getOrElse { HistoricalStatistics( 2, false ) }
      log.debug( "series density initial distance history = {}", initialHistory )

      // if no last value then start distance stats from head
      val points = DataPoint toDoublePoints data.points
      val last = initialHistory.lastPoints.lastOption map { new DoublePoint( _ ) }
      val basis = last map { l => points.zip( l +: points ) } getOrElse { (points drop 1).zip( points ) }

      val updatedHistory = basis.foldLeft( initialHistory ) { case (h, (cur, prev)) =>
        val ts = cur.getPoint.head
        val dist = distance.compute( prev.getPoint, cur.getPoint )
        h.add( Array(ts, dist) )
      }

      _distanceHistories += key -> updatedHistory
      log.debug( "series density updated distance history = {}", updatedHistory )
      updatedHistory.right
    }
  }
}
