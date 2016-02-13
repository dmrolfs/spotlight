package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.ml.distance.DistanceMeasure
import scalaz._, Scalaz._
import lineup.model.timeseries._
import lineup.model.outlier.{ OutlierPlan, Outliers, NoOutliers, SeriesOutliers }
import lineup.analysis.outlier.{ HistoryKey, HistoricalStatistics }


/**
 * Created by rolfsd on 9/29/15.
 */
object SeriesDensityAnalyzer {
  val Algorithm: Symbol = 'dbscanSeries
  def props( router: ActorRef ): Props = Props { new SeriesDensityAnalyzer( router ) }
}

class SeriesDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  import DBSCANAnalyzer._

  override val algorithm: Symbol = SeriesDensityAnalyzer.Algorithm

  override def findOutliers( source: TimeSeries ): Op[(AnalyzerContext, Clusters), Outliers] = {
    val pullContext = Kleisli[TryV, (AnalyzerContext, Clusters), AnalyzerContext] { case (ctx, _) => ctx.right }
    val timeseries = Kleisli[TryV, AnalyzerContext, TimeSeriesBase] { case ctx => ctx.message.payload.source.right }

    val outliers: Op[(AnalyzerContext, Clusters), Seq[DataPoint]] = for {
      contextAndClusters <- identityK[(AnalyzerContext, Clusters)]
      (context, clusters) = contextAndClusters
      ts <- timeseries <=< pullContext
      distance <- distanceMeasure <=< pullContext
      isOutlier = makeOutlierTest( clusters )
    } yield {
      for {
        dp <- context.data if isOutlier( dp )
        o <- source.points if o.timestamp.getMillis == dp.getPoint.head.toLong
      } yield o
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
  val updateHistory: Op[(OutlierPlan, TimeSeriesBase, DistanceMeasure), Option[HistoricalStatistics]] = {
    Kleisli[TryV, (OutlierPlan, TimeSeriesBase, DistanceMeasure), Option[HistoricalStatistics]] { case (plan, data, distance) =>
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
      Option( updatedHistory ).right
    }
  }

  override val history: Op[AnalyzerContext, Option[HistoricalStatistics]] = {
    val planSourceAndDistance = for {
      p <- plan
      s <- source
      d <- distanceMeasure
    } yield ( p, s, d )

    planSourceAndDistance >=> updateHistory
  }
}
