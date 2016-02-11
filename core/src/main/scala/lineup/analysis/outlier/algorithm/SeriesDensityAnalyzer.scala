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

  case class DistanceHistory( stats: HistoricalStatistics, last: Option[DoublePoint] )
  var _distanceHistories: Map[HistoryKey, DistanceHistory] = Map.empty[HistoryKey, DistanceHistory]
  val updateHistory: Op[(OutlierPlan, TimeSeriesBase, DistanceMeasure), DistanceHistory] = {
    Kleisli[TryV, (OutlierPlan, TimeSeriesBase, DistanceMeasure), DistanceHistory] { case (plan, data, distance) =>
      val key = HistoryKey( plan, data.topic )
      val DistanceHistory( stats, last ) = _distanceHistories get key getOrElse {
        DistanceHistory( HistoricalStatistics( 2, false ), None )
      }

      // if no last value then start distance stats from head
      val points = DataPoint toDoublePoints data.points
      val basis: Seq[(DoublePoint, DoublePoint)] = last map { l => points.zip( l +: points ) } getOrElse { (points drop 1).zip( points ) }
      basis foreach { case (dp, prev) => stats add Array( dp.getPoint.head, distance.compute(prev.getPoint, dp.getPoint) ) }

      val result = DistanceHistory( stats, last = Option(data.points.last) )
      _distanceHistories += key -> result

      log.debug( "series density distance history = {}", stats )
      result.right
    }
  }

  override val history: Op[AnalyzerContext, Option[HistoricalStatistics]] = {
    val planSourceAndDistance = for {
      p <- plan
      s <- source
      d <- distanceMeasure
    } yield ( p, s, d )

    planSourceAndDistance >=> updateHistory map { dh => Option( dh.stats ) }
  }
}
