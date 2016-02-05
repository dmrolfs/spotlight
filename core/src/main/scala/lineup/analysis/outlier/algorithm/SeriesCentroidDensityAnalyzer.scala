package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import lineup.model.outlier.{ OutlierPlan, NoOutliers, SeriesOutliers, Outliers }
import scalaz._, Scalaz._
import lineup.analysis.outlier.{ HistoricalStatistics, DetectOutliersInSeries, DetectUsing }
import lineup.model.timeseries.{ TimeSeries, DataPoint }
import org.apache.commons.math3.ml.clustering.{ Cluster, DoublePoint }


/**
  * Created by rolfsd on 2/2/16.
  */
object SeriesCentroidDensityAnalyzer {
  val Algorithm: Symbol = 'dbscanSeriesCentroid
  def props( router: ActorRef ): Props = Props { new SeriesCentroidDensityAnalyzer( router ) }
}

class SeriesCentroidDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  override def algorithm: Symbol = SeriesCentroidDensityAnalyzer.Algorithm

  override def detect: Receive = LoggingReceive {
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, plan, history, algorithmConfig ) => {
      val distances = centroidDistances( DataPoint.toDoublePoints(payload.source.points), history )
      val ctx = TestContext( payload = distances, algorithmConfig, history )
      ( cluster >==> findOutliers(payload.source, plan, history) ).run( ctx ) match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => log.error( ex, s"failed ${algorithm.name} analysis on ${payload.topic}[${payload.source.interval}]" )
      }
    }
  }

  val extractDistancesFromCentroid: Op[TestContext, TestContext] = {
    Kleisli[TryV, TestContext, TestContext] { ctx => ctx.copy( payload = centroidDistances(ctx.payload, ctx.history) ).right }
  }

  def centroidDistances( points: Seq[DoublePoint], history: Option[HistoricalStatistics] ): Seq[DoublePoint] = {
    val h = history getOrElse { HistoricalStatistics.fromActivePoints( points.toArray, false ) }
    val mean = h.mean( 1 )
    points map { _.getPoint } map { case Array(x, y) => new DoublePoint( Array(x, y - mean) ) }
  }

  def findOutliers(
    source: TimeSeries,
    plan: OutlierPlan,
    history: Option[HistoricalStatistics]
  ): Op[Seq[Cluster[DoublePoint]], Outliers] = {
    Kleisli[TryV, Seq[Cluster[DoublePoint]], Outliers] { clusters =>
      val distances: Seq[DoublePoint] = centroidDistances( DataPoint.toDoublePoints(source.points), history )
      val isOutlier = makeOutlierTest( clusters )
      val centroidOutliers: Set[Long] = {
        distances
        .filter { isOutlier }
        .map { _.getPoint.apply(0).toLong }
        .toSet
      }

      val outliers = source.points filter { dp => centroidOutliers.contains( dp.getPoint.apply(0).toLong ) }
      if ( outliers.nonEmpty ) {
        SeriesOutliers( algorithms = Set(algorithm), source = source, outliers = outliers, plan = plan ).right
      }
      else {
        NoOutliers( algorithms = Set(algorithm), source = source, plan = plan ).right
      }
    }
  }
}
