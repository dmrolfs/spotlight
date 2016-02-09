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
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      ( extractTestContext >==> cluster >==> findOutliers( payload.source ) ).run( s ) match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => log.error( ex, "failed {} analysis on {}[{}]", algorithm.name, payload.topic, payload.source.interval )
      }
    }
  }

  override val extractTestContext: Op[DetectUsing, TestContext] = {
    def centroidDistances( points: Seq[DoublePoint], history: Option[HistoricalStatistics] ): Seq[DoublePoint] = {
      val h = history getOrElse { HistoricalStatistics.fromActivePoints( points.toArray, false ) }
      val mean = h.mean( 1 )
      val result = points map { _.getPoint } map { case Array(x, y) => new DoublePoint( Array(x, y - mean) ) }
      log.debug( "points             : [{}]", points.mkString(",") )
      log.debug( "dists from centroid: [{}]", result.mkString(",") )
      result
    }

    Kleisli[TryV, DetectUsing, TestContext] { d =>
      val points: TryV[Seq[DoublePoint]] = d.payload.source match {
        case s: TimeSeries => centroidDistances( DataPoint.toDoublePoints(s.points), d.history ).right
        case x => -\/( new UnsupportedOperationException( s"cannot extract test context from [${x.getClass}]" ) )
      }

      points map { pts => TestContext( message = d, data = pts ) }
    }
  }

  def findOutliers( source: TimeSeries ): Op[(TestContext, Seq[Cluster[DoublePoint]]), Outliers] = {
    Kleisli[TryV, (TestContext, Seq[Cluster[DoublePoint]]), Outliers] { case (context, clusters) =>
      val isOutlier = makeOutlierTest( clusters )
      val centroidOutliers: Set[Long] = {
        context.data
        .filter { isOutlier }
        .map { _.getPoint.apply(0).toLong }
        .toSet
      }

      val outliers = source.points filter { dp => centroidOutliers.contains( dp.getPoint.apply(0).toLong ) }
      if ( outliers.nonEmpty ) {
        SeriesOutliers( algorithms = Set(algorithm), source = source, outliers = outliers, plan = context.message.plan ).right
      }
      else {
        NoOutliers( algorithms = Set(algorithm), source = source, plan = context.message.plan ).right
      }
    }
  }
}
