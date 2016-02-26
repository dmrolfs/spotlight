package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import scalaz._
import Scalaz._
import org.apache.commons.math3.ml.clustering.DoublePoint
import lineup.model.outlier.{ NoOutliers, Outliers, SeriesOutliers }
import lineup.model.timeseries.{ DataPoint, TimeSeries }
import lineup.analysis.outlier.{ DetectUsing, HistoricalStatistics }
import lineup.analysis.outlier.algorithm.DBSCANAnalyzer.Clusters


/**
  * Created by rolfsd on 2/2/16.
  */
object SeriesCentroidDensityAnalyzer {
  val Algorithm: Symbol = 'dbscanSeriesCentroid
  def props( router: ActorRef ): Props = Props { new SeriesCentroidDensityAnalyzer( router ) }
}

class SeriesCentroidDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  import AlgorithmActor._

  override def algorithm: Symbol = SeriesCentroidDensityAnalyzer.Algorithm

  override val algorithmContext: Op[DetectUsing, AlgorithmContext] = {
    def centroidDistances( points: Seq[DoublePoint], history: HistoricalStatistics ): Seq[DoublePoint] = {
      val h = if ( history.n > 0 ) history else { HistoricalStatistics.fromActivePoints( points.toArray, false ) }
      val mean = h.mean( 1 )
      val distFromCentroid = points map { _.getPoint } map { case Array(x, y) => new DoublePoint( Array(x, y - mean) ) }
      log.debug( "points             : [{}]", points.mkString(",") )
      log.debug( "dists from centroid: [{}]", distFromCentroid.mkString(",") )
      distFromCentroid
    }

    Kleisli[TryV, DetectUsing, AlgorithmContext] { d =>
      val points: TryV[Seq[DoublePoint]] = d.payload.source match {
        case s: TimeSeries => centroidDistances( DataPoint.toDoublePoints(s.points), d.history ).right
        case x => -\/( new UnsupportedOperationException( s"cannot extract test context from [${x.getClass}]" ) )
      }

      points map { pts => AlgorithmContext( message = d, data = pts ) }
    }
  }

  override def findOutliers( source: TimeSeries ): Op[(AlgorithmContext, Clusters), Outliers] = {
    Kleisli[TryV, (AlgorithmContext, Clusters), Outliers] { case (context, clusters) =>
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
      } else {
        NoOutliers( algorithms = Set(algorithm), source = source, plan = context.message.plan ).right
      }
    }
  }
}
