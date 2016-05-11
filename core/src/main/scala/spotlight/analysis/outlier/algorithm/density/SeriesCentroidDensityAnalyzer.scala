package spotlight.analysis.outlier.algorithm.density

import scala.reflect.ClassTag
import scalaz._
import Scalaz._
import akka.actor.{ActorRef, Props}
import org.apache.commons.math3.ml.clustering.DoublePoint
import peds.commons.{KOp, TryV}
import spotlight.analysis.outlier.algorithm.AlgorithmActor
import spotlight.analysis.outlier.algorithm.density.DBSCANAnalyzer.Clusters
import spotlight.analysis.outlier.{DetectUsing, HistoricalStatistics}
import spotlight.model.outlier.{NoOutliers, Outliers, SeriesOutliers}
import spotlight.model.timeseries._



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

  override val algorithmContext: KOp[DetectUsing, AlgorithmContext] = {
    def centroidDistances( points: Seq[DoublePoint], history: HistoricalStatistics ): Seq[DoublePoint] = {
      val h = if ( history.N > 0 ) history else HistoricalStatistics.fromActivePoints( points, false )
      val mean = h.mean( 1 )
      val distFromCentroid = points map { dp => ( dp.timestamp, dp.value - mean ).toDoublePoint }
      log.debug( "points             : [{}]", points.mkString(",") )
      log.debug( "dists from centroid: [{}]", distFromCentroid.mkString(",") )
      distFromCentroid
    }

    Kleisli[TryV, DetectUsing, AlgorithmContext] { d =>
      val points: TryV[Seq[DoublePoint]] = d.payload.source match {
        case s: TimeSeries => centroidDistances( s.points.toDoublePoints, d.history ).right
        case x => -\/( new UnsupportedOperationException( s"cannot extract test context from [${x.getClass}]" ) )
      }

      points map { pts => AlgorithmContext( message = d, data = pts ) }
    }
  }

  override def findOutliers: KOp[(AlgorithmContext, Clusters), Outliers] = {
    Kleisli[TryV, (AlgorithmContext, Clusters), Outliers] { case (context, clusters) =>
      val isOutlier = makeOutlierTest( clusters )
      val centroidOutliers: Set[Long] = {
        context.data
        .filter { isOutlier }
        .map { _.getPoint.apply(0).toLong }
        .toSet
      }

      val outliers = context.source.points filter { dp => centroidOutliers.contains( dp.getPoint.apply(0).toLong ) }
      val tsTag = ClassTag[TimeSeries]( classOf[TimeSeries] )
      context.source match {
        case tsTag( src ) if outliers.nonEmpty => {
          SeriesOutliers(
            algorithms = Set(algorithm),
            source = src,
            outliers = outliers,
            plan = context.message.plan
          ).right
        }

        case src => NoOutliers( algorithms = Set(algorithm), source = src, plan = context.message.plan ).right
      }
    }
  }
}
