package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import peds.commons.log.Trace
import scalaz._, Scalaz._
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.commons.math3.stat.{ descriptive => stat }
import org.apache.commons.math3.ml.clustering.{ DBSCANClusterer, Cluster, DoublePoint }
import org.apache.commons.math3.ml.distance.EuclideanDistance
import com.typesafe.config.Config
import peds.commons.math.MahalanobisDistance
import lineup.model.timeseries.{ Matrix, DataPoint }
import lineup.model.outlier.{ CohortOutliers, NoOutliers, SeriesOutliers }
import lineup.analysis.outlier.{ HistoricalStatistics, DetectUsing, DetectOutliersInSeries, DetectOutliersInCohort }


/**
 * Created by rolfsd on 9/29/15.
 */
trait DBSCANAnalyzer extends AlgorithmActor {
  val trace = Trace[DBSCANAnalyzer]
  type TryV[T] = \/[Throwable, T]
  type Op[I, O] = Kleisli[TryV, I, O]
  case class TestContext(payload: Seq[DoublePoint], algorithmConfig: Config, history: Option[HistoricalStatistics] )

  val eps: Op[Config, Double] = {
    Kleisli[TryV, Config, Double] { c => \/ fromTryCatchNonFatal { c getDouble algorithm.name +".eps" } }
  }

  val minDensityConnectedPoints: Op[Config, Int] = {
    Kleisli[TryV, Config, Int] {c =>
      \/ fromTryCatchNonFatal { c getInt algorithm.name+".minDensityConnectedPoints" }
    }
  }

  val distanceMeasure: Op[TestContext, DistanceMeasure] = {
    Kleisli[TryV, TestContext, DistanceMeasure] { case TestContext( payload, config, history ) =>
      val distancePath = algorithm.name + ".distance"
      if ( config hasPath distancePath ) {
        config.getString( distancePath ).toLowerCase match {
          case "euclidean" | "euclid" => new EuclideanDistance( ).right[Throwable]
          case "mahalanobis" | "mahal" | _ => {
            val mahal = history map {h =>
              MahalanobisDistance.fromCovariance( h.covariance )
            } getOrElse {
              MahalanobisDistance.fromPoints( MatrixUtils.createRealMatrix( payload.toArray map {_.getPoint} ) )
            }

            mahal.disjunction.leftMap{ _.head }
          }
        }
      } else {
        MahalanobisDistance
        .fromPoints( MatrixUtils.createRealMatrix( payload.toArray map {_.getPoint} ) )
        .disjunction
        .leftMap { _.head }
      }
    }
  }

  val extractPayload: Op[TestContext, Seq[DoublePoint]] = {
    Kleisli[TryV, TestContext, Seq[DoublePoint]] { case TestContext(payload, _, _ ) => payload.right }
  }

  val extractConfig: Op[TestContext, Config] = {
    Kleisli[TryV, TestContext, Config] { case TestContext(_, config, _ ) => config.right }
  }

  val cluster: Op[TestContext, Seq[Cluster[DoublePoint]]] = {
    for {
      payload <- extractPayload
      e <- eps <=< extractConfig
      pts <- minDensityConnectedPoints <=< extractConfig
      distance <- distanceMeasure
    } yield {
      import scala.collection.JavaConverters._
      new DBSCANClusterer[DoublePoint]( e, pts, distance ).cluster( payload.asJava ).asScala.toSeq
    }
  }

  def makeOutlierTest( clusters: Seq[Cluster[DoublePoint]] ): DataPoint => Boolean = {
    import scala.collection.JavaConversions._

    if ( clusters.isEmpty ) (pt: DataPoint) => { false }
    else {
      val largestCluster = clusters.maxBy( _.getPoints.size ).getPoints.toSet
      ( pt: DataPoint ) => { !largestCluster.contains( pt ) }
    }
  }
}
