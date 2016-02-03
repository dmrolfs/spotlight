package lineup.analysis.outlier.algorithm

import scalaz._, Scalaz._
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.commons.math3.ml.clustering.{ DBSCANClusterer, Cluster, DoublePoint }
import org.apache.commons.math3.ml.distance.EuclideanDistance
import com.typesafe.config.Config
import peds.commons.log.Trace
import peds.commons.math.MahalanobisDistance
import lineup.model.timeseries._
import lineup.analysis.outlier.{ DetectUsing, HistoricalStatistics }


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

  val extractTestContext: Op[DetectUsing, TestContext] = {
    Kleisli[TryV, DetectUsing, TestContext] { d =>
      val points: TryV[Seq[DoublePoint]] = d.payload.source match {
        case s: TimeSeries => DataPoint.toDoublePoints( s.points ).right[Throwable]

        case c: TimeSeriesCohort => {
//          def cohortDistances( cohort: TimeSeriesCohort ): Matrix[DataPoint] = {
//            for {
//              frame <- cohort.toMatrix
//              timestamp = frame.head._1 // all of frame's _1 better be the same!!!
//              frameValues = frame map { _._2 }
//              stats = new DescriptiveStatistics( frameValues.toArray )
//              median = stats.getPercentile( 50 )
//            } yield frameValues map { v => DataPoint( timestamp, v - median ) }
//          }
//
//          val outlierMarks: Row[Row[DoublePoint]] = for {
//            frameDistances <- cohortDistances( c )
//            points = frameDistances map { DataPoint.toDoublePoint }
//          } yield points

          //todo: work in progress as part of larger goal to type class b/h around outlier calcs
          -\/( new UnsupportedOperationException( s"can't support cohorts yet" ) )
        }

        case x => -\/( new UnsupportedOperationException( s"cannot extract test context from [${x.getClass}]" ) )
      }

      points map { pts => TestContext( payload = pts, algorithmConfig = d.properties, history = d.history ) }
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
      trace( s"""cluster: eps = [${e}]""")
      trace( s"""cluster: minDensityConnectedPoints = [${pts}]""")
      trace( s"""cluster: distanceMeasure = [${distance}]""")
      trace( s"""cluster: payload = [${payload.mkString(",")}]""")

      import scala.collection.JavaConverters._
      new DBSCANClusterer[DoublePoint]( e, pts, distance ).cluster( payload.asJava ).asScala.toSeq
    }
  }

  def makeOutlierTest( clusters: Seq[Cluster[DoublePoint]] ): DoublePoint => Boolean = {
    import scala.collection.JavaConverters._

    if ( clusters.isEmpty ) (pt: DoublePoint) => { false }
    else {
      val largestCluster = {
        clusters
        .maxBy { _.getPoints.size }
        .getPoints
        .asScala
        .toSet
      }

      ( pt: DoublePoint ) => { !largestCluster.contains( pt ) }
    }
  }
}
