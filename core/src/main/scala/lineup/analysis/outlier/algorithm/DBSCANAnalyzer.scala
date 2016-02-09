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

  case class TestContext( message: DetectUsing, data: Seq[DoublePoint] )

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

      points map { pts => TestContext( data = pts, message = d ) }
    }
  }

  val distanceMeasure: Op[TestContext, DistanceMeasure] = {
    Kleisli[TryV, TestContext, DistanceMeasure] { case TestContext( message, data ) =>
      def makeMahalanobisDistance: TryV[DistanceMeasure] = {
        val mahal = message.history map { h =>
          MahalanobisDistance.fromCovariance( h.covariance )
        } getOrElse {
          MahalanobisDistance.fromPoints( MatrixUtils.createRealMatrix( data.toArray map { _.getPoint } ) )
        }

        mahal.disjunction.leftMap{ _.head }
      }

      val distancePath = algorithm.name + ".distance"
      if ( message.properties hasPath distancePath ) {
        message.properties.getString( distancePath ).toLowerCase match {
          case "euclidean" | "euclid" => new EuclideanDistance( ).right[Throwable]
          case "mahalanobis" | "mahal" | _ => makeMahalanobisDistance
        }
      } else {
        makeMahalanobisDistance
      }
    }
  }

  val extractPayload: Op[TestContext, Seq[DoublePoint]] = {
    Kleisli[TryV, TestContext, Seq[DoublePoint]] { case TestContext(_, payload) => payload.right }
  }

  val extractConfig: Op[TestContext, Config] = {
    Kleisli[TryV, TestContext, Config] { case TestContext(message, _) => message.properties.right }
  }

  val cluster: Op[TestContext, (TestContext, Seq[Cluster[DoublePoint]])] = {
    val context = Kleisli[TryV, TestContext, TestContext] { ctx => ctx.right[Throwable] }

    for {
      ctx <- context
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
      ( ctx, new DBSCANClusterer[DoublePoint]( e, pts, distance ).cluster( payload.asJava ).asScala.toSeq )
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
