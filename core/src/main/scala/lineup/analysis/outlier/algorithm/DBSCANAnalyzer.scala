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
object DBSCANAnalyzer {
  def seriesDensity( router: ActorRef ): Props = Props { new SeriesDensityAnalyzer( router ) }
  def cohortDensity( router: ActorRef ): Props = Props { new CohortDensityAnalyzer( router ) }

  val SeriesDensityAlgorithm = 'dbscanSeries
  val CohortDensityAlgorithm = 'dbscanCohort
}

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

class SeriesDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  override val algorithm: Symbol = DBSCANAnalyzer.SeriesDensityAlgorithm

  override val detect: Receive = LoggingReceive {
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      val points = payload.data.points map {DataPoint.toDoublePoint}

      val result = cluster.run( TestContext( points, algorithmConfig, history ) ) map {clusters =>
        val isOutlier = makeOutlierTest( clusters )
        val outliers = payload.data.points collect { case dp if isOutlier( dp ) => dp }
        if ( outliers.nonEmpty ) SeriesOutliers( algorithms = Set( algorithm ), source = payload.data, outliers = outliers )
        else NoOutliers( algorithms = Set( algorithm ), source = payload.data )
      }

      result match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => log.error( ex, s"failed ${algorithm.name} analysis on ${payload.topic}[${payload.source.interval}]" )
      }
    }
  }
}


class CohortDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  override val algorithm: Symbol = DBSCANAnalyzer.CohortDensityAlgorithm

  //todo: consider sensitivity here and in series!!!
  // DataDog: We use a simplified form of DBSCAN to detect outliers on time series. We consider each host to be a point in
  // d-dimensions, where d is the number of elements in the time series. Any point can agglomerate, and any point that is not in
  // the largest cluster will be considered an outlier.
  //
  // The only parameter we take is tolerance, the constant by which the initial threshold is multiplied to yield DBSCAN’s
  // distance parameter 𝜀. Here is DBSCAN with a tolerance of 3.0 in action on a pool of Cassandra workers:
  override val detect: Receive = LoggingReceive {
    case c @ DetectUsing( algo, aggregator, payload: DetectOutliersInCohort, history, algorithmConfig ) => {
      val outlierMarks = for {
        frameDistances <- cohortDistances( payload ).toList
        points = frameDistances map { DataPoint.toDoublePoint }
      } yield {
        cluster.run( TestContext(points, algorithmConfig, history) )
        .map { clusters =>
          val isOutlier = makeOutlierTest( clusters )
          frameDistances.toList.zipWithIndex collect { case (fd, i) if isOutlier( fd ) => i }
        }
      }

      val result = {
        outlierMarks
        .sequenceU
        .map { oms: List[List[Int]] =>
          val outlierSeries = oms.flatten.toSet[Int] map { index => payload.data.data( index ) }
          if ( outlierSeries.isEmpty ) NoOutliers( algorithms = Set(algorithm), source = payload.data )
          else CohortOutliers( algorithms = Set(algorithm), source = payload.data, outliers = outlierSeries )
        }
      }

      result match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => log.error( ex, s"failed ${algorithm.name} analysis on ${payload.topic}[${payload.source.interval}]" )
      }
    }
  }

  def cohortDistances( underlying: DetectOutliersInCohort ): Matrix[DataPoint] = {
    for {
      frame <- underlying.data.toMatrix
      timestamp = frame.head._1 // all of frame's _1 better be the same!!!
      frameValues = frame map { _._2 }
      stats = new stat.DescriptiveStatistics( frameValues.toArray )
      median = stats.getPercentile( 50 )
    } yield frameValues map { v => DataPoint( timestamp, v - median ) }
  }
}
