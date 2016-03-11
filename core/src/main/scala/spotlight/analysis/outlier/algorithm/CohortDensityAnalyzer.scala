package spotlight.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import scalaz._, Scalaz._
import org.apache.commons.math3.ml.clustering.{ Cluster, DoublePoint }
import org.apache.commons.math3.stat.{ descriptive => stat }
import spotlight.model.timeseries.{ TimeSeries, Matrix, DataPoint }
import spotlight.model.outlier.{ Outliers, CohortOutliers, NoOutliers }
import spotlight.analysis.outlier.{ DetectUsing, DetectOutliersInCohort }


/**
 * Created by rolfsd on 9/29/15.
 */
object CohortDensityAnalyzer {
  val Algorithm: Symbol = 'dbscanCohort
  def props( router: ActorRef ): Props = Props { new CohortDensityAnalyzer( router ) }
}

class CohortDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  import AlgorithmActor._

  override val algorithm: Symbol = CohortDensityAnalyzer.Algorithm

  //todo: consider sensitivity here and in series!!!
  // DataDog: We use a simplified form of DBSCAN to detect outliers on time series. We consider each host to be a point in
  // d-dimensions, where d is the number of elements in the time series. Any point can agglomerate, and any point that is not in
  // the largest cluster will be considered an outlier.
  //
  // The only parameter we take is tolerance, the constant by which the initial threshold is multiplied to yield DBSCAN’s
  // distance parameter 𝜀. Here is DBSCAN with a tolerance of 3.0 in action on a pool of Cassandra workers:
  override val detect: Receive = LoggingReceive {
    case c @ DetectUsing( algo, aggregator, payload: DetectOutliersInCohort, history, algorithmConfig ) => {
      val outlierMarks = {
        cohortDistances( payload )
        .toList
        .map { DataPoint.toDoublePoints }
        .map { frameDistances =>
          val context = AlgorithmContext( message = c, data = frameDistances )

          cluster.run( context )
          .map { case (_, clusters) =>
            val isOutlier = makeOutlierTest( clusters )
            val ms = frameDistances.zipWithIndex collect { case (fd, i) if isOutlier( fd ) => i }
            ms.toList
          }
        }
      }

      val result = {
        outlierMarks
        .sequenceU
        .map { oms: List[List[Int]] =>
          val outlierSeries = oms.flatten.toSet[Int] map { index => payload.source.data( index ) }
          if ( outlierSeries.isEmpty ) NoOutliers( algorithms = Set(algorithm), source = payload.source, plan = payload.plan )
          else {
            CohortOutliers( algorithms = Set(algorithm), source = payload.source, outliers = outlierSeries, plan = payload.plan )
          }
        }
      }

      result match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => log.error( ex, "failed {} analysis on {}[{}]", algorithm.name, payload.topic, payload.source.interval )
      }
    }
  }

  def cohortDistances( underlying: DetectOutliersInCohort ): Matrix[DataPoint] = {
    for {
      frame <- underlying.source.toMatrix
      timestamp = frame.head._1 // all of frame's _1 better be the same!!!
      frameValues = frame map { _._2 }
      stats = new stat.DescriptiveStatistics( frameValues.toArray )
      median = stats.getPercentile( 50 )
    } yield frameValues map { v => DataPoint( timestamp, v - median ) }
  }

  //todo figure out how to unify into a single density algorithm
  override def findOutliers: Op[(AlgorithmContext, Seq[Cluster[DoublePoint]]), Outliers] = ???
}
