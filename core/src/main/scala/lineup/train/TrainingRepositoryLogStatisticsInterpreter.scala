package lineup.train

import java.util.concurrent.ExecutorService
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import scalaz.concurrent.Task
import com.typesafe.scalalogging.Logger
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.log.Trace
import peds.commons.math.MahalanobisDistance
import lineup.model.timeseries.{ Topic, TimeSeriesCohort, TimeSeries }



/**
  * Created by rolfsd on 1/18/16.
  */
case class TrainingRepositoryLogStatisticsInterpreter(
  logger: Logger
)(
  implicit ec: ExecutionContext
) extends TrainingRepository.Interpreter {
  import TrainingRepositoryLogStatisticsInterpreter._
  import TrainingRepository._

  val trace = Trace[TrainingRepositoryLogStatisticsInterpreter]

  implicit val pool: ExecutorService = TrainingRepositoryLogStatisticsInterpreter.ExecutionContextExecutorServiceBridge( ec )

  writeHeader().unsafePerformSync

  override def step[Next]( action: TrainingProtocolF[TrainingProtocol[Next]] ): Task[TrainingProtocol[Next]] = {
    action match {
      case PutTimeSeries(series, next) => write( seriesStatistics(series) ) map { _ => next }
      case PutTimeSeriesCohort(cohort, next) => write( cohortStatistics(cohort) ) map { _ => next }
    }
  }

  def writeHeader(): Task[Unit] = trace.block( "writeHeader" ) {
    trace( s"HEADER = ${Statistics.header}")
    log( Statistics.header )
  }
  def write( stats: Statistics ): Task[Unit] = trace.block( "write(stats)" ) { log( stats.toString ) }
  def log( line: String ): Task[Unit] = trace.block( s"log(${line.take(10)})" ) { Task { logger debug line }( pool ) }
}

object TrainingRepositoryLogStatisticsInterpreter {
  def seriesStatistics( series: TimeSeries ): Statistics = {
    val valueStats = new DescriptiveStatistics
    series.points foreach { valueStats addValue _.value }

    val data = series.points map { p => Array(p.timestamp.getMillis.toDouble, p.value) }

    val euclideanDistance = new org.apache.commons.math3.ml.distance.EuclideanDistance
    val mahalanobisDistance = MahalanobisDistance( MatrixUtils.createRealMatrix(data.toArray) )
    val edStats = new DescriptiveStatistics //acc euclidean stats
    val mdStats = new DescriptiveStatistics //acc mahalanobis stats

    val distances = data.take( data.size - 1 ).zip( data drop 1 ) map { case (l, r) =>
      val e = euclideanDistance.compute( l, r )
      val m = mahalanobisDistance.compute( l, r )
      edStats addValue e
      mdStats addValue m
      ( e, m )
    }

    val (ed, md) = distances.unzip
    val edOutliers = ed count { _ > edStats.getPercentile(95) }
    val mdOutliers = md count { _ > mdStats.getPercentile(95) }

    Statistics(
      source = series.topic,
      points = valueStats,
      mahalanobis = mdStats,
      mahalanobisOutliers = mdOutliers,
      euclidean = edStats,
      euclideanOutliers = edOutliers
    )
  }

  def cohortStatistics( cohort: TimeSeriesCohort ): Statistics = {
    val st = new DescriptiveStatistics
    for {
      s <- cohort.data
      p <- s.points
    } { st addValue p.value }

    Statistics(
      source = cohort.topic,
      points = st,
      mahalanobis = new DescriptiveStatistics,
      mahalanobisOutliers = 0,
      euclidean = new DescriptiveStatistics,
      euclideanOutliers = 0
    )
  }

  case class Statistics private[train](
    source: Topic,
    points: DescriptiveStatistics,
    mahalanobis: DescriptiveStatistics,
    mahalanobisOutliers: Int,
    euclidean: DescriptiveStatistics,
    euclideanOutliers: Int
  ) {
    override def toString: String = {
      source.name + "," +
      points.getN + "," +
      points.getMin + "," +
      points.getMax + "," +
      points.getMean + "," +
      points.getStandardDeviation + "," +
      points.getPercentile(50) + "," +
      points.getSkewness + "," +
      points.getKurtosis + "," +
      mahalanobis.getN + "," +
      mahalanobis.getMin + "," +
      mahalanobis.getMax + "," +
      mahalanobis.getMean + "," +
      mahalanobis.getStandardDeviation + "," +
      mahalanobis.getPercentile(50) + "," +
      mahalanobis.getSkewness + "," +
      mahalanobis.getKurtosis + "," +
      mahalanobis.getPercentile(95) + "," +
      mahalanobis.getPercentile(99) + "," +
      mahalanobisOutliers + "," +
      euclidean.getN + "," +
      euclidean.getMin + "," +
      euclidean.getMax + "," +
      euclidean.getMean + "," +
      euclidean.getStandardDeviation + "," +
      euclidean.getPercentile(50) + "," +
      euclidean.getSkewness + "," +
      euclidean.getKurtosis + "," +
      euclidean.getPercentile(95) + "," +
      euclidean.getPercentile(99) + "," +
      euclideanOutliers
    }
  }

  object Statistics {
    def header: String = {
      "metric" + "," +
      "points_N" + "," +
      "points_Min" + "," +
      "points_Max" + "," +
      "points_Mean" + "," +
      "points_StandardDeviation" + "," +
      "points_Median" + "," +
      "points_Skewness" + "," +
      "points_Kurtosis" + "," +
      "mahalanobis_N" + "," +
      "mahalanobis_Min" + "," +
      "mahalanobis_Max" + "," +
      "mahalanobis_Mean" + "," +
      "mahalanobis_StandardDeviation" + "," +
      "mahalanobis_Median" + "," +
      "mahalanobis_Skewness" + "," +
      "mahalanobis_Kurtosis" + "," +
      "mahalanobis_95th" + "," +
      "mahalanobis_99th" + "," +
      "mahalanobis_Outliers" + "," +
      "euclidean_N" + "," +
      "euclidean_Min" + "," +
      "euclidean_Max" + "," +
      "euclidean_Mean" + "," +
      "euclidean_StandardDeviation" + "," +
      "euclidean_Median" + "," +
      "euclidean_Skewness" + "," +
      "euclidean_Kurtosis" + "," +
      "euclidean_95th" + "," +
      "euclidean_99th" + "," +
      "euclidean_Outliers"
    }
  }


  object ExecutionContextExecutorServiceBridge {
    import java.util.Collections
    import java.util.concurrent.{ TimeUnit, AbstractExecutorService }
    import scala.concurrent.{ ExecutionContextExecutorService, ExecutionContext }

    def apply( ec: ExecutionContext ): ExecutionContextExecutorService = {
      ec match {
        case null => throw null
        case eces: ExecutionContextExecutorService => eces
        case other => new AbstractExecutorService with ExecutionContextExecutorService {
          override def prepare(): ExecutionContext = other
          override def isShutdown = false
          override def isTerminated = false
          override def shutdown() = ()
          override def shutdownNow() = Collections.emptyList[Runnable]
          override def execute(runnable: Runnable): Unit = other execute runnable
          override def reportFailure(t: Throwable): Unit = other reportFailure t
          override def awaitTermination(length: Long,unit: TimeUnit): Boolean = false
        }
      }
    }
  }
}
