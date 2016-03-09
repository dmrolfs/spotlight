package spotlight.train

import java.util.concurrent.ExecutorService
import scala.concurrent.ExecutionContext
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import com.typesafe.scalalogging.Logger
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.V
import peds.commons.log.Trace
import peds.commons.math.MahalanobisDistance
import spotlight.model.timeseries.{ Topic, TimeSeriesCohort, TimeSeries }


/**
  * Created by rolfsd on 1/18/16.
  */
case class LogStatisticsTrainingRepositoryInterpreter(
  logger: Logger
)(
  implicit ec: ExecutionContext
) extends TrainingRepository.Interpreter {
  import LogStatisticsTrainingRepositoryInterpreter._
  import TrainingRepository._

  val trace = Trace[LogStatisticsTrainingRepositoryInterpreter]

  implicit val pool: ExecutorService = executionContextToService( ec )

  writeHeader().unsafePerformSync

  override def step[Next]( action: TrainingProtocolF[TrainingProtocol[Next]] ): Task[TrainingProtocol[Next]] = {
    action match {
      case PutTimeSeries(series, next) => write( seriesStatistics(series) ) map { _ => next }
      case PutTimeSeriesCohort(cohort, next) => write( cohortStatistics(cohort) ) map { _ => next }
    }
  }

  def writeHeader(): Task[Unit] = trace.block( "writeHeader" ) { log( Statistics.header ) }

  def write( stats: V[Statistics] ): Task[Unit] = trace.block( "write(stats)" ) {
    implicit val showErrors = Show.shows[NonEmptyList[Throwable]]{ _.map{ _.getMessage }.toList.mkString("; ") }
    log( stats.map{ _.toString }.shows )
  }

  def log( line: String ): Task[Unit] = trace.block( s"log(${line.take(10)})" ) { Task { logger debug line }( pool ) }
}

object LogStatisticsTrainingRepositoryInterpreter {
  def seriesStatistics( series: TimeSeries ): V[Statistics] = {
    val valueStats = new DescriptiveStatistics
    series.points foreach { valueStats addValue _.value }

    val data = series.points map { p => Array(p.timestamp.getMillis.toDouble, p.value) }

    val result = for {
      mahalanobisDistance <- MahalanobisDistance.fromPoints( MatrixUtils.createRealMatrix(data.toArray) ).disjunction
    } yield {
      val edStats = new DescriptiveStatistics //acc euclidean stats
      val mdStats = new DescriptiveStatistics //acc mahalanobis stats

      val euclideanDistance = new org.apache.commons.math3.ml.distance.EuclideanDistance

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

    result.leftMap { _ map { ex => DistanceError( series.topic, ex ) } }
  }

  def cohortStatistics( cohort: TimeSeriesCohort ): V[Statistics] = {
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
    ).right
  }


  final case class DistanceError private[train]( topic: Topic, root: Throwable ) extends Throwable {
    override def getMessage: String = s"[${topic}] could not create distance: [${root}]"
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
}
