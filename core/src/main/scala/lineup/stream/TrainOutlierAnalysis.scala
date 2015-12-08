package lineup.stream

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ ExecutionContext, Future }
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.Logger
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.slf4j.LoggerFactory
import peds.commons.math.MahalanobisDistance
import lineup.model.outlier.Outliers
import lineup.model.timeseries.{ TimeSeriesCohort, TimeSeries, Topic }


/**
  * Created by rolfsd on 12/7/15.
  */
object TrainOutlierAnalysis {
  def feedOutlierTraining( implicit system: ActorSystem ): Flow[Outliers, Outliers, Unit] = {
    import org.apache.commons.math3.stat.descriptive._

    writeHeader()

    val elementsSeen = new AtomicInteger()

    Flow[Outliers]
    .map { e => (elementsSeen.incrementAndGet(), e) }
    .map { case (i, o) =>
      val stats  = o.source match {
        case s: TimeSeries => {
          val valueStats = new DescriptiveStatistics
          s.points foreach { valueStats addValue _.value }

          val data = s.points map { p => Array(p.timestamp.getMillis.toDouble, p.value) }

          val euclideanDistance = new org.apache.commons.math3.ml.distance.EuclideanDistance
          val mahalanobisDistance = MahalanobisDistance( MatrixUtils.createRealMatrix(data.toArray) )
          val edStats = new DescriptiveStatistics//acc euclidean stats
          val mdStats = new DescriptiveStatistics //acc mahalanobis stats

          val distances = data.take(data.size - 1).zip(data.drop(1)) map { case (l, r) =>
            val e = euclideanDistance.compute( l, r )
            val m = mahalanobisDistance.compute( l, r )
            edStats addValue e
            mdStats addValue m
            (e, m)
          }

          val (ed, md) = distances.unzip
          val edOutliers = ed count { _ > edStats.getPercentile(95) }
          val mdOutliers = md count { _ > mdStats.getPercentile(95) }

          Statistics(
            source = s.topic,
            points = valueStats,
            mahalanobis = mdStats,
            mahalanobisOutliers = mdOutliers,
            euclidean = edStats,
            euclideanOutliers = edOutliers
          )
        }

        case c: TimeSeriesCohort => {
          val st = new DescriptiveStatistics
          for {
            s <- c.data
            p <- s.points
          } { st addValue p.value }

          Statistics(
            source = c.topic,
            points = st,
            mahalanobis = new DescriptiveStatistics,
            mahalanobisOutliers = 0,
            euclidean = new DescriptiveStatistics,
            euclideanOutliers = 0
          )
        }
      }

      write( stats )
      o
    }
  }

  def writeHeader()( implicit system: ActorSystem ): Future[Unit] = log( Statistics.header )

  def write( stats: Statistics )( implicit system: ActorSystem ): Future[Unit] = log( stats.toString )

  private val trainingLogger: Logger = Logger( LoggerFactory getLogger "Training" )

  private def loggerDispatcher( system: ActorSystem ): ExecutionContext = system.dispatchers lookup "logger-dispatcher"

  private def log( line: String )( implicit system: ActorSystem ): Future[Unit] = {
    Future { trainingLogger debug line }( loggerDispatcher(system) )
  }



  case class Statistics(
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
