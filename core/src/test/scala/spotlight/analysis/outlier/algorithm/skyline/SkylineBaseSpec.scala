package spotlight.analysis.outlier.algorithm.skyline

import akka.testkit._
import com.typesafe.scalalogging.LazyLogging
import spotlight.analysis.outlier.HistoricalStatistics
import spotlight.model.outlier._
import spotlight.model.timeseries.{DataPoint, Row, TimeSeries, TimeSeriesBase}
import spotlight.testkit.ParallelAkkaSpec
import org.apache.commons.math3.random.RandomDataGenerator
import org.joda.{time => joda}
import org.mockito.Mockito._
import org.scalatest.Tag
import org.scalatest.mock.MockitoSugar
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by rolfsd on 2/15/16.
  */
abstract class SkylineBaseSpec extends ParallelAkkaSpec with MockitoSugar with LazyLogging {
  object SkylineFixture {
    val appliesToAll: OutlierPlan.AppliesTo = {
      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification(0, 0)
      val reduce: ReduceOutliers = new ReduceOutliers {
        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: OutlierPlan
        )
        (
          implicit ec: ExecutionContext
        ): Future[Outliers] = Future.failed( new IllegalStateException("should not use" ) )
      }

      import scala.concurrent.duration._
      OutlierPlan.default( "", 1.second, isQuorun, reduce, Set.empty[Symbol] ).appliesTo
    }
  }

  abstract class SkylineFixture extends AkkaFixture {
    implicit def scalaDurationToJoda( d: FiniteDuration ): joda.Duration = new joda.Duration( d.toMillis )

    val router = TestProbe()
    val aggregator = TestProbe()

    def makeDataPoints(
      values: Row[Double],
      start: joda.DateTime = joda.DateTime.now,
      period: FiniteDuration = 1.second,
      timeWiggle: (Double, Double) = (1D, 1D),
      valueWiggle: (Double, Double) = (1D, 1D)
    ): Row[DataPoint] = {
      val secs = start.getMillis / 1000L
      val epochStart = new joda.DateTime( secs * 1000L )
      val random = new RandomDataGenerator
      def nextFactor( wiggle: (Double, Double) ): Double = {
        val (lower, upper) = wiggle
        if ( upper <= lower ) upper else random.nextUniform( lower, upper )
      }

      values.zipWithIndex map { vi =>
        import com.github.nscala_time.time.Imports._
        val (v, i) = vi
        val tadj = ( i * nextFactor(timeWiggle) ) * period
        val ts = epochStart + tadj.toJodaDuration
        val vadj = nextFactor( valueWiggle )
        DataPoint( timestamp = ts, value = (v * vadj) )
      }
    }

    def spike( data: Row[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
      val (front, last) = data.sortBy{ _.timestamp.getMillis }.splitAt( position )
      val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
      TimeSeries( "test-series", spiked )
    }

    def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = {
      prior map { h =>
        series.points.foldLeft( h ){ (history, dp) => history :+ dp.getPoint }
      } getOrElse {
        HistoricalStatistics.fromActivePoints( DataPoint.toDoublePoints(series.points).toArray, false )
      }
    }

    def tailAverage( data: Row[DataPoint], last: Row[DataPoint] = Row.empty[DataPoint], tailLength: Int = 3 ): Row[DataPoint] = {
      val l = last.drop( last.size - tailLength + 1 )

      data
      .map { _.timestamp }
      .zipWithIndex
      .map { case (ts, i) =>
        val valuesToAverage = if ( i < tailLength ) {
          val all = l ++ data.take( i + 1 )
          all.drop( all.size - tailLength )
        } else {
          data.drop( i - tailLength + 1 ).take( tailLength )
        }

        ( ts, valuesToAverage.map{ _.value } )
      }
      .map { case (ts, vs) => DataPoint( timestamp = ts, value = vs.sum / vs.size ) }
    }
  }

  object DONE extends Tag( "done" )
}

object SkylineBaseSpec {
  val points = Row(
    DataPoint( new joda.DateTime(440), 9.46 ),
    DataPoint( new joda.DateTime(441), 9.9 ),
    DataPoint( new joda.DateTime(442), 11.6 ),
    DataPoint( new joda.DateTime(443), 14.5 ),
    DataPoint( new joda.DateTime(444), 17.3 ),
    DataPoint( new joda.DateTime(445), 19.2 ),
    DataPoint( new joda.DateTime(446), 18.4 ),
    DataPoint( new joda.DateTime(447), 14.5 ),
    DataPoint( new joda.DateTime(448), 12.2 ),
    DataPoint( new joda.DateTime(449), 10.8 ),
    DataPoint( new joda.DateTime(450), 8.58 ),
    DataPoint( new joda.DateTime(451), 8.36 ),
    DataPoint( new joda.DateTime(452), 8.58 ),
    DataPoint( new joda.DateTime(453), 7.5 ),
    DataPoint( new joda.DateTime(454), 7.1 ),
    DataPoint( new joda.DateTime(455), 7.3 ),
    DataPoint( new joda.DateTime(456), 7.71 ),
    DataPoint( new joda.DateTime(457), 8.14 ),
    DataPoint( new joda.DateTime(458), 8.14 ),
    DataPoint( new joda.DateTime(459), 7.1 ),
    DataPoint( new joda.DateTime(460), 7.5 ),
    DataPoint( new joda.DateTime(461), 7.1 ),
    DataPoint( new joda.DateTime(462), 7.1 ),
    DataPoint( new joda.DateTime(463), 7.3 ),
    DataPoint( new joda.DateTime(464), 7.71 ),
    DataPoint( new joda.DateTime(465), 8.8 ),
    DataPoint( new joda.DateTime(466), 9.9 ),
    DataPoint( new joda.DateTime(467), 14.2 ),
    DataPoint( new joda.DateTime(468), 18.8 ),
    DataPoint( new joda.DateTime(469), 25.2 ),
    DataPoint( new joda.DateTime(470), 31.5 ),
    DataPoint( new joda.DateTime(471), 22 ),
    DataPoint( new joda.DateTime(472), 24.1 ),
    DataPoint( new joda.DateTime(473), 39.2 )
  )


  val pointsA = Row(
    DataPoint( new joda.DateTime(440), 9.46 ),
    DataPoint( new joda.DateTime(441), 9.9 ),
    DataPoint( new joda.DateTime(442), 11.6 ),
    DataPoint( new joda.DateTime(443), 14.5 ),
    DataPoint( new joda.DateTime(444), 17.3 ),
    DataPoint( new joda.DateTime(445), 19.2 ),
    DataPoint( new joda.DateTime(446), 18.4 ),
    DataPoint( new joda.DateTime(447), 14.5 ),
    DataPoint( new joda.DateTime(448), 12.2 ),
    DataPoint( new joda.DateTime(449), 10.8 ),
    DataPoint( new joda.DateTime(450), 8.58 ),
    DataPoint( new joda.DateTime(451), 8.36 ),
    DataPoint( new joda.DateTime(452), 8.58 ),
    DataPoint( new joda.DateTime(453), 7.5 ),
    DataPoint( new joda.DateTime(454), 7.1 ),
    DataPoint( new joda.DateTime(455), 7.3 ),
    DataPoint( new joda.DateTime(456), 7.71 ),
    DataPoint( new joda.DateTime(457), 8.14 ),
    DataPoint( new joda.DateTime(458), 8.14 ),
    DataPoint( new joda.DateTime(459), 7.1 ),
    DataPoint( new joda.DateTime(460), 7.5 ),
    DataPoint( new joda.DateTime(461), 7.1 ),
    DataPoint( new joda.DateTime(462), 7.1 ),
    DataPoint( new joda.DateTime(463), 7.3 ),
    DataPoint( new joda.DateTime(464), 7.71 ),
    DataPoint( new joda.DateTime(465), 8.8 ),
    DataPoint( new joda.DateTime(466), 9.9 ),
    DataPoint( new joda.DateTime(467), 14.2 )
  )

  val pointsB = Row(
    DataPoint( new joda.DateTime(440), 10.1 ),
    DataPoint( new joda.DateTime(441), 10.1 ),
    DataPoint( new joda.DateTime(442), 9.68 ),
    DataPoint( new joda.DateTime(443), 9.46 ),
    DataPoint( new joda.DateTime(444), 10.3 ),
    DataPoint( new joda.DateTime(445), 11.6 ),
    DataPoint( new joda.DateTime(446), 13.9 ),
    DataPoint( new joda.DateTime(447), 13.9 ),
    DataPoint( new joda.DateTime(448), 12.5 ),
    DataPoint( new joda.DateTime(449), 11.9 ),
    DataPoint( new joda.DateTime(450), 12.2 ),
    DataPoint( new joda.DateTime(451), 13 ),
    DataPoint( new joda.DateTime(452), 13.3 ),
    DataPoint( new joda.DateTime(453), 13 ),
    DataPoint( new joda.DateTime(454), 12.7 ),
    DataPoint( new joda.DateTime(455), 11.9 ),
    DataPoint( new joda.DateTime(456), 13.3 ),
    DataPoint( new joda.DateTime(457), 12.5 ),
    DataPoint( new joda.DateTime(458), 11.9 ),
    DataPoint( new joda.DateTime(459), 11.6 ),
    DataPoint( new joda.DateTime(460), 10.5 ),
    DataPoint( new joda.DateTime(461), 10.1 ),
    DataPoint( new joda.DateTime(462), 9.9 ),
    DataPoint( new joda.DateTime(463), 9.68 ),
    DataPoint( new joda.DateTime(464), 9.68 ),
    DataPoint( new joda.DateTime(465), 9.9 ),
    DataPoint( new joda.DateTime(466), 10.8 ),
    DataPoint( new joda.DateTime(467), 11 )
  )
}