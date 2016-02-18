package lineup.analysis.outlier.algorithm

import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import org.scalatest.mock.MockitoSugar
import org.joda.{ time => joda }
import com.github.nscala_time.time.JodaImplicits._
import org.apache.commons.math3.random.RandomDataGenerator
import lineup.model.timeseries.{ TimeSeriesBase, TimeSeries, DataPoint, Row }
import lineup.testkit.ParallelAkkaSpec
import lineup.analysis.outlier.{ HistoricalStatistics, DetectOutliersInSeries, DetectUsing, DetectionAlgorithmRouter }
import lineup.model.outlier._


/**
  * Created by rolfsd on 2/15/16.
  */
class SkylineAnalyzerSpec extends ParallelAkkaSpec with MockitoSugar {
  import SkylineAnalyzerSpec._

  object Fixture {
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

  class Fixture extends AkkaFixture {
    implicit def scalaDurationToJoda( d: FiniteDuration ): joda.Duration = new joda.Duration( d.toMillis )

    val router = TestProbe()
    val aggregator = TestProbe()
    val plan = mock[OutlierPlan]
    when( plan.appliesTo ).thenReturn( Fixture.appliesToAll )
    when( plan.algorithms ).thenReturn(
      Set(
        SkylineAnalyzer.FirstHourAverageAlgorithm,
        SkylineAnalyzer.MeanSubtractionCumulationAlgorithm,
        SkylineAnalyzer.StddevFromAverageAlgorithm,
        SkylineAnalyzer.StddevFromMovingAverageAlgorithm,
        SkylineAnalyzer.LeastSquaresAlgorithm,
        SkylineAnalyzer.GrubbsAlgorithm,
        SkylineAnalyzer.HistogramBinsAlgorithm,
        SkylineAnalyzer.MedianAbsoluteDeviationAlgorithm,
        SkylineAnalyzer.KsTestAlgorithm
      )
    )

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
  }

  override def makeAkkaFixture(): Fixture = new Fixture

  "SkylineAnalyzer" should {
    "find outliers deviating from first hour" taggedAs (WIP) in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SkylineAnalyzer]( SkylineAnalyzer.props(router.ref) )
      val firstHour = analyzer.underlyingActor.firstHour
      trace( s"firstHour = $firstHour" )
      val full = makeDataPoints(
        values = points.map{ case DataPoint(_, v) => v },
        start = firstHour.start,
        period = 2.minutes,
        timeWiggle = (0.97, 1.03),
        valueWiggle = (0.95, 1.05)
      )

      val (front, last) = full.splitAt( full.size - 1 )
      val data = front ++ last.map { dp => dp.copy( value = 1000D ) }
      val series = TimeSeries( "series", data )
      trace( s"test series = $series" )
      val algoS = SkylineAnalyzer.FirstHourAverageAlgorithm
      val algProps = ConfigFactory.parseString(
        s"""
           |${algoS.name}.tolerance: 3
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( SkylineAnalyzer.FirstHourAverageAlgorithm ) )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), HistoricalStatistics(2, false), algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "first hour" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 2
          outliers mustBe Row( data(30), data.last )
        }
      }
    }

    "find outliers via Grubbs Test" in { f: Fixture =>
      import f._

      val full: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 10 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val (front, last) = full.splitAt( full.size - 1 )
      val data = front ++ last.map{ dp => dp.copy( value = 1000D ) }
      val series = TimeSeries( "series", data )

      val algoS = SkylineAnalyzer.GrubbsAlgorithm
      val algProps = ConfigFactory.parseString(
        s"""
         |${algoS.name}.tolerance: 3
        """.stripMargin
      )

      val analyzer = TestActorRef[SkylineAnalyzer]( SkylineAnalyzer.props(router.ref) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( SkylineAnalyzer.GrubbsAlgorithm ) )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), HistoricalStatistics(2, false), algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "grubbs" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Row( data.last )
        }
      }
    }
  }
}

object SkylineAnalyzerSpec {
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