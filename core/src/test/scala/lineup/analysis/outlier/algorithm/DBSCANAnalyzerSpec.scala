package lineup.analysis.outlier.algorithm

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger
import akka.event.EventStream
import akka.testkit._
import shapeless._
import org.joda.{ time => joda }
import com.github.nscala_time.time.OrderingImplicits._
import org.apache.commons.math3.random.RandomDataGenerator
import org.scalatest.mock.MockitoSugar
import peds.akka.envelope._
import lineup.model.outlier.{NoOutliers, CohortOutliers, SeriesOutliers}
import lineup.model.timeseries._
import lineup.analysis.outlier._
import lineup.testkit.ParallelAkkaSpec


/**
 * Created by damonrolfs on 9/18/14.
 */
class DBSCANAnalyzerSpec extends ParallelAkkaSpec with MockitoSugar {
  import DBSCANAnalyzerSpec._

  class Fixture extends AkkaFixture {
    val router = TestProbe()
    val aggregator = TestProbe()
    val bus = mock[EventStream]
    val randomGenerator = new RandomDataGenerator
    def tweakSeries( s: TimeSeries, factorRange: (Double, Double) ): TimeSeries = {
      val factor = randomGenerator.nextUniform( factorRange._1, factorRange._2, true )
      val valueLens = lens[DataPoint] >> 'value
      val tweaked = s.points map { dp => valueLens.set( dp )( dp.value * factor ) }
      TimeSeries.pointsLens.set( s )( tweaked )
    }
  }

  override def makeAkkaFixture(): Fixture = new Fixture

  "DBSCANAnalyzer" should  {
    "register with router upon create" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[DBSCANAnalyzer]( DBSCANAnalyzer.props(router.ref) )
      router.expectMsgPF( 1.second.dilated, "register" ) {
        case DetectionAlgorithmRouter.RegisterDetectionAlgorithm(algorithm, _) => algorithm must equal( 'dbscan )
      }
    }

    "raise error if used before registration" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[DBSCANAnalyzer]( DBSCANAnalyzer.props(router.ref) )

      an [AlgorithmActor.AlgorithmUsedBeforeRegistrationError] must be thrownBy
      analyzer.receive(
        DetectUsing(
          'dbscan,
          aggregator.ref,
          DetectOutliersInSeries( TimeSeries("series", points) ),
          None,
          ConfigFactory.parseString(
            s"""
               |${DBSCANAnalyzer.Eps}: 5.0
               |${DBSCANAnalyzer.MinDensityConnectedPoints}: 3
             """.stripMargin
          )
        )
      )
    }

    "detect outlying points in series" taggedAs (WIP) in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[DBSCANAnalyzer]( DBSCANAnalyzer.props(router.ref) )
      val series = TimeSeries( "series", points )
      val expectedValues = Row( 18.8, 25.2, 31.5, 22.0, 24.1, 39.2 )
      val expected = points filter { expectedValues contains _.value } sortBy { _.timestamp }

      val algProps = ConfigFactory.parseString(
        s"""
           |${DBSCANAnalyzer.Eps}: 5.0
           |${DBSCANAnalyzer.MinDensityConnectedPoints}: 3
           |${DBSCANAnalyzer.Distance}: Euclidean
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( 'dbscan ) )
      analyzer.receive( DetectUsing( 'dbscan, aggregator.ref, DetectOutliersInSeries(series), None, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        //todo stream envelope
        //        case Envelope(SeriesOutliers(alg, source, outliers), hdr) => {
        //          alg must equal( 'dbscan )
        //          source mustBe series
        //          outliers.size mustBe 6
        //        }
        case m @ SeriesOutliers(alg, source, outliers) => {
          alg mustBe Set('dbscan)
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 6
          outliers mustBe expected
        }
      }
    }

    "detect no outliers points in series" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[DBSCANAnalyzer]( DBSCANAnalyzer.props(router.ref) )

      val myPoints = Row(
        DataPoint( new joda.DateTime(448), 8.46 ),
        DataPoint( new joda.DateTime(449), 8.9 ),
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
        DataPoint( new joda.DateTime(466), 8.9 )
      )

      val series = TimeSeries( "series", myPoints )

      val algProps = ConfigFactory.parseString(
        s"""
           |${DBSCANAnalyzer.Eps}: 5.0
           |${DBSCANAnalyzer.MinDensityConnectedPoints}: 3
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered('dbscan) )
      analyzer.receive( DetectUsing( 'dbscan, aggregator.ref, DetectOutliersInSeries(series), None, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        //todo stream envelope
        //        case Envelope(SeriesOutliers(alg, source, outliers), hdr) => {
        //          alg must equal( 'dbscan )
        //          source mustBe series
        //          outliers.size mustBe 6
        //        }
        case m @ NoOutliers(alg, source) => {
          alg mustBe Set('dbscan)
          source mustBe series
          m.hasAnomalies mustBe false
        }
      }
    }

    "detect outlying series in cohort" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[DBSCANAnalyzer]( DBSCANAnalyzer.props(router.ref) )
      val series1 = TimeSeries( "series.one", pointsA )
      val range = (0.99998, 1.00003)
      val series2 = tweakSeries( TimeSeries("series.two", pointsB), range )
      val series3 = tweakSeries( TimeSeries("series.three", pointsB), range )
      val series4 = tweakSeries( TimeSeries("series.four", pointsB), range )
      val cohort = TimeSeriesCohort( series1, series2, series3, series4 )

      val algProps = ConfigFactory.parseString(
        s"""
           |${DBSCANAnalyzer.Eps}: 5.0
           |${DBSCANAnalyzer.MinDensityConnectedPoints}: 2
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered('dbscan) )
      analyzer.receive( DetectUsing( 'dbscan, aggregator.ref, DetectOutliersInCohort(cohort), None, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        //todo stream envelope
        //        case Envelope(CohortOutliers(alg, source, outliers), hdr) => {
        //          trace( s"ENVELOPE HEADER = $hdr" )
        //          trace( s"""outliers=[${outliers.mkString(",")}]""" )
        //          alg must equal( 'dbscan )
        //          source mustBe cohort
        //          outliers.size mustBe 1
        //          outliers.head.name mustBe "series.one"
        //        }
        case m @ CohortOutliers(alg, source, outliers) => {
          trace( s"""outliers=[${outliers.mkString(",")}]""" )
          alg mustBe Set('dbscan)
          source mustBe cohort
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers.head.topic mustBe Topic("series.one")
        }
      }
    }

    "detect no outliers series in cohort" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[DBSCANAnalyzer]( DBSCANAnalyzer.props(router.ref) )
      val series1 = TimeSeries( "series.one", pointsB )
      val range = (0.99998, 1.00003)
      val series2 = tweakSeries( TimeSeries("series.two", pointsB), range )
      val series3 = tweakSeries( TimeSeries("series.three", pointsB), range )
      val series4 = tweakSeries( TimeSeries("series.four", pointsB), range )
      val cohort = TimeSeriesCohort( series1, series2, series3, series4 )

      val algProps = ConfigFactory.parseString(
        s"""
           |${DBSCANAnalyzer.Eps}: 5.0
           |${DBSCANAnalyzer.MinDensityConnectedPoints}: 2
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered('dbscan) )
      analyzer.receive( DetectUsing( 'dbscan, aggregator.ref, DetectOutliersInCohort(cohort), None, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        //todo stream envelope
        //        case Envelope(CohortOutliers(alg, source, outliers), hdr) => {
        //          trace( s"ENVELOPE HEADER = $hdr" )
        //          trace( s"""outliers=[${outliers.mkString(",")}]""" )
        //          alg must equal( 'dbscan )
        //          source mustBe cohort
        //          outliers.size mustBe 1
        //          outliers.head.name mustBe "series.one"
        //        }
        case m @ NoOutliers(alg, source) => {
          alg mustBe Set('dbscan)
          source mustBe cohort
          m.hasAnomalies mustBe false
        }
      }
    }

    "handle no clusters found" in { f: Fixture => pending }
  }
}

object DBSCANAnalyzerSpec {
  val sysId = new AtomicInteger()

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
