package lineup.analysis.outlier.algorithm

import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.ml.distance.EuclideanDistance

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger

import akka.event.EventStream
import akka.testkit._
import shapeless._
import org.joda.{time => joda}
import com.github.nscala_time.time.OrderingImplicits._
import org.apache.commons.math3.random.RandomDataGenerator
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import lineup.model.outlier._
import lineup.model.timeseries._
import lineup.analysis.outlier._
import lineup.testkit.ParallelAkkaSpec
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics


/**
 * Created by damonrolfs on 9/18/14.
 */
class SeriesDensityAnalyzerSpec extends ParallelAkkaSpec with MockitoSugar {
  import SeriesDensityAnalyzerSpec._

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

      OutlierPlan.default( "", 1.second, isQuorun, reduce, Set.empty[Symbol] ).appliesTo
    }
  }

  class Fixture extends AkkaFixture {
    val metric = Topic( "metric.a" )
    val algoS = SeriesDensityAnalyzer.Algorithm
    val algoC = CohortDensityAnalyzer.Algorithm
    val plan = mock[OutlierPlan]
    when( plan.appliesTo ).thenReturn( Fixture.appliesToAll )

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

    def makeDensityExpectedHistory(points: Seq[DataPoint], start: Option[DescriptiveStatistics], last: Option[DataPoint] ): DescriptiveStatistics = {
      val initial = start getOrElse { new DescriptiveStatistics }
//      val last = initialH.lastPoints.lastOption map { new DoublePoint( _ ) }
      val dps = DataPoint toDoublePoints points
      val basis = last map { l => dps.zip( DataPoint.toDoublePoint(l) +: dps ) } getOrElse { (dps drop 1).zip( dps ) }
      basis.foldLeft( initial ) { case (s, (c, p)) =>
        val ts = c.getPoint.head
        val d = new EuclideanDistance().compute( p.getPoint, c.getPoint )
        s addValue d
        s
      }
    }
  }

  def assertHistoricalStats( actual: HistoricalStatistics, expected: HistoricalStatistics ): Unit = {
    actual.dimension mustBe expected.dimension
    actual.N mustBe expected.N
    actual.max mustBe expected.max
    actual.mean mustBe expected.mean
    actual.min mustBe expected.min
    actual.sum mustBe expected.sum
    actual.lastPoints.flatten mustBe expected.lastPoints.flatten
    actual.geometricMean mustBe expected.geometricMean
    actual.standardDeviation mustBe expected.standardDeviation
    actual.sumLog mustBe expected.sumLog
    actual.sumOfSquares mustBe expected.sumOfSquares
    actual.covariance mustBe expected.covariance
  }

  def assertDescriptiveStats(actual: DescriptiveStatistics, expected: DescriptiveStatistics ): Unit = {
    actual.getN mustBe expected.getN
    actual.getMax mustBe expected.getMax
    actual.getMean mustBe expected.getMean
    actual.getMin mustBe expected.getMin
    actual.getSum mustBe expected.getSum
    actual.getGeometricMean mustBe expected.getGeometricMean
    actual.getStandardDeviation mustBe expected.getStandardDeviation
    actual.getSumsq mustBe expected.getSumsq
  }


  override def makeAkkaFixture(): Fixture = new Fixture

  "SeriesDensityAnalyzer" should  {
    "register with router upon create" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props(router.ref) )
      router.expectMsgPF( 1.second.dilated, "register" ) {
        case DetectionAlgorithmRouter.RegisterDetectionAlgorithm(algorithm, _) => algorithm must equal( algoS )
      }
    }

    "raise error if used before registration" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props(router.ref) )

      an [AlgorithmActor.AlgorithmUsedBeforeRegistrationError] must be thrownBy
      analyzer.receive(
        DetectUsing(
          algoS,
          aggregator.ref,
          DetectOutliersInSeries( TimeSeries("series", points), plan ),
          HistoricalStatistics(2, false),
          ConfigFactory.parseString(
            s"""
               |${algoS.name}.seedEps: 5.0
               |${algoS.name}.minDensityConnectedPoints: 3
             """.stripMargin
          )
        )
      )
    }

    "detect outlying points in series" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props(router.ref) )
      val series = TimeSeries( "series", points )
      val expectedValues = Row( 18.8, 25.2, 31.5, 22.0, 24.1, 39.2 )
      val expected = points filter { expectedValues contains _.value } sortBy { _.timestamp }
      val algProps = ConfigFactory.parseString(
        s"""
           |${algoS.name}.tolerance: 1.70532639  // to bring effective eps to 5.0
           |${algoS.name}.seedEps: 1.0
           |${algoS.name}.minDensityConnectedPoints: 3
           |${algoS.name}.distance: Euclidean
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries( series, plan ), HistoricalStatistics(2, false), algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 6
          outliers mustBe expected
        }
      }
    }

    "detect no outliers points in series" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props(router.ref) )

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
           |${algoS.name}.seedEps: 5.0
           |${algoS.name}.minDensityConnectedPoints: 3
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries( series, plan ), HistoricalStatistics(2, false), algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        //todo stream envelope
        //        case Envelope(SeriesOutliers(alg, source, outliers), hdr) => {
        //          alg must equal( 'dbscan )
        //          source mustBe series
        //          outliers.size mustBe 6
        //        }
        case m @ NoOutliers(alg, source, plan) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe false
        }
      }
    }

    "series analyzer doesn't recognize cohort" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props(router.ref) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )

      val series1 = TimeSeries( "series.one", pointsA )
      val range = (0.99998, 1.00003)
      val series2 = tweakSeries( TimeSeries("series.two", pointsB), range )
      val series3 = tweakSeries( TimeSeries("series.three", pointsB), range )
      val series4 = tweakSeries( TimeSeries("series.four", pointsB), range )
      val cohort = TimeSeriesCohort( series1, series2, series3, series4 )

      val algProps = ConfigFactory.parseString(
        s"""
           |${algoC.name}.eps: 5.0
           |${algoC.name}.minDensityConnectedPoints: 2
        """.stripMargin
      )

      val expected = DetectUsing( algoC, aggregator.ref, DetectOutliersInCohort(cohort, plan), HistoricalStatistics(2, false), algProps )
      analyzer.receive( expected )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        case UnrecognizedPayload( alg, actual ) => {
          alg.name mustBe algoS.name
          actual mustBe expected
        }
      }
    }


    "history is updated with each detect request" taggedAs (WIP) in { f: Fixture =>
      import f._

      def detectUsing( message: OutlierDetectionMessage, history: HistoricalStatistics ): DetectUsing = {
        val config = ConfigFactory.parseString(
          s"""
             |${algoS.name}.tolerance: 3
             |${algoS.name}.seedEps: 1.0
             |${algoS.name}.minDensityConnectedPoints: 3
             |${algoS.name}.distance: Euclidean
           """.stripMargin
        )

        DetectUsing( algorithm = algoS, aggregator.ref, payload = message, history, properties = config )
      }

      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props( router.ref ) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )

      val detectHistoryA = HistoricalStatistics.fromActivePoints( DataPoint.toDoublePoints(pointsA).toArray, false )
      val msgA = detectUsing(
        message = OutlierDetectionMessage( TimeSeries(topic = metric, points = pointsA), plan ).toOption.get,
        history = detectHistoryA
      )

      val densityExpectedA = makeDensityExpectedHistory( pointsA, None, None )
      densityExpectedA.getN mustBe ( pointsA.size - 1)

      assertDescriptiveStats(
        analyzer.underlyingActor.algorithmContext.run(msgA).toOption.get.asInstanceOf[SeriesDensityAnalyzer.Context].distanceHistory,
        densityExpectedA
      )

      detectHistoryA.N mustBe (pointsA.size)
      val detectHistoryARecorded = detectHistoryA.recordLastDataPoints( pointsA )
      detectHistoryARecorded.N mustBe (pointsA.size)
      val detectHistoryAB = DataPoint.toDoublePoints( pointsB ).foldLeft( detectHistoryARecorded ){ (h, dp) => h :+ dp.getPoint }
      detectHistoryAB.N mustBe (pointsA.size + pointsB.size )
      val detectHistoryABLast: Seq[Point] = detectHistoryAB.lastPoints
      val pointsALast: Seq[Point] = pointsA.drop( pointsA.size - 3 ).map{ dp => Array(dp.timestamp.getMillis.toDouble, dp.value) }.toList
      detectHistoryABLast.size mustBe pointsALast.size
      detectHistoryABLast.zip(pointsALast).foreach{ case (f, b) => f mustBe b }

      val msgAB = detectUsing(
        message = OutlierDetectionMessage( TimeSeries( topic = metric, points = pointsB ), plan ).toOption.get,
        history = detectHistoryAB
      )
      val densityExpectedAB = makeDensityExpectedHistory( pointsB, Some(densityExpectedA), pointsA.lastOption )

      trace( s"pointsA.size = ${pointsA.size}" )
      trace( s"pointsB.size = ${pointsB.size}" )
      trace( s"expectedA.n = ${densityExpectedA.getN}" )
      trace( s"expectedAB.N = ${densityExpectedAB.getN}" )
      densityExpectedAB.getN mustBe ( pointsA.size - 1 + pointsB.size )
      trace( s"historyAB.n = ${detectHistoryAB.N}" )
      trace( s"expectedAB = $densityExpectedAB" )
      trace( s"historyAB = ${detectHistoryAB}" )
      assertDescriptiveStats(
        analyzer.underlyingActor.algorithmContext.run(msgAB).toOption.get.asInstanceOf[SeriesDensityAnalyzer.Context].distanceHistory,
        densityExpectedAB
      )

      val analyzerB = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props( router.ref ) )
      analyzerB.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )

      val densityExpectedB_A = makeDensityExpectedHistory( pointsA, None, None )
      analyzerB.underlyingActor._scopedContexts.get( HistoryKey(plan, metric) ) mustBe None
      analyzerB receive msgA
      aggregator.expectMsgPF( 2.seconds.dilated, "default-foo-A" ) {
        case m: SeriesOutliers => {
          assertDescriptiveStats(
            analyzerB.underlyingActor._scopedContexts( HistoryKey(plan, metric) ).distanceHistory,
            densityExpectedB_A
          )
          m.topic mustBe metric
          m.algorithms mustBe Set(algoS)
          m.anomalySize mustBe 10
          m.outliers mustBe pointsA.take(9) :+ pointsA.last
        }
      }

      val densityExpectedB_AB = makeDensityExpectedHistory( pointsB, Option(densityExpectedB_A), pointsA.lastOption )
      analyzerB receive msgAB
      aggregator.expectMsgPF( 2.seconds.dilated, "default-foo-AB" ){
        case m: SeriesOutliers => {
          assertDescriptiveStats(
            analyzerB.underlyingActor._scopedContexts( HistoryKey(plan, metric) ).distanceHistory,
            densityExpectedB_AB
          )
          m.topic mustBe metric
          m.algorithms mustBe Set(algoS)
          m.anomalySize mustBe 6
          m.outliers mustBe pointsB.take(6)
        }
      }
//      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar-A" ) {
//        case m @ DetectUsing( algo, _, payload, history, properties ) => {
//          m.topic mustBe metric
//          algo must equal('bar)
//          history.get.n mustBe pointsA.size
//          assertHistoricalStats( history.get, expectedA )
//        }
//      }
//
//      val expectedAB = DataPoint.toDoublePoints( pointsB ).foldLeft( expectedA ){ (h, dp) => h.add( dp.getPoint ) }
//      expectedAB.n mustBe (pointsA.size + pointsB.size)
//      trace( s"expectedAB = $expectedAB" )
//
//      val msgB = OutlierDetectionMessage( TimeSeries( topic = metric, points = pointsB ), defaultPlan ).toOption.get
//      detect receive msgB
//
//      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo-AB" ) {
//        case m @ DetectUsing( algo, _, payload, history, properties ) => {
//          trace( s"history = $history" )
//          m.topic mustBe metric
//          algo must equal('foo)
//          history.get.n mustBe (pointsA.size + pointsB.size)
//          assertHistoricalStats( history.get, expectedAB )
//        }
//      }
//
//      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar-AB" ) {
//        case m @ DetectUsing( algo, _, payload, history, properties ) => {
//          m.topic mustBe metric
//          algo must equal('bar)
//          history.get.n mustBe (pointsA.size + pointsB.size)
//          assertHistoricalStats( history.get, expectedAB )
//        }
//      }
    }

    "handle no clusters found" in { f: Fixture => pending }
  }
}

object SeriesDensityAnalyzerSpec {
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
    DataPoint( new joda.DateTime(468), 10.1 ),
    DataPoint( new joda.DateTime(469), 10.1 ),
    DataPoint( new joda.DateTime(470), 9.68 ),
    DataPoint( new joda.DateTime(471), 9.46 ),
    DataPoint( new joda.DateTime(472), 10.3 ),
    DataPoint( new joda.DateTime(473), 11.6 ),
    DataPoint( new joda.DateTime(474), 13.9 ),
    DataPoint( new joda.DateTime(475), 13.9 ),
    DataPoint( new joda.DateTime(476), 12.5 ),
    DataPoint( new joda.DateTime(477), 11.9 ),
    DataPoint( new joda.DateTime(478), 12.2 ),
    DataPoint( new joda.DateTime(479), 13 ),
    DataPoint( new joda.DateTime(480), 13.3 ),
    DataPoint( new joda.DateTime(481), 13 ),
    DataPoint( new joda.DateTime(482), 12.7 ),
    DataPoint( new joda.DateTime(483), 11.9 ),
    DataPoint( new joda.DateTime(484), 13.3 ),
    DataPoint( new joda.DateTime(485), 12.5 ),
    DataPoint( new joda.DateTime(486), 11.9 ),
    DataPoint( new joda.DateTime(487), 11.6 ),
    DataPoint( new joda.DateTime(488), 10.5 ),
    DataPoint( new joda.DateTime(489), 10.1 ),
    DataPoint( new joda.DateTime(490), 9.9 ),
    DataPoint( new joda.DateTime(491), 9.68 ),
    DataPoint( new joda.DateTime(492), 9.68 ),
    DataPoint( new joda.DateTime(493), 9.9 ),
    DataPoint( new joda.DateTime(494), 10.8 ),
    DataPoint( new joda.DateTime(495), 11 )
  )

}
