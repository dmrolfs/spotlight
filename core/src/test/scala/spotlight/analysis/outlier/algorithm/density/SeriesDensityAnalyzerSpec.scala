package spotlight.analysis.outlier.algorithm.density

import java.util.concurrent.atomic.AtomicInteger

import akka.event.EventStream
import akka.testkit._
import com.github.nscala_time.time.OrderingImplicits._
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.joda.{time => joda}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import peds.commons.V
import shapeless._
import spotlight.analysis.outlier._
import spotlight.analysis.outlier.algorithm.{AlgorithmActor, CommonAnalyzer}
import spotlight.model.outlier._
import spotlight.model.timeseries._
import spotlight.testkit.ParallelAkkaSpec

import scala.annotation.tailrec
import scala.concurrent.duration._


/**
 * Created by damonrolfs on 9/18/14.
 */
class SeriesDensityAnalyzerSpec extends ParallelAkkaSpec with MockitoSugar {
  import SeriesDensityAnalyzerSpec._

  object Fixture {
    import scalaz._

    val appliesToAll: OutlierPlan.AppliesTo = {
      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification(0, 0)
      val reduce: ReduceOutliers = new ReduceOutliers {
        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: OutlierPlan
        ): V[Outliers] = Validation.failureNel[Throwable, Outliers]( new IllegalStateException("should not use" ) ).disjunction
      }

      val grouping: Option[OutlierPlan.Grouping] = {
        val window = None
        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
      }

      OutlierPlan.default( "", 1.second, isQuorun, reduce, Set.empty[Symbol], grouping ).appliesTo
    }
  }

  class Fixture extends AkkaFixture {
    val metric = Topic( "metric.a" )
    val algoS = SeriesDensityAnalyzer.Algorithm
    val algoC = CohortDensityAnalyzer.Algorithm
    val plan = mock[OutlierPlan]
    when( plan.appliesTo ).thenReturn( Fixture.appliesToAll )
    when( plan.algorithms ) thenReturn Set( algoS )

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
//      val initial = start getOrElse { Moment.withAlpha( "expected", 0.05 ).toOption.get }
      val initial = start getOrElse { new DescriptiveStatistics( CommonAnalyzer.ApproximateDayWindow ) }
//      val last = initialH.lastPoints.lastOption map { new DoublePoint( _ ) }
      val dps = DataPoint toDoublePoints points
      val basis = last map { l => dps.zip( DataPoint.toDoublePoint(l) +: dps ) } getOrElse { (dps drop 1).zip( dps ) }
      basis.foldLeft( initial ) { case (s, (c, p)) =>
        val ts = c.getPoint.head
        s.addValue( new EuclideanDistance().compute( p.getPoint, c.getPoint ) )
        s
//        s :+ new EuclideanDistance().compute( p.getPoint, c.getPoint )
      }
    }

    def makeDataPoints(
      values: Seq[Double],
      start: joda.DateTime = joda.DateTime.now,
      period: FiniteDuration = 1.second,
      timeWiggle: (Double, Double) = (1D, 1D),
      valueWiggle: (Double, Double) = (1D, 1D)
    ): Seq[DataPoint] = {
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

    def spike( data: Seq[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
      val (front, last) = data.sortBy{ _.timestamp.getMillis }.splitAt( position )
      val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
      TimeSeries( "test-series", spiked )
    }

    def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = {
      prior map { h =>
        series.points.foldLeft( h ){ (history, dp) => history :+ dp.getPoint }
      } getOrElse {
        HistoricalStatistics.fromActivePoints( DataPoint.toDoublePoints(series.points), false )
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
      val expectedValues = Seq( 18.8, 25.2, 31.5, 22.0, 24.1, 39.2 )
      val expected = points filter { expectedValues contains _.value } sortBy { _.timestamp }
      val algProps = ConfigFactory.parseString(
        s"""
           |${algoS.name}.tolerance: 0.756574715  // to bring effective eps to 5.0
           |${algoS.name}.seedEps: 1.0
           |${algoS.name}.minDensityConnectedPoints: 3
           |${algoS.name}.distance: Euclidean
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries( series, plan ), HistoricalStatistics(2, false), algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers, control) => {
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

      val myPoints = Seq(
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
           |${algoS.name} {
           |  tolerance: 3
           |  seedEps: 5.0
           |  minDensityConnectedPoints: 3
           |}
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries( series, plan ), HistoricalStatistics(2, false), algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        case m @ NoOutliers(alg, source, plan, control) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe false
        }
      }
    }

    "detect outlier through series of micro batches" taggedAs (WIP) in { f: Fixture =>
      import f._
      import spotlight.model.timeseries.DataPoint._

      def detectUsing( series: TimeSeries, history: HistoricalStatistics ): DetectUsing = {
        DetectUsing(
          algorithm = algoS,
          aggregator = aggregator.ref,
          payload = OutlierDetectionMessage( series, plan ).toOption.get,
          history = history,
          properties = ConfigFactory.parseString(
            s"""
               |${algoS.name}.seedEps: 5.0
               |${algoS.name}.tolerance: 2.0
               |${algoS.name}.minDensityConnectedPoints: 3
            """.stripMargin
          )
        )
      }

      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props( router.ref ) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )

      val topic = "test.series"
      val start = joda.DateTime.now
      val rnd = new RandomDataGenerator()

      @tailrec def loop( i: Int, left: Int, previous: Option[(TimeSeries, HistoricalStatistics)] = None ): Unit = {
        log.info( ">>>>>>>>>  TEST-LOOP( i:[{}] left:[{}]", i, left )
        val dt = start plusSeconds (10 * i)
        val v = if ( left == 0 ) 1000.0 else rnd.nextUniform( 0.99, 1.01, true )
        val s = TimeSeries( topic, Seq( DataPoint(dt, v) ) )
        val h = {
          previous
          .map { case (ps, ph) => s.points.foldLeft( ph recordLastPoints ps.points ) { (acc, p) => acc :+ p } }
          .getOrElse { HistoricalStatistics.fromActivePoints( DataPoint.toDoublePoints(s.points), false ) }
        }
        analyzer receive detectUsing( s, h )

        val expected: PartialFunction[Any, Unit] = {
          if ( left == 0 ) {
            case m: SeriesOutliers => {
              m.algorithms mustBe Set( algoS )
              m.source mustBe s
              m.hasAnomalies mustBe true
              m.outliers mustBe s.points
            }
          } else {
            case m: NoOutliers => {
              m.algorithms mustBe Set( algoS )
              m.source mustBe s
              m.hasAnomalies mustBe false
            }
          }
        }

        aggregator.expectMsgPF( 2.seconds.dilated, s"point-$i" )( expected )

        if ( left == 0 ) () else loop( i + 1, left - 1, Some( (s, h) ) )
      }

      loop( 0, 20 )
//      val s1 = TimeSeries( topic, Seq( DataPoint(start, 1.0) ) )
//      val h1 = HistoricalStatistics.fromActivePoints( DataPoint.toDoublePoints(s1.points).toArray, false )
//      analyzer receive detectUsing( s1, h1 )
//      aggregator.expectMsgPF( 2.seconds.dilated, "point 1" ) {
//        case m: NoOutliers => {
//          m.algorithms mustBe Set( algoS )
//          m.source mustBe s1
//          m.hasAnomalies mustBe false
//        }
//      }
//
//      val s2 = TimeSeries( topic, Seq( DataPoint(start plusSeconds 10, 1.0) ) )
//      val h2 = h1.recordLastDataPoints(s1.points) :+ s2.points.head
//      analyzer receive detectUsing( s2, h2 )
//      aggregator.expectMsgPF( 2.seconds.dilated, "point 2" ) {
//        case m: NoOutliers => {
//          m.algorithms mustBe Set( algoS )
//          m.source mustBe s2
//          m.hasAnomalies mustBe false
//        }
//      }
//
//      val s3 = TimeSeries( topic, Seq( DataPoint(start plusSeconds 20, 1000.0) ) )
//      val h3 = h2.recordLastDataPoints(s2.points) :+ s3.points.head
//      analyzer receive detectUsing( s3, h3 )
//      aggregator.expectMsgPF( 2.seconds.dilated, "point 3" ) {
//        case m: SeriesOutliers => {
//          m.algorithms mustBe Set( algoS )
//          m.source mustBe s3
//          m.hasAnomalies mustBe true
//          m.outliers mustBe s3.points
//        }
//      }
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
           |${algoS.name}.tolerance: 3.0
           |${algoC.name}.seedEps: 5.0
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


    "history is updated with each detect request" in { f: Fixture =>
      import f._

      def detectUsing( message: OutlierDetectionMessage, history: HistoricalStatistics ): DetectUsing = {
        val config = ConfigFactory.parseString(
          s"""
             |${algoS.name}.tolerance: 1.785898778
             |${algoS.name}.seedEps: 1.0
             |${algoS.name}.minDensityConnectedPoints: 3
             |${algoS.name}.distance: Euclidean
           """.stripMargin
        )

        DetectUsing( algorithm = algoS, aggregator.ref, payload = message, history, properties = config )
      }

      val PointsForLast = HistoricalStatistics.LastN

      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props( router.ref ) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )

      val detectHistoryA = HistoricalStatistics.fromActivePoints( DataPoint.toDoublePoints(pointsA), false )
      val msgA = detectUsing(
        message = OutlierDetectionMessage( TimeSeries(topic = metric, points = pointsA), plan ).toOption.get,
        history = detectHistoryA
      )

      val densityExpectedA = makeDensityExpectedHistory( pointsA, None, None )
//      densityExpectedA.getN mustBe ( pointsA.size - 1)

      val operationToTest = analyzer.underlyingActor.algorithmContext >=> analyzer.underlyingActor.updateDistanceMoment

      assertDescriptiveStats(
        operationToTest.run(msgA).toOption.get.asInstanceOf[SeriesDensityAnalyzer.Context].distanceStatistics,
        densityExpectedA
      )

      detectHistoryA.N mustBe (pointsA.size)
      val detectHistoryARecorded = detectHistoryA recordLastPoints pointsA
      detectHistoryARecorded.N mustBe (pointsA.size)
      val detectHistoryAB = DataPoint.toDoublePoints( pointsB ).foldLeft( detectHistoryARecorded ){ (h, dp) => h :+ dp.getPoint }
      detectHistoryAB.N mustBe (pointsA.size + pointsB.size )
      val detectHistoryABLast: Seq[PointA] = detectHistoryAB.lastPoints
      val pointsALast: Seq[PointA] = {
        pointsA.drop( pointsA.size - PointsForLast ).map{ dp => Array(dp.timestamp.getMillis.toDouble, dp.value) }.toList
      }
      detectHistoryABLast.size mustBe pointsALast.size
      detectHistoryABLast.zip(pointsALast).foreach{ case (f, b) => f mustBe b }

      val msgAB = detectUsing(
        message = OutlierDetectionMessage( TimeSeries( topic = metric, points = pointsB ), plan ).toOption.get,
        history = detectHistoryAB
      )
      val densityExpectedAB = makeDensityExpectedHistory( pointsB, Some(densityExpectedA), pointsA.lastOption )

      trace( s"pointsA.size = ${pointsA.size}" )
      trace( s"pointsB.size = ${pointsB.size}" )
//      trace( s"expectedA.n = ${densityExpectedA.getN}" )
//      trace( s"expectedAB.N = ${densityExpectedAB.getN}" )
//      densityExpectedAB.getN mustBe ( pointsA.size - 1 + pointsB.size )
      trace( s"historyAB.n = ${detectHistoryAB.N}" )
      trace( s"expectedAB = $densityExpectedAB" )
      trace( s"historyAB = ${detectHistoryAB}" )
      assertDescriptiveStats(
        operationToTest.run(msgAB).toOption.get.asInstanceOf[SeriesDensityAnalyzer.Context].distanceStatistics,
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
            analyzerB.underlyingActor._scopedContexts( HistoryKey(plan, metric) ).asInstanceOf[SeriesDensityAnalyzer.Context].distanceStatistics,
            densityExpectedB_A
          )
          m.topic mustBe metric
          m.algorithms mustBe Set(algoS)
          m.anomalySize mustBe 9
          m.outliers mustBe pointsA.take(8) :+ pointsA.last
        }
      }

      val densityExpectedB_AB = makeDensityExpectedHistory( pointsB, Option(densityExpectedB_A), pointsA.lastOption )
      analyzerB receive msgAB
      aggregator.expectMsgPF( 2.seconds.dilated, "default-foo-AB" ){
        case m: NoOutliers => {
          assertDescriptiveStats(
            analyzerB.underlyingActor._scopedContexts( HistoryKey(plan, metric) ).asInstanceOf[SeriesDensityAnalyzer.Context].distanceStatistics,
            densityExpectedB_AB
          )
          m.topic mustBe metric
          m.algorithms mustBe Set(algoS)
          m.anomalySize mustBe 0
        }
//        case m: SeriesOutliers => {
//          assertDescriptiveStats(
//            analyzerB.underlyingActor._scopedContexts( HistoryKey(plan, metric) ).distanceHistory,
//            densityExpectedB_AB
//          )
//          m.topic mustBe metric
//          m.algorithms mustBe Set(algoS)
//          m.anomalySize mustBe 6
//          m.outliers mustBe pointsB.take(6)
//        }
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

    "create controls during detect analysis" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SeriesDensityAnalyzer]( SeriesDensityAnalyzer.props(router.ref) )
      val series = TimeSeries( "series", points )
      val expectedValues = Seq( 18.8, 25.2, 31.5, 22.0, 24.1, 39.2 )
      val expected = points filter { expectedValues contains _.value } sortBy { _.timestamp }
      val algProps = ConfigFactory.parseString(
        s"""
           |${algoS.name}.tolerance: 0.756574715  // to bring effective eps to 5.0
           |${algoS.name}.seedEps: 1.0
           |${algoS.name}.minDensityConnectedPoints: 3
           |${algoS.name}.distance: Euclidean
           |${algoS.name}.publish-controls: yes
        """.stripMargin
      )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries( series, plan ), HistoricalStatistics(2, false), algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "detect" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers, actual) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 6
          outliers mustBe expected

          actual( algoS ).size mustBe series.points.size - 1
          actual mustBe expectedControls
        }
      }
    }


    "handle no clusters found" in { f: Fixture => pending }
  }
}

object SeriesDensityAnalyzerSpec {
  val sysId = new AtomicInteger()

  val points = Seq(
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


  val pointsA = Seq(
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

  val pointsB = Seq(
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

  val expectedControls = {
    Map(
      'dbscanSeries ->  Seq(
        ControlBoundary( new joda.DateTime(441), Some(4.561020454283958), Some(6.901121606999746), Some(9.241222759715534) ),
        ControlBoundary( new joda.DateTime(442), Some(5.001020446680885), Some(7.341121607251624), Some(9.681222767822364) ),
        ControlBoundary( new joda.DateTime(443), Some(6.701020416176415), Some(9.041121606980441), Some(11.381222797784467) ),
        ControlBoundary( new joda.DateTime(444), Some(9.601020363752442), Some(11.94112160559595), Some(14.281222847439457) ),
        ControlBoundary( new joda.DateTime(445), Some(12.401020316178887), Some(14.741121604078307), Some(17.081222891977728) ),
        ControlBoundary( new joda.DateTime(446), Some(14.301020286379911), Some(16.641121602934252), Some(18.981222919488594) ),
        ControlBoundary( new joda.DateTime(447), Some(13.50102029866888), Some(15.841121604108707), Some(18.181222909548534) ),
        ControlBoundary( new joda.DateTime(448), Some(9.601020363752443), Some(11.941121605092246), Some(14.281222846432048) ),
        ControlBoundary( new joda.DateTime(449), Some(7.301020405229818), Some(9.641121605794186), Some(11.981222806358556) ),
        ControlBoundary( new joda.DateTime(450), Some(5.901020430684881), Some(8.241121606738828), Some(10.581222782792775) ),
        ControlBoundary( new joda.DateTime(451), Some(3.6810204688308397), Some(6.0211216072992375), Some(8.361222745767636) ),
        ControlBoundary( new joda.DateTime(452), Some(3.4610204722912554), Some(5.80112160745571), Some(8.141222742620165) ),
        ControlBoundary( new joda.DateTime(453),Some(3.681020468830839), Some(6.021121607384459), Some(8.361222745938079) ),
        ControlBoundary( new joda.DateTime(454),Some(2.6010204849255207), Some(4.941121607780923), Some(7.281222730636326) ),
        ControlBoundary( new joda.DateTime(455), Some(2.201020490220107), Some(4.54112160800231), Some(6.8812227257845135) ),
        ControlBoundary( new joda.DateTime(456), Some(2.401020487624715), Some(4.7411216079626115), Some(7.081222728300508) ),
        ControlBoundary( new joda.DateTime(457),Some(2.811020481986834), Some(5.151121607792507), Some(7.49122273359818) ),
        ControlBoundary( new joda.DateTime(458), Some(3.2410204756664327), Some(5.581121607519147), Some(7.921222739371862) ),
        ControlBoundary( new joda.DateTime(459), Some(3.2410204756664327), Some(5.581121607574689), Some(7.921222739482944) ),
        ControlBoundary( new joda.DateTime(460), Some(2.201020490220107), Some(4.541121608043156), Some(6.881222725866205) ),
        ControlBoundary( new joda.DateTime(461), Some(2.6010204849255207), Some(4.941121607780923), Some(7.281222730636326) ),
        ControlBoundary( new joda.DateTime(462), Some(2.201020490220107), Some(4.541121607972484), Some(6.881222725724862) ),
        ControlBoundary( new joda.DateTime(463), Some(2.201020490220107), Some(4.54112160800231), Some(6.8812227257845135) ),
        ControlBoundary( new joda.DateTime(464), Some(2.401020487624715), Some(4.7411216079626115), Some(7.081222728300508) ),
        ControlBoundary( new joda.DateTime(465), Some(2.8110204819868336), Some(5.151121607991389), Some(7.491222733995944) ),
        ControlBoundary( new joda.DateTime(466), Some(3.901020465292792), Some(6.241121607519583), Some(8.581222749746374) ),
        ControlBoundary( new joda.DateTime(467), Some(5.001020446680886), Some(7.341121608310925), Some(9.681222769940966) ),
        ControlBoundary( new joda.DateTime(468), Some(9.301020369072278), Some(11.64112160653513), Some(13.981222843997982) ),
        ControlBoundary( new joda.DateTime(469), Some(13.901020292477257), Some(16.241121605250854), Some(18.58122291802445) ),
        ControlBoundary( new joda.DateTime(470), Some(20.301020514045767), Some(22.641121602796463), Some(24.98122269154716) ),
        ControlBoundary( new joda.DateTime(471), Some(26.601020514045256), Some(28.941121601667728), Some(31.2812226892902) ),
        ControlBoundary( new joda.DateTime(472), Some(17.101020514046052), Some(19.44112160252131), Some(21.781222690996568) ),
        ControlBoundary( new joda.DateTime(473), Some(19.201020514045865), Some(21.541121606784742), Some(23.88122269952362) )
      )
    )
  }
}
