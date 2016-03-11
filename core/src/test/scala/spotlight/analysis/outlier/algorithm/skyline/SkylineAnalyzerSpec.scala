package spotlight.analysis.outlier.algorithm.skyline

import akka.testkit._
import com.github.nscala_time.time.JodaImplicits._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import spotlight.analysis.outlier.{DetectOutliersInSeries, DetectUsing, DetectionAlgorithmRouter, HistoricalStatistics}
import spotlight.model.outlier._
import spotlight.model.timeseries.{DataPoint, Row, TimeSeries, TimeSeriesBase}
import spotlight.testkit.ParallelAkkaSpec
import org.apache.commons.math3.random.RandomDataGenerator
import org.joda.{time => joda}
import org.mockito.Mockito._
import org.scalatest.Tag
import org.scalatest.mock.MockitoSugar

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by rolfsd on 2/15/16.
  */
class SkylineAnalyzerSpec extends ParallelAkkaSpec with MockitoSugar with LazyLogging {
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
    when( plan.name ).thenReturn( "mock-plan" )
    when( plan.appliesTo ).thenReturn( Fixture.appliesToAll )
    when( plan.algorithms ).thenReturn(
      Set(
        FirstHourAverageAnalyzer.Algorithm,
        MeanSubtractionCumulationAnalyzer.Algorithm,
        SimpleMovingAverageAnalyzer.Algorithm,
        LeastSquaresAnalyzer.Algorithm,
        GrubbsAnalyzer.Algorithm,
        HistogramBinsAnalyzer.Algorithm,
        MedianAbsoluteDeviationAnalyzer.Algorithm,
        KolmogorovSmirnovAnalyzer.Algorithm
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

    def spike( data: Row[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
      val (front, last) = data.sortBy{ _.timestamp.getMillis }.splitAt( position )
//      trace( s"""front[${front.size}] = [${front.mkString(",")}]""")
//      trace( s"""last[${last.size}] = [${last.mkString(",")}]""")
      val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
//      trace( s"""spiked = [${spiked.mkString(",")}]""")
      TimeSeries( "test-series", spiked )
    }

    def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = {
      prior map { h =>
        series.points.foldLeft( h ){ (history, dp) => history :+ dp.getPoint }
      } getOrElse {
        HistoricalStatistics.fromActivePoints( DataPoint.toDoublePoints(series.points).toArray, false )
      }
    }
  }

  override def makeAkkaFixture(): Fixture = new Fixture

  object DONE extends Tag( "done" )


  "SkylineAnalyzer" should {
    "find outliers deviating from first hour" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[FirstHourAverageAnalyzer]( FirstHourAverageAnalyzer.props(router.ref) )
      val firstHour = FirstHourAverageAnalyzer.Context.FirstHour
      trace( s"firstHour = $firstHour" )
      val full = makeDataPoints(
        values = points.map{ case DataPoint(_, v) => v },
        start = firstHour.start,
        period = 2.minutes,
        timeWiggle = (0.97, 1.03),
        valueWiggle = (0.99, 1.01)
      )

      val series = spike( full )()
      trace( s"test series = $series" )
      val algoS = FirstHourAverageAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 4""" )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "first hour" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Row( series.points.last )
        }
      }

      val start2 = series.points.last.timestamp + 1.minute
      val full2 = makeDataPoints(
        values = points.map{ case DataPoint(_, v) => v },
        start = start2,
        period = 2.minutes,
        timeWiggle = (0.97, 1.03),
        valueWiggle = (0.99, 1.01)
      )

      val series2 = spike( full2 )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "first hour again" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 3
          outliers mustBe series2.points.take(3)
        }
      }
    }

    "find outliers over 2 first hour messages" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[FirstHourAverageAnalyzer]( FirstHourAverageAnalyzer.props(router.ref) )
      val firstHour = FirstHourAverageAnalyzer.Context.FirstHour
      trace( s"firstHour = $firstHour" )
      val full = makeDataPoints(
        values = points.map{ case DataPoint(_, v) => v },
        start = firstHour.start,
        period = 1.minute,
        timeWiggle = (0.97, 1.03),
        valueWiggle = (0.99, 1.01)
      )

      val series = spike( full )()
      trace( s"test series = $series" )
      val algoS = FirstHourAverageAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 2""" )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered(algoS) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "first hour" ) {
        case m @ NoOutliers(alg, source, plan) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe false
        }
      }

      val start2 = series.points.last.timestamp + 1.minute
      val full2 = makeDataPoints(
        values = points.map{ case DataPoint(_, v) => v },
        start = start2,
        period = 1.minute,
        timeWiggle = (0.97, 1.03),
        valueWiggle = (0.99, 1.01)
      )

      val series2 = spike( full2 )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "first hour again" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 2
          outliers mustBe series2.points.take(2)
        }
      }
    }

    "find outliers deviating stddev from simple moving average" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SimpleMovingAverageAnalyzer]( SimpleMovingAverageAnalyzer.props( router.ref ) )
      val full = makeDataPoints(
        values = immutable.IndexedSeq.fill( 1000 )( 1.0 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (1.0, 1.0)
      )

      val series = spike( full )()
      trace( s"test series = $series" )
      val algoS = SimpleMovingAverageAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 3""" )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from average" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Row( series.points.last )
        }
      }

      val full2 = makeDataPoints(
        values = immutable.IndexedSeq.fill( 1000 )( 1.0 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (1.0, 1.0)
      )
      val series2 = spike( full2 )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from average again" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 3
          outliers mustBe series2.points.take(3)
        }
      }
    }

    "find outliers via Grubbs Test" in { f: Fixture =>
      import f._
      // helpful online grubbs calculator: http://graphpad.com/quickcalcs/Grubbs1.cfm

      val full: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 10 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series = spike( full )()

      val algoS = GrubbsAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 2""" )

      val analyzer = TestActorRef[GrubbsAnalyzer]( GrubbsAnalyzer.props(router.ref) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "grubbs" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Row( series.points.last )
        }
      }


      val full2: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 10 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series2 = spike( full )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing(algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "grubbs again" ) {
        case m @ NoOutliers(alg, source, plan) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe false
        }
      }
    }

    "find outliers based on absolute median deviation" in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[MedianAbsoluteDeviationAnalyzer]( MedianAbsoluteDeviationAnalyzer.props(router.ref) )
      val full = makeDataPoints(
        values = immutable.IndexedSeq.fill( 5 )( 1.0 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (1.0, 1.0)
      )

      val series = spike( full )()
      trace( s"test series = $series" )
      val algoS = MedianAbsoluteDeviationAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"${algoS.name}.tolerance: 3" )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "median absolute deviation" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Row( series.points.last )
        }
      }

      val full2 = makeDataPoints(
        values = immutable.IndexedSeq.fill( 5 )( 1.0 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (1.0, 1.0)
      )
      val series2 = spike( full2 )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "median absolute deviation again" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe series2.points.take(1)
        }
      }
    }

    "find outliers via cumulative mean subtraction Test" in { f: Fixture =>
      import f._
      val full: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 50 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series = spike( full )()

      val algoS = MeanSubtractionCumulationAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 3""" )

      val analyzer = TestActorRef[MeanSubtractionCumulationAnalyzer]( MeanSubtractionCumulationAnalyzer.props(router.ref) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "mean subtraction cumulation" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Row( series.points.last )
        }
      }


      val full2: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 50 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series2 = spike( full )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing(algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "mean subtraction cumulation again" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe series2.points.take(1)
        }
      }
    }

    "find outliers via least squares Test" in { f: Fixture =>
      import f._
      val full: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 10 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series = spike( full, 1000 )()

      val algoS = LeastSquaresAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 3""" )

      val analyzer = TestActorRef[LeastSquaresAnalyzer]( LeastSquaresAnalyzer.props(router.ref) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "least squares" ) {
        case m @ NoOutliers(alg, source, plan) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe false
        }
//        case m @ SeriesOutliers(alg, source, plan, outliers) => {
//          alg mustBe Set( algoS )
//          source mustBe series
//          m.hasAnomalies mustBe true
//          outliers.size mustBe 1
//          outliers mustBe Row( series.points.last )
//        }
      }


      val full2: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 10 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series2 = spike( full )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing(algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "least squares again" ) {
        case m @ NoOutliers(alg, source, plan) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe false
        }
//        case m @ SeriesOutliers(alg, source, plan, outliers) => {
//          alg mustBe Set( algoS )
//          source mustBe series2
//          m.hasAnomalies mustBe true
//          outliers.size mustBe 1
//          outliers mustBe series2.points.take(3).drop(2)
//        }
      }
      pending
    }

    "find outliers via histogram bins Test" in { f: Fixture =>
      import f._
      val full: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 20 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series = spike( full, 1000 )()

      val algoS = HistogramBinsAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.minimum-bin-size: 5""" )

      val analyzer = TestActorRef[HistogramBinsAnalyzer]( HistogramBinsAnalyzer.props(router.ref) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "histogram bins" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Row( series.points.last )
        }
      }


      val full2: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 20 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series2 = spike( full )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing(algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "histogram bins again" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 3
          outliers mustBe series2.points.take(3)
        }
      }
    }


    "find outliers via kolmogorov-smirnov (with augmented dickey fuller) Test" taggedAs (WIP) in { f: Fixture =>
      import f._
      val now = joda.DateTime.now

      val reference: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 20 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        start = now - 1.hour,
        period = 10.seconds,
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series = spike( reference, 1000 )()

      val algoS = KolmogorovSmirnovAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.reference-offset: 1 h""" )

      val analyzer = TestActorRef[KolmogorovSmirnovAnalyzer]( KolmogorovSmirnovAnalyzer.props(router.ref) )
      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "ks-test" ) {
        case m @ NoOutliers(alg, source, plan) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe false
        }
      }

      val next1: Row[DataPoint] = makeDataPoints(
        values = IndexedSeq.fill( 20 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
        start = now,
        period = 10.seconds,
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series2 = spike( next1, 1000 )()
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing(algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "ks-test II" ) {
        case m @ NoOutliers(alg, source, plan) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe false
        }
      }


      val next3: Row[DataPoint] = makeDataPoints(
        values = points.map{ _.value },
        start = now,
        period = 10.seconds,
        timeWiggle = (0.98, 1.02),
        valueWiggle = (0.98, 1.02)
      )

      val series3 = spike( next3 )( 0 )
      val history3 = historyWith( Option(history1.recordLastDataPoints(series.points)), series3 )

      analyzer.receive( DetectUsing(algoS, aggregator.ref, DetectOutliersInSeries(series3, plan), history3, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "ks-test III" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers) => {
          alg mustBe Set( algoS )
          source mustBe series3
          m.hasAnomalies mustBe true
          outliers.size mustBe series3.points.size
          outliers mustBe series3.points
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