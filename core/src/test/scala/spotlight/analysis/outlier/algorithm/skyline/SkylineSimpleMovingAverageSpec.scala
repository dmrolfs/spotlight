package spotlight.analysis.outlier.algorithm.skyline

import scala.collection.immutable
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import spotlight.analysis.outlier.{DetectOutliersInSeries, DetectUsing, DetectionAlgorithmRouter}
import spotlight.model.outlier.{OutlierPlan, SeriesOutliers}
import spotlight.model.timeseries.{ControlBoundary, DataPoint}



/**
  * Created by rolfsd on 3/22/16.
  */
class SkylineSimpleMovingAverageSpec extends SkylineBaseSpec {
  class Fixture extends SkylineFixture {
    val algoS = SimpleMovingAverageAnalyzer.Algorithm
    val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 3""" )

    val plan = mock[OutlierPlan]
    when( plan.name ).thenReturn( "mock-plan" )
    when( plan.appliesTo ).thenReturn( SkylineFixture.appliesToAll )
    when( plan.algorithms ).thenReturn( Set(algoS) )
  }

  override def makeAkkaFixture(): Fixture = new Fixture


  "SimpleMovingAverageAnalyzer" should {
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
        case m @ SeriesOutliers(alg, source, plan, outliers, control) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Seq( series.points.last )
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
        case m @ SeriesOutliers(alg, source, plan, outliers, control) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 3
          outliers mustBe series2.points.take(3)
        }
      }
    }

    "provide full control boundaries" taggedAs (WIP) in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[SimpleMovingAverageAnalyzer]( SimpleMovingAverageAnalyzer.props( router.ref ) )
      val full = makeDataPoints(
        values = immutable.IndexedSeq.fill( 10 )( 1.0 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (1.0, 1.0)
      )

      val series = spike( full, 10D )()
      trace( s"test series = $series" )
      val algoS = SimpleMovingAverageAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 3""" )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "sma control" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers, control) => {
          control.keySet mustBe Set( algoS )

          val expectedControls = full collect {
            case DataPoint( ts, _ ) if ts == full.head.timestamp => ControlBoundary( ts, None, None, None )
            case DataPoint( ts, _ ) => ControlBoundary( ts, Some(1D), Some(1D), Some(1D) )
          }

          control( algoS ) mustBe expectedControls
        }
      }

      val full2 = makeDataPoints(
        values = immutable.IndexedSeq.fill( 3 )( 1.0 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (1.0, 1.0)
      )
      val series2 = spike( full2, 10 )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )

      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "sma control again" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers, control) => {
          control.keySet mustBe Set( algoS )

          val expectedControls = Seq(
            ControlBoundary( full2(0).timestamp, Some(-1.5460498941515415), Some(1.3), Some(4.146049894151542) ),
            ControlBoundary( full2(1).timestamp, Some(-4.001846297963951), Some(1.8181818181818181), Some(7.6382099343275875) ),
            ControlBoundary( full2(2).timestamp, Some(-4.886653149888831), Some(2.25), Some(9.386653149888831) )
          )

            full collect {
            case DataPoint( ts, _ ) if ts == full2(0).timestamp => ControlBoundary( ts, None, None, None )
            case DataPoint( ts, _ ) => ControlBoundary( ts, Some(1D), Some(1D), Some(1D) )
          }

          control( algoS ).zip( expectedControls ) foreach { case (a, e) =>
            a.timestamp mustBe e.timestamp
            a.floor.get mustBe (e.floor.get +- 0.005)
            a.expected.get mustBe (e.expected.get +- 0.005)
            a.ceiling.get mustBe (e.ceiling.get +- 0.005)
          }

          control( algoS ) mustBe expectedControls
        }
      }
    }
  }
}
