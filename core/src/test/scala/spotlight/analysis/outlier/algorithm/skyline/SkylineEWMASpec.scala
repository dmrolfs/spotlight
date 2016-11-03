package spotlight.analysis.outlier.algorithm.skyline

import akka.actor.ActorSystem

import scala.collection.immutable
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito._
import spotlight.analysis.outlier.{DetectOutliersInSeries, DetectUsing, DetectionAlgorithmRouter}
import spotlight.model.outlier._


/**
  * Created by rolfsd on 2/15/16.
  */
class SkylineEWMASpec extends SkylineBaseSpec {

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends SkylineFixture( _config, _system, _slug ) {
    val algoS = ExponentialMovingAverageAnalyzer.Algorithm
    val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 3""" )

    val plan = mock[OutlierPlan]
    when( plan.name ).thenReturn( "mock-plan" )
    when( plan.appliesTo ).thenReturn( SkylineFixture.appliesToAll )
    when( plan.algorithms ).thenReturn( Set(algoS) )
  }


  "ExponentialMovingAverageAnalyzer" should {
    "find outliers deviating stddev from exponential moving average" taggedAs (WIP) in { f: Fixture =>
      import f._
      val analyzer = TestActorRef[ExponentialMovingAverageAnalyzer]( ExponentialMovingAverageAnalyzer.props(router.ref) )
      val full = makeDataPoints(
        values = immutable.IndexedSeq.fill( 5 )( 1.0 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (1.0, 1.0)
      )

      val series = spike( full )()
      trace( s"test series = $series" )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      implicit val sender = aggregator.ref
      analyzer ! DetectUsing(
        algoS,
        DetectOutliersInSeries(series, plan, subscriber.ref, Set()),
        history1,
        algProps
      )

      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from moving average" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers, control) => {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Seq( series.points.last )
        }
      }

      val full2 = makeDataPoints(
        values = immutable.IndexedSeq.fill( 5 )( 1.0 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (1.0, 1.0)
      )

      val series2 = spike( full2 )( 0 )
      val history2 = historyWith( Option(history1 recordLastPoints series.points), series2 )

      analyzer ! DetectUsing(
        algoS,
        DetectOutliersInSeries(series2, plan, subscriber.ref, Set()),
        history2,
        algProps
      )

      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from moving average again" ) {
        case m @ NoOutliers(alg, source, plan, control) => {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe false
        }
      }
    }

//    "publish outliers result to" in { f: Fixture =>
//      import f._
//      val analyzer = TestActorRef[ExponentialMovingAverageAnalyzer]( ExponentialMovingAverageAnalyzer.elasticProps(router.ref) )
//      val full = makeDataPoints(
//                                 values = immutable.IndexedSeq.fill( 5 )( 1.0 ),
//                                 timeWiggle = (0.97, 1.03),
//                                 valueWiggle = (1.0, 1.0)
//                               )
//
//      val series = spike( full )()
//      trace( s"test series = $series" )
//
//      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
//      val history1 = historyWith( None, series )
//      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
//      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from moving average" ) {
//        case m @ SeriesOutliers(alg, source, plan, outliers) => {
//          alg mustBe Set( algoS )
//          source mustBe series
//          m.hasAnomalies mustBe true
//          outliers.size mustBe 1
//          outliers mustBe Row( series.points.last )
//        }
//      }
//
//      val full2 = makeDataPoints(
//                                  values = immutable.IndexedSeq.fill( 5 )( 1.0 ),
//                                  timeWiggle = (0.97, 1.03),
//                                  valueWiggle = (1.0, 1.0)
//                                )
//
//      val series2 = spike( full2 )( 0 )
//      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )
//
//      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
//      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from moving average again" ) {
//        case m @ NoOutliers(alg, source, plan) => {
//          alg mustBe Set( algoS )
//          source mustBe series2
//          m.hasAnomalies mustBe false
//        }
//      }
//    }
  }
}
