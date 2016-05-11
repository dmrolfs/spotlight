package spotlight.analysis.outlier.algorithm.skyline

import scala.collection.immutable
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.mockito.Mockito._
import org.joda.{ time => joda }
import spotlight.analysis.outlier.{DetectOutliersInSeries, DetectUsing, DetectionAlgorithmRouter}
import spotlight.model.outlier.{OutlierPlan, SeriesOutliers}
import spotlight.model.timeseries.{ControlBoundary, DataPoint}

import scala.annotation.tailrec



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

    def tailAverageData( data: Seq[DataPoint], last: Seq[DataPoint] = Seq.empty[DataPoint] ): Seq[DataPoint] = {
      val TailLength = 3
      val lastPoints = last.drop( last.size - TailLength + 1 ) map { _.value }
      data.map { _.timestamp }
      .zipWithIndex
      .map { case (ts, i ) =>
        val pointsToAverage = {
          if ( i < TailLength ) {
            val all = lastPoints ++ data.take( i + 1 ).map{ _.value }
            all.drop( all.size - TailLength )
          } else {
            data.drop( i - TailLength + 1 ).take( TailLength ).map{ _.value }
          }
        }

        ( ts, pointsToAverage )
      }
      .map { case (ts, pts) => DataPoint( timestamp = ts, value = pts.sum / pts.size ) }
    }

    def calculateControlBoundaries(
      points: Seq[DataPoint],
      factor: Double,
      lastPoints: Seq[DataPoint] = Seq.empty[DataPoint]
    ): Seq[ControlBoundary] = {
      @tailrec def loop( pts: List[DataPoint], history: Array[Double], acc: Seq[ControlBoundary] ): Seq[ControlBoundary] = {
        pts match {
          case Nil => acc

          case p :: tail => {
            val stats = new DescriptiveStatistics( history )
            val mean = stats.getMean
            val stddev = stats.getStandardDeviation
            val control = ControlBoundary.fromExpectedAndDistance( p.timestamp, expected = mean, distance = math.abs(factor * stddev) )
            logger.debug( "EXPECTED for point:[{}] Control [{}] = [{}]", (p.timestamp.getMillis, p.value), acc.size.toString, control )
            loop( tail, history :+ p.value, acc :+ control )
          }
        }
      }

      loop( points.toList, lastPoints.map{ _.value }.toArray, Seq.empty[ControlBoundary] )
    }
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
      val now = joda.DateTime.now
      val full = makeDataPoints(
        values = immutable.IndexedSeq.fill( 3 )( 1.0 ),
        start = now,
        timeWiggle = (0.97, 1.03),
        valueWiggle = (0.75, 1.25)
      )

      val series = spike( full, 10D )()
      trace( s"test series = $series" )
      val algoS = SimpleMovingAverageAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS.name}.tolerance: 3""" )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      val lastPoints1 = history1.lastPoints.map{ p => DataPoint( new joda.DateTime(p(0).toLong), p(1) ) }
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "sma control" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers, actual) => {
          actual.keySet mustBe Set( algoS )
          val expected = calculateControlBoundaries(
            points = tailAverage( data = series.points, lastPoints = lastPoints1 ),
            factor = 3.0
          )
          actual( algoS ).zip( expected ).zipWithIndex foreach  { case ((a, e), i) => (i, a) mustBe (i, e) }
        }
      }

      val full2 = makeDataPoints(
        values = immutable.IndexedSeq.fill( 3 )( 1.0 ),
        start = now.plus( 3000 ),
        timeWiggle = (0.97, 1.03),
        valueWiggle = (0.0, 10)
      )
      val series2 = spike( full2, 100 )( 0 )
      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )
      val lastPoints2 = history2.lastPoints.map{ p => DataPoint( new joda.DateTime(p(0).toLong), p(1) ) }
      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
      aggregator.expectMsgPF( 2.seconds.dilated, "sma control again" ) {
        case m @ SeriesOutliers(alg, source, plan, outliers, actual) => {
          actual.keySet mustBe Set( algoS )

          val expected = calculateControlBoundaries(
             points = tailAverage( data = series2.points, lastPoints = lastPoints2 ),
             factor = 3,
             lastPoints = tailAverage( data = series.points, lastPoints = lastPoints1 )
           )
          actual( algoS ).zip( expected ).zipWithIndex foreach { case ((a, e), i) =>
            (i, a) mustBe (i,e)
          }

//          control( algoS ).zip( expectedControls ) foreach { case (a, e) =>
//            a.timestamp mustBe e.timestamp
//            a.floor.get mustBe (e.floor.get +- 0.005)
//            a.expected.get mustBe (e.expected.get +- 0.005)
//            a.ceiling.get mustBe (e.ceiling.get +- 0.005)
//          }
//
        }
      }
    }
  }
}
