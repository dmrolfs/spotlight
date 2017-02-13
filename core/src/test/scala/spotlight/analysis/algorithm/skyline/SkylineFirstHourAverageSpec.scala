package spotlight.analysis.algorithm.skyline

import akka.actor.ActorSystem

import scala.collection.immutable
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.mockito.Mockito._
import org.joda.{ time ⇒ joda }
import com.github.nscala_time.time.JodaImplicits._
import peds.akka.envelope.WorkId
import peds.commons.identifier.{ ShortUUID, TaggedID }
import spotlight.analysis.algorithm.CommonAnalyzer
import spotlight.analysis.{ DetectOutliersInSeries, DetectUsing, DetectionAlgorithmRouter }
import spotlight.model.outlier.{ AnalysisPlan, SeriesOutliers }
import spotlight.model.timeseries.{ DataPoint, ThresholdBoundary }
import spotlight.testkit.TestCorrelatedSeries

/** Created by rolfsd on 3/22/16.
  */
class SkylineFirstHourAverageSpec extends SkylineBaseSpec {
  import SkylineFirstHourAverageSpec.points

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends SkylineFixture( _config, _system, _slug ) {
    val algoS = FirstHourAverageAnalyzer.Algorithm
    val algProps = ConfigFactory.parseString( s"""${algoS}.tolerance: 3""" )

    val plan = mock[AnalysisPlan]
    when( plan.id ).thenReturn( TaggedID( 'plan, ShortUUID() ) )
    when( plan.name ).thenReturn( "mock-plan" )
    when( plan.appliesTo ).thenReturn( SkylineFixture.appliesToAll )
    when( plan.algorithms ).thenReturn( Set( algoS ) )

    def tailAverageData( data: Seq[DataPoint], last: Seq[DataPoint] = Seq.empty[DataPoint] ): Seq[DataPoint] = {
      val TailLength = CommonAnalyzer.DefaultTailAverageLength
      val lastPoints = last.drop( last.size - TailLength + 1 ) map { _.value }
      data.map { _.timestamp }
        .zipWithIndex
        .map {
          case ( ts, i ) ⇒
            val pointsToAverage = {
              if ( i < TailLength ) {
                val all = lastPoints ++ data.take( i + 1 ).map { _.value }
                all.drop( all.size - TailLength )
              } else {
                data.drop( i - TailLength + 1 ).take( TailLength ).map { _.value }
              }
            }

            ( ts, pointsToAverage )
        }
        .map { case ( ts, pts ) ⇒ DataPoint( timestamp = ts, value = pts.sum / pts.size ) }
    }

    def calculateControlBoundaries(
      points: Seq[DataPoint],
      tolerance: Double,
      lastPoints: Seq[DataPoint] = Seq.empty[DataPoint]
    ): Seq[ThresholdBoundary] = {
      val effective = ( lastPoints ++ points ).filter { p ⇒ FirstHourAverageAnalyzer.Context.FirstHour.contains( p.timestamp ) }
      val stats = new DescriptiveStatistics( effective.map { _.value }.toArray )
      logger.debug( "expected threshold statistics=[{}]", stats )
      points.map { p ⇒
        ThresholdBoundary.fromExpectedAndDistance(
          timestamp = p.timestamp,
          expected = stats.getMean,
          distance = tolerance * stats.getStandardDeviation
        )
      }
    }

    def compareControls( index: Int, actual: ThresholdBoundary, expected: ThresholdBoundary ): Unit = {
      ( index, actual.timestamp ) mustBe ( index, expected.timestamp )
      ( index, actual.floor.isDefined ) mustBe ( index, expected.floor.isDefined )
      ( index, actual.expected.isDefined ) mustBe ( index, expected.expected.isDefined )
      ( index, actual.ceiling.isDefined ) mustBe ( index, expected.ceiling.isDefined )

      logger.debug( "first-hour comparing index:[{}]", index.toString )

      for {
        a ← actual.floor
        e ← expected.floor
      } { a mustBe ( e +- 0.00001 ) }

      for {
        a ← actual.expected
        e ← expected.expected
      } { a mustBe ( e +- 0.00001 ) }

      for {
        a ← actual.ceiling
        e ← expected.ceiling
      } { a mustBe ( e +- 0.00001 ) }
    }
  }

  "FirstHourAverageAnalyzer" should {
    "find outliers deviating from first hour" in { f: Fixture ⇒
      import f._
      val analyzer = TestActorRef[FirstHourAverageAnalyzer]( FirstHourAverageAnalyzer.props( router.ref ) )
      val firstHour = FirstHourAverageAnalyzer.Context.FirstHour
      trace( s"firstHour = $firstHour" )
      val full = makeDataPoints(
        values = points.map { case DataPoint( _, v ) ⇒ v },
        start = firstHour.start,
        period = 2.minutes,
        timeWiggle = ( 1.0, 1.0 ),
        valueWiggle = ( 1.0, 1.0 )
      )

      val series = spike( full )()
      trace( s"test series = $series" )
      val algoS = FirstHourAverageAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS}.tolerance: 4""" )

      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
      val history1 = historyWith( None, series )
      implicit val sender = aggregator.ref
      analyzer ! DetectUsing(
        plan.id,
        algoS,
        DetectOutliersInSeries( series, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
        history1,
        algProps
      )

      aggregator.expectMsgPF( 2.seconds.dilated, "first hour" ) {
        case m @ SeriesOutliers( alg, source, plan, outliers, control ) ⇒ {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers mustBe Seq( series.points.last )
          outliers.size mustBe 1
        }
      }

      val start2 = series.points.last.timestamp + 1.minute
      val full2 = makeDataPoints(
        values = points.map { case DataPoint( _, v ) ⇒ v },
        start = start2,
        period = 2.minutes,
        timeWiggle = ( 1.0, 1.0 ),
        valueWiggle = ( 1.0, 1.0 )
      )

      val series2 = spike( full2 )( 0 )
      val history2 = historyWith( Option( history1 recordLastPoints series.points ), series2 )

      analyzer ! DetectUsing(
        plan.id,
        algoS,
        DetectOutliersInSeries( series2, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
        history2,
        algProps
      )

      aggregator.expectMsgPF( 2.seconds.dilated, "first hour again" ) {
        case m @ SeriesOutliers( alg, source, plan, outliers, control ) ⇒ {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 2 //3
          outliers mustBe Seq( series2.points.head, series2.points.last )
        }
      }
    }

    "find outliers over 2 first hour messages" taggedAs ( WIP ) in { f: Fixture ⇒
      import f._
      val analyzer = TestActorRef[FirstHourAverageAnalyzer]( FirstHourAverageAnalyzer.props( router.ref ) )
      val firstHour = FirstHourAverageAnalyzer.Context.FirstHour
      trace( s"firstHour = $firstHour" )
      val full = makeDataPoints(
        values = points.map { case DataPoint( _, v ) ⇒ v },
        start = firstHour.start,
        period = 1.minute,
        timeWiggle = ( 0.97, 1.03 ),
        valueWiggle = ( 0.99, 1.01 )
      )

      val series = spike( full )()
      trace( s"test series = $series" )
      val algoS = FirstHourAverageAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS}.tolerance: 2""" )

      analyzer ! DetectionAlgorithmRouter.AlgorithmRegistered( algoS )
      val history1 = historyWith( None, series )
      implicit val sender = aggregator.ref
      analyzer ! DetectUsing(
        plan.id,
        algoS,
        DetectOutliersInSeries( series, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
        history1,
        algProps
      )

      aggregator.expectMsgPF( 2.seconds.dilated, "first hour" ) {
        case m @ SeriesOutliers( alg, source, plan, outliers, control ) ⇒ {
          alg mustBe Set( algoS )
          source mustBe series
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe Seq( series.points.last )
        }
      }

      val start2 = series.points.last.timestamp + 1.minute
      val full2 = makeDataPoints(
        values = points.map { case DataPoint( _, v ) ⇒ v },
        start = start2,
        period = 1.minute,
        timeWiggle = ( 0.97, 1.03 ),
        valueWiggle = ( 0.99, 1.01 )
      )

      val series2 = spike( full2 )( 0 )
      val history2 = historyWith( Option( history1 recordLastPoints series.points ), series2 )

      analyzer ! DetectUsing(
        plan.id,
        algoS,
        DetectOutliersInSeries( series2, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
        history2,
        algProps
      )

      aggregator.expectMsgPF( 2.seconds.dilated, "first hour again" ) {
        case m @ SeriesOutliers( alg, source, plan, outliers, control ) ⇒ {
          alg mustBe Set( algoS )
          source mustBe series2
          m.hasAnomalies mustBe true
          outliers.size mustBe 1
          outliers mustBe series2.points.take( 1 )
        }
      }
    }

    "find outliers beyond first hour" in { f: Fixture ⇒ pending }

    "provide full threshold boundaries" in { f: Fixture ⇒
      import f._
      val analyzer = TestActorRef[FirstHourAverageAnalyzer]( FirstHourAverageAnalyzer.props( router.ref ) )
      val firstHour = FirstHourAverageAnalyzer.Context.FirstHour
      val now = firstHour.start
      val full = makeDataPoints(
        values = immutable.IndexedSeq.fill( 8 )( 1.0 ),
        start = now + 1.second,
        period = 10.minutes,
        timeWiggle = ( 0.97, 1.03 ),
        valueWiggle = ( 0.75, 1.25 )
      )

      val series = spike( full, 10 )()
      trace( s"test series = $series" )
      val algoS = FirstHourAverageAnalyzer.Algorithm
      val algProps = ConfigFactory.parseString( s"""${algoS}.tolerance: 3""" )

      analyzer ! DetectionAlgorithmRouter.AlgorithmRegistered( algoS )
      val history1 = historyWith( None, series )
      implicit val sender = aggregator.ref
      analyzer ! DetectUsing(
        plan.id,
        algoS,
        DetectOutliersInSeries( series, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
        history1,
        algProps
      )

      aggregator.expectMsgPF( 2.seconds.dilated, "first-hour threshold" ) {
        case m @ SeriesOutliers( alg, source, plan, outliers, actual ) ⇒ {
          actual.keySet mustBe Set( algoS )
          val expected = calculateControlBoundaries( series.points, 3 )
          actual( algoS ).zip( expected ).zipWithIndex foreach { case ( ( a, e ), i ) ⇒ compareControls( i, a, e ) }
        }
      }

      val full2 = makeDataPoints(
        values = immutable.IndexedSeq.fill( 3 )( 1.0 ),
        start = now + 70.minutes,
        period = 10.minutes,
        timeWiggle = ( 0.97, 1.03 ),
        valueWiggle = ( 0.0, 10 )
      )
      val series2 = spike( full2, 100 )( 0 )
      val history2 = historyWith( Option( history1 recordLastPoints series.points ), series2 )

      analyzer ! DetectUsing(
        plan.id,
        algoS,
        DetectOutliersInSeries( series2, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
        history2,
        algProps
      )

      aggregator.expectMsgPF( 2.seconds.dilated, "first-hour threshold again" ) {
        case m @ SeriesOutliers( alg, source, plan, outliers, actual ) ⇒ {
          actual.keySet mustBe Set( algoS )

          val expected = calculateControlBoundaries(
            points = series2.points,
            tolerance = 3,
            lastPoints = series.points
          )
          logger.error( "actual controls = [{}]", actual( algoS ).mkString( "\n", ", \n", "\n" ) )
          logger.error( "expected controls = [{}]", expected.mkString( "\n", ", \n", "\n" ) )
          actual( algoS ).zip( expected ).zipWithIndex foreach { case ( ( a, e ), i ) ⇒ compareControls( i, a, e ) }

          //          threshold( algoS ).zip( expectedControls ) foreach { case (a, e) =>
          //            a.timestamp mustBe e.timestamp
          //            a.floor.get mustBe (e.floor.get +- 0.005)
          //            a.expected.get mustBe (e.expected.get +- 0.005)
          //            a.ceiling.get mustBe (e.ceiling.get +- 0.005)
          //          }
          //
        }
      }
    }

    "controls are fixed beyond first hour" in { f: Fixture ⇒ pending }
  }
}

object SkylineFirstHourAverageSpec {
  val points = Seq(
    DataPoint( new joda.DateTime( 440 ), 9.46 ),
    DataPoint( new joda.DateTime( 441 ), 9.9 ),
    DataPoint( new joda.DateTime( 442 ), 11.6 ),
    DataPoint( new joda.DateTime( 443 ), 14.5 ),
    DataPoint( new joda.DateTime( 444 ), 17.3 ),
    DataPoint( new joda.DateTime( 445 ), 19.2 ),
    DataPoint( new joda.DateTime( 446 ), 18.4 ),
    DataPoint( new joda.DateTime( 447 ), 14.5 ),
    DataPoint( new joda.DateTime( 448 ), 12.2 ),
    DataPoint( new joda.DateTime( 449 ), 10.8 ),
    DataPoint( new joda.DateTime( 450 ), 8.58 ),
    DataPoint( new joda.DateTime( 451 ), 8.36 ),
    DataPoint( new joda.DateTime( 452 ), 8.58 ),
    DataPoint( new joda.DateTime( 453 ), 7.5 ),
    DataPoint( new joda.DateTime( 454 ), 7.1 ),
    DataPoint( new joda.DateTime( 455 ), 7.3 ),
    DataPoint( new joda.DateTime( 456 ), 7.71 ),
    DataPoint( new joda.DateTime( 457 ), 8.14 ),
    DataPoint( new joda.DateTime( 458 ), 8.14 ),
    DataPoint( new joda.DateTime( 459 ), 7.1 ),
    DataPoint( new joda.DateTime( 460 ), 7.5 ),
    DataPoint( new joda.DateTime( 461 ), 7.1 ),
    DataPoint( new joda.DateTime( 462 ), 7.1 ),
    DataPoint( new joda.DateTime( 463 ), 7.3 ),
    DataPoint( new joda.DateTime( 464 ), 7.71 ),
    DataPoint( new joda.DateTime( 465 ), 8.8 ),
    DataPoint( new joda.DateTime( 466 ), 9.9 ),
    DataPoint( new joda.DateTime( 467 ), 14.2 ),
    DataPoint( new joda.DateTime( 468 ), 18.8 ),
    DataPoint( new joda.DateTime( 469 ), 25.2 ),
    DataPoint( new joda.DateTime( 470 ), 31.5 ),
    DataPoint( new joda.DateTime( 471 ), 22 ),
    DataPoint( new joda.DateTime( 472 ), 24.1 ),
    DataPoint( new joda.DateTime( 473 ), 39.2 )
  )
}