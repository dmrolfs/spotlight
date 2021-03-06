//package spotlight.analysis.algorithm.skyline
//
//import scala.collection.immutable
//import scala.concurrent.duration._
//import akka.actor.{ActorSystem, Props}
//import akka.testkit._
//import com.typesafe.config.{Config, ConfigFactory}
//import org.joda.{time => joda}
//import org.mockito.Mockito._
//import spotlight.analysis.{DetectOutliersInSeries, DetectUsing, DetectionAlgorithmRouter, Moment}
//import spotlight.model.outlier._
//import spotlight.testkit.TestCorrelatedSeries
//
//
///**
//  * Created by rolfsd on 2/15/16.
//  */
//class SkylineSeasonalEWMASpec extends SkylineBaseSpec {
//  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
//    new Fixture( config, system, slug )
//  }
//
//  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends SkylineFixture( _config, _system, _slug ) {
//    val algoS: Symbol = SeasonalExponentialMovingAverageAnalyzer.Algorithm
//    val algProps = ConfigFactory.parseString(
//      s"""
//         |${algoS.name}.tolerance: 3
//         |${algoS.name}.wavelength: 1 hour
//         |${algoS.name}.bins = 4
//       """.stripMargin
//    )
//    val plan = mock[AnalysisPlan]
//    when( plan.name ).thenReturn( "mock-plan" )
//    when( plan.appliesTo ).thenReturn( SkylineFixture.appliesToAll )
//    when( plan.algorithms ).thenReturn( Set(algoS) )
//  }
//
//
//  "SeasonalExponentialMovingAverageAnalyzer" should {
//    "create SeasonModel" in { f: Fixture =>
//      import SeasonalExponentialMovingAverageAnalyzer.SeasonalModel
//      val startOfToday = joda.LocalDate.now.toDateTimeAtStartOfDay
//      val actualModel = SeasonalModel( reference = startOfToday, waveLength = joda.Days.ONE.toStandardDuration, bins = 24 ).toOption.get
//      actualModel.waveLength mustBe joda.Days.ONE.toStandardDuration
//      actualModel.binLength mustBe joda.Hours.ONE.toStandardDuration
//      actualModel.bins mustBe 24
//      actualModel.reference mustBe startOfToday
//
//      import scalaz._, Scalaz._
//      val expectedMoments = ( 0 until 24 ).map{ i => Moment.withAlpha( id = s"seasonal-${i}", alpha = 0.05 ) }.toList.sequence.toOption.get
//      val concreteModel = actualModel.asInstanceOf[SeasonalModel.SimpleSeasonalModel]
//      concreteModel.moments mustBe expectedMoments.toVector
//    }
//
//    "identifies season start for date" in { f: Fixture =>
//      import SeasonalExponentialMovingAverageAnalyzer.SeasonalModel
//      val startOfToday = joda.LocalDate.now.toDateTimeAtStartOfDay
//      val today = joda.LocalDate.now
//      val tomorrow = today plus joda.Days.ONE
//      val weekFromToday = today plus joda.Weeks.ONE
//      val endOfMonth = new joda.DateTime( 2016, 10, 31, 7, 0, 0, 1 )
//      val model = SeasonalModel( reference = startOfToday, waveLength = joda.Days.ONE.toStandardDuration, bins = 24 ).toOption.get
//      model.seasonStartFor( today.toDateTime( new joda.LocalTime(0, 0, 0) ) ) mustBe startOfToday
//      model.seasonStartFor( today.toDateTime( new joda.LocalTime(1, 0, 37, 746) ) ) mustBe startOfToday
//      model.seasonStartFor( today.toDateTime( new joda.LocalTime(23, 59, 37, 746) ) ) mustBe startOfToday
//      model.seasonStartFor( today.toDateTime( new joda.LocalTime(23, 59, 59, 999) ) ) mustBe startOfToday
//      model.seasonStartFor( tomorrow.toDateTime( new joda.LocalTime(13, 37, 59, 999) ) ) mustBe (startOfToday plus joda.Days.ONE)
////      model.seasonStartFor( weekFromToday.toDateTime( new joda.LocalTime(7, 0, 0, 1) ) ) mustBe (startOfToday plus joda.Weeks.ONE)
//      model.seasonStartFor( endOfMonth plus joda.Weeks.ONE ) mustBe (startOfToday plus joda.Weeks.ONE)
//    }
//
//    "identifies bin start for date" in { f: Fixture =>
//      import SeasonalExponentialMovingAverageAnalyzer.SeasonalModel
//      val startOfToday = joda.LocalDate.now.toDateTimeAtStartOfDay
//      val today = joda.LocalDate.now
//      val tomorrow = today plus joda.Days.ONE
//      val weekFromToday = today plus joda.Weeks.ONE
//      val model = {
//        SeasonalModel( reference = startOfToday, waveLength = joda.Days.ONE.toStandardDuration, bins = 24 )
//        .toOption
//        .get
//        .asInstanceOf[SeasonalModel.SimpleSeasonalModel]
//      }
//      model.binStartFor( today.toDateTime( new joda.LocalTime(0, 0, 0) ) ) mustBe today.toDateTime( new joda.LocalTime(0,0,0) )
//      model.binStartFor( today.toDateTime( new joda.LocalTime(1, 0, 37, 746) ) ) mustBe today.toDateTime( new joda.LocalTime(1,0,0) )
//      model.binStartFor( today.toDateTime( new joda.LocalTime(23, 59, 37, 746) ) ) mustBe today.toDateTime( new joda.LocalTime(23,0,0) )
//      model.binStartFor( today.toDateTime( new joda.LocalTime(23, 59, 59, 999) ) ) mustBe today.toDateTime( new joda.LocalTime(23,0,0) )
//      model.binStartFor( tomorrow.toDateTime( new joda.LocalTime(13, 37, 59, 999) ) ) mustBe tomorrow.toDateTime( new joda.LocalTime(13,0,0) )
//      model.binStartFor( weekFromToday.toDateTime( new joda.LocalTime(7, 0, 0, 1) ) ) mustBe weekFromToday.toDateTime( new joda.LocalTime(7,0,0) )
//    }
//
//    "identified bin for date" in { f: Fixture =>
//      import SeasonalExponentialMovingAverageAnalyzer.SeasonalModel
//      val startOfToday = joda.LocalDate.now.toDateTimeAtStartOfDay
//      // would be nice to use now() but joda smartly handles DST which makes calculating expected bin slightly more than involved
//      // than I prefer to handle at this time
//      val today = new joda.LocalDate( 2016, 8, 30 )
//      val tomorrow = today plus joda.Days.ONE
//      val weekFromToday = today plus joda.Weeks.ONE
//      val model = {
//        SeasonalModel( reference = startOfToday, waveLength = joda.Days.ONE.toStandardDuration, bins = 24 )
//        .toOption
//        .get
//        .asInstanceOf[SeasonalModel.SimpleSeasonalModel]
//      }
//      model.binFor( today.toDateTime( new joda.LocalTime(0, 0, 0) ) ) mustBe 0
//      model.binFor( today.toDateTime( new joda.LocalTime(1, 0, 37, 746) ) ) mustBe 1
//      model.binFor( today.toDateTime( new joda.LocalTime(23, 59, 37, 746) ) ) mustBe 23
//      model.binFor( today.toDateTime( new joda.LocalTime(23, 59, 59, 999) ) ) mustBe 23
//      model.binFor( tomorrow.toDateTime( new joda.LocalTime(13, 37, 59, 999) ) ) mustBe 13
//      model.binFor( weekFromToday.toDateTime( new joda.LocalTime(7, 0, 0, 1) ) ) mustBe 7
//    }
//
//    "identified bin for earlier date" in { f: Fixture =>
//      import SeasonalExponentialMovingAverageAnalyzer.SeasonalModel
//      // would be nice to use now() but joda smartly handles DST which makes calculating expected bin slightly more than involved
//      // than I prefer to handle at this time
//      val today = new joda.LocalDate( 2016, 8, 30 )
//      val startOfToday = today.toDateTimeAtStartOfDay
//      val yesterday = today minus joda.Days.ONE
//      val weekBeforeToday = today minus joda.Weeks.ONE
//      val model = {
//        SeasonalModel( reference = startOfToday, waveLength = joda.Days.ONE.toStandardDuration, bins = 24 )
//        .toOption
//        .get
//        .asInstanceOf[SeasonalModel.SimpleSeasonalModel]
//      }
//      model.binFor( yesterday.toDateTime( new joda.LocalTime(0, 0, 0) ) ) mustBe 0
//      model.binFor( yesterday.toDateTime( new joda.LocalTime(1, 0, 37, 746) ) ) mustBe 1
//      model.binFor( yesterday.toDateTime( new joda.LocalTime(23, 59, 37, 746) ) ) mustBe 23
//      model.binFor( yesterday.toDateTime( new joda.LocalTime(23, 59, 59, 999) ) ) mustBe 23
//      model.binFor( yesterday.toDateTime( new joda.LocalTime(13, 37, 59, 999) ) ) mustBe 13
//      model.binFor( weekBeforeToday.toDateTime( new joda.LocalTime(7, 0, 0, 1) ) ) mustBe 7
//    }
//
//    "track bin averages for seasons" in { f: Fixture =>
//      import f._
//      import SeasonalExponentialMovingAverageAnalyzer.SeasonalModel
//
//      val startOfToday = joda.LocalDate.now.toDateTimeAtStartOfDay
//      val seed = {
//        SeasonalModel( reference = startOfToday, waveLength = joda.Weeks.ONE.toStandardDuration, bins = 7 )
//        .toOption
//        .get
//      }
//
//      val points = {
//        ( 0 until 7*24*60*60 )
//        .map { i =>
//          val ts = startOfToday.plusSeconds( i )
//          val v = ts.getDayOfMonth
//          ( ts, v )
//        }
//      }
//
//      val seasonalModel = {
//        points
//        .foldLeft( seed ){ case (sm, (ts, v)) =>
//          val m = sm momentAt ts
//          sm.withMomentAtDateTime( m :+ v, ts )
//        }
//        .asInstanceOf[SeasonalModel.SimpleSeasonalModel]
//      }
//
//      //todo 1.0 was not 31.0 plus or minus 1.0E-5 (SkylineSeasonalEWMASpec.scala:163) run on Mon 4/25
//      seasonalModel.moments.zipWithIndex foreach { case (m, i) =>
//        m.statistics.get.ewma mustBe ( startOfToday.plusDays(i).getDayOfMonth.toDouble +- 0.00001 )
//        m.statistics.get.ewmsd mustBe ( 0.0 +- 0.00001 )
//      }
//    }
//
//    "find outliers deviating stddev from seasonal exponential moving average" in { f: Fixture =>
//      import f._
//
//      trace( s"plan.algorithms = ${plan.algorithms}")
//      plan.algorithms mustBe Set( f.algoS )
//
//      val now = joda.DateTime.now
//      val offset = new joda.Duration(1.hour.toMillis)
//      val referencePoint = now minus offset
//
//      implicit val sender = aggregator.ref
//
//      val analyzer = TestActorRef[SeasonalExponentialMovingAverageAnalyzer](
//        Props {
//          new SeasonalExponentialMovingAverageAnalyzer( router.ref )
//              with SeasonalExponentialMovingAverageAnalyzer.ReferenceProvider {
//            override def reference: joda.DateTime = referencePoint
//          }
//        }
//      )
//
//      val full = makeDataPoints(
//        values = immutable.IndexedSeq.fill( 60 )( 1.0 ),
//        start = (referencePoint plus 1.second),
//        period = 1.minute,
//        timeWiggle = (0.97, 1.00),
//        valueWiggle = (1.0, 1.0)
//      )
//
//      val series = spike( full )()
//      trace( s"test series = $series" )
//
//      analyzer ! DetectionAlgorithmRouter.AlgorithmRegistered( algoS)
//      val history1 = historyWith( None, series )
//
//      analyzer ! DetectUsing(
//        algoS,
//        DetectOutliersInSeries(TestCorrelatedSeries(series), plan, subscriber.ref),
//        history1,
//        algProps
//      )
//
//      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from seasonal ewma" ) {
//        case m @ SeriesOutliers(alg, source, plan, outliers, control) => {
//          alg mustBe Set( algoS )
//          source mustBe series
//          m.hasAnomalies mustBe true
//          outliers.size mustBe 1
//          outliers mustBe Seq( series.points.last )
//        }
//      }
//
//      val full2 = makeDataPoints(
//        values = immutable.IndexedSeq.fill( 60 )( 1.0 ),
//        start = (now plus 70.seconds),
//        period = 1.minute,
//        timeWiggle = (1.0, 1.03),
//        valueWiggle = (1.0, 1.0)
//      )
//
//      val series2 = spike( full2 )( 0 )
//      val history2 = historyWith( Option(history1 recordLastPoints series.points), series2 )
//
//      analyzer ! DetectUsing(
//        algoS,
//        DetectOutliersInSeries(TestCorrelatedSeries(series2), plan, subscriber.ref),
//        history2,
//        algProps
//      )
//
//      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from seasonal ewma again" ) {
//        case m @ SeriesOutliers(alg, source, plan, outliers, control) => {
//          alg mustBe Set(algoS)
//          source mustBe series2
//          m.hasAnomalies mustBe true
//          outliers.size mustBe 1
//          outliers mustBe series2.points.take(1)
//        }
//      }
//    }
//
////    "publish outliers result to" in { f: Fixture =>
////      import f._
////      val analyzer = TestActorRef[ExponentialMovingAverageAnalyzer]( ExponentialMovingAverageAnalyzer.elasticProps(router.ref) )
////      val full = makeDataPoints(
////                                 values = immutable.IndexedSeq.fill( 5 )( 1.0 ),
////                                 timeWiggle = (0.97, 1.03),
////                                 valueWiggle = (1.0, 1.0)
////                               )
////
////      val series = spike( full )()
////      trace( s"test series = $series" )
////
////      analyzer.receive( DetectionAlgorithmRouter.AlgorithmRegistered( algoS ) )
////      val history1 = historyWith( None, series )
////      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series, plan), history1, algProps ) )
////      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from moving average" ) {
////        case m @ SeriesOutliers(alg, source, plan, outliers) => {
////          alg mustBe Set( algoS )
////          source mustBe series
////          m.hasAnomalies mustBe true
////          outliers.size mustBe 1
////          outliers mustBe Row( series.points.last )
////        }
////      }
////
////      val full2 = makeDataPoints(
////                                  values = immutable.IndexedSeq.fill( 5 )( 1.0 ),
////                                  timeWiggle = (0.97, 1.03),
////                                  valueWiggle = (1.0, 1.0)
////                                )
////
////      val series2 = spike( full2 )( 0 )
////      val history2 = historyWith( Option(history1.recordLastDataPoints(series.points)), series2 )
////
////      analyzer.receive( DetectUsing( algoS, aggregator.ref, DetectOutliersInSeries(series2, plan), history2, algProps ) )
////      aggregator.expectMsgPF( 2.seconds.dilated, "stddev from moving average again" ) {
////        case m @ NoOutliers(alg, source, plan) => {
////          alg mustBe Set( algoS )
////          source mustBe series2
////          m.hasAnomalies mustBe false
////        }
////      }
////    }
//  }
//}
