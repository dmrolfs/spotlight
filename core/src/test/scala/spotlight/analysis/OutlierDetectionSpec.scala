package spotlight.analysis

import scala.concurrent.duration._
import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit._

import scalaz.Scalaz.{ when ⇒ _, _ }
import org.scalatest.mockito.MockitoSugar
import com.typesafe.config.{ Config, ConfigFactory }
import peds.akka.envelope.{ Envelope, WorkId }
import spotlight.model.timeseries._
import spotlight.model.outlier._
import spotlight.testkit.{ ParallelAkkaSpec, TestCorrelatedSeries }

/** Created by rolfsd on 10/20/15.
  */
class OutlierDetectionSpec extends ParallelAkkaSpec with MockitoSugar {

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AkkaFixture( _config, _system, _slug ) {
    fixture ⇒

    val router = TestProbe()
    val subscriber = TestProbe()
    val isQuorumA = mock[IsQuorum]
    val reduceA = mock[ReduceOutliers]

    val metric = Topic( "metric.a" )

    trait TestConfigurationProvider extends OutlierDetection.ConfigurationProvider {
      override def router: ActorRef = fixture.router.ref
    }

    val detect = TestActorRef[OutlierDetection with OutlierDetection.ConfigurationProvider](
      Props(
        new OutlierDetection with TestConfigurationProvider {
          override def preStart(): Unit = {}
        }
      )
    )

    val grouping: Option[AnalysisPlan.Grouping] = {
      val window = None
      window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
    }

    val plan = AnalysisPlan.forTopics(
      name = "plan-a",
      timeout = 2.seconds,
      isQuorum = isQuorumA,
      reduce = reduceA,
      algorithms = Set( "foo", "bar" ),
      grouping = grouping,
      planSpecification = ConfigFactory.empty,
      extractTopic = OutlierDetection.extractOutlierDetectionTopic,
      topics = Set( Topic( "metric" ) )
    )

    val plans: Set[AnalysisPlan] = Set( plan )
  }

  def assertHistoricalStats( actual: HistoricalStatistics, expected: HistoricalStatistics ): Unit = {
    actual.covariance mustBe expected.covariance
    actual.dimension mustBe expected.dimension
    actual.geometricMean mustBe expected.geometricMean
    actual.max mustBe expected.max
    actual.mean mustBe expected.mean
    actual.min mustBe expected.min
    actual.N mustBe expected.N
    actual.standardDeviation mustBe expected.standardDeviation
    actual.sum mustBe expected.sum
    actual.sumLog mustBe expected.sumLog
    actual.sumOfSquares mustBe expected.sumOfSquares
    actual.lastPoints.flatten mustBe expected.lastPoints.flatten
  }

  "OutlierDetection" should {
    "apply default plan if no other plan is assigned" in { f: Fixture ⇒
      import f._

      val grouping: Option[AnalysisPlan.Grouping] = {
        val window = None
        window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = AnalysisPlan.default(
        name = "DEFAULT_PLAN",
        timeout = 2.seconds,
        algorithms = Set( "foo", "bar" ),
        grouping = grouping,
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      detect.underlyingActor.router mustBe f.router.ref

      val msg = OutlierDetectionMessage(
        TimeSeries( topic = "dummy", points = Seq.empty[DataPoint] ),
        defaultPlan,
        Option( subscriber.ref )
      ).toOption.get

      detect receive msg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe Topic( "dummy" )
          algo must equal( "foo" )
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe Topic( "dummy" )
          algo must equal( "bar" )
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "apply plan if assigned" in { f: Fixture ⇒
      import f._

      val grouping: Option[AnalysisPlan.Grouping] = {
        val window = None
        window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = AnalysisPlan.default(
        name = "dummy",
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA,
        algorithms = Set( "foo", "bar" ),
        grouping = grouping,
        planSpecification = ConfigFactory.empty
      )

      val msg = OutlierDetectionMessage(
        TimeSeries( topic = metric, points = Seq.empty[DataPoint] ),
        defaultPlan,
        Option( subscriber.ref )
      ).toOption.get

      detect receive msg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe metric
          algo must equal( "foo" )
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe metric
          algo must equal( "bar" )
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "apply default plan if nameExtractor does not apply and no other plan is assigned" in { f: Fixture ⇒
      import f._

      val grouping: Option[AnalysisPlan.Grouping] = {
        val window = None
        window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = AnalysisPlan.default(
        name = "DEFAULT_PLAN",
        algorithms = Set( "zed" ),
        grouping = grouping,
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      val msgForDefault = OutlierDetectionMessage(
        TimeSeries( topic = "dummy", points = Seq.empty[DataPoint] ),
        defaultPlan,
        Option( subscriber.ref )
      ).toOption.get

      detect receive msgForDefault

      router.expectMsgPF( 2.seconds.dilated, "default-routed" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe Topic( "dummy" )
          algo must equal( "zed" )
          payload mustBe msgForDefault
          properties mustBe ConfigFactory.empty
        }
      }

      val metricMsg = OutlierDetectionMessage(
        TimeSeries( topic = metric, points = Seq.empty[DataPoint] ),
        defaultPlan,
        Option( subscriber.ref )
      ).toOption.get

      detect receive metricMsg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-2" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe metric
          algo must equal( "zed" )
          payload mustBe metricMsg
          properties mustBe ConfigFactory.empty
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "shape is updated with each detect request" in { f: Fixture ⇒
      import f._

      import org.joda.{ time ⇒ joda }
      val pointsA = Seq(
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
        DataPoint( new joda.DateTime( 467 ), 14.2 )
      )

      val pointsB = Seq(
        DataPoint( new joda.DateTime( 440 ), 10.1 ),
        DataPoint( new joda.DateTime( 441 ), 10.1 ),
        DataPoint( new joda.DateTime( 442 ), 9.68 ),
        DataPoint( new joda.DateTime( 443 ), 9.46 ),
        DataPoint( new joda.DateTime( 444 ), 10.3 ),
        DataPoint( new joda.DateTime( 445 ), 11.6 ),
        DataPoint( new joda.DateTime( 446 ), 13.9 ),
        DataPoint( new joda.DateTime( 447 ), 13.9 ),
        DataPoint( new joda.DateTime( 448 ), 12.5 ),
        DataPoint( new joda.DateTime( 449 ), 11.9 ),
        DataPoint( new joda.DateTime( 450 ), 12.2 ),
        DataPoint( new joda.DateTime( 451 ), 13 ),
        DataPoint( new joda.DateTime( 452 ), 13.3 ),
        DataPoint( new joda.DateTime( 453 ), 13 ),
        DataPoint( new joda.DateTime( 454 ), 12.7 ),
        DataPoint( new joda.DateTime( 455 ), 11.9 ),
        DataPoint( new joda.DateTime( 456 ), 13.3 ),
        DataPoint( new joda.DateTime( 457 ), 12.5 ),
        DataPoint( new joda.DateTime( 458 ), 11.9 ),
        DataPoint( new joda.DateTime( 459 ), 11.6 ),
        DataPoint( new joda.DateTime( 460 ), 10.5 ),
        DataPoint( new joda.DateTime( 461 ), 10.1 ),
        DataPoint( new joda.DateTime( 462 ), 9.9 ),
        DataPoint( new joda.DateTime( 463 ), 9.68 ),
        DataPoint( new joda.DateTime( 464 ), 9.68 ),
        DataPoint( new joda.DateTime( 465 ), 9.9 ),
        DataPoint( new joda.DateTime( 466 ), 10.8 ),
        DataPoint( new joda.DateTime( 467 ), 11 )
      )

      val grouping: Option[AnalysisPlan.Grouping] = {
        val window = None
        window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = AnalysisPlan.default(
        name = "dummy",
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA,
        algorithms = Set( "foo", "bar" ),
        grouping = grouping,
        planSpecification = ConfigFactory.empty
      )

      val expectedA = HistoricalStatistics.fromActivePoints( pointsA, isCovarianceBiasCorrected = false )

      val msgA = OutlierDetectionMessage(
        TimeSeries( topic = metric, points = pointsA ),
        defaultPlan,
        Option( subscriber.ref )
      ).toOption.get
      detect receive msgA

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo-A" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe metric
          algo must equal( "foo" )
          history.N mustBe pointsA.size
          assertHistoricalStats( history, expectedA )
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar-A" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe metric
          algo must equal( "bar" )
          history.N mustBe pointsA.size
          assertHistoricalStats( history, expectedA )
        }
      }

      val expectedAB = pointsB.foldLeft( expectedA.recordLastPoints( pointsA ) ) { ( h, dp ) ⇒ h :+ dp }
      expectedAB.N mustBe ( pointsA.size + pointsB.size )
      trace( s"expectedAB = $expectedAB" )
      trace( s"""expectedAB LAST= [${expectedAB.lastPoints.toPointTs.mkString( "," )}]""" )

      val msgB = OutlierDetectionMessage(
        TimeSeries( topic = metric, points = pointsB ),
        defaultPlan,
        Option( subscriber.ref )
      ).toOption.get
      detect receive msgB

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo-AB" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          trace( s"shape = $history" )
          m.topic mustBe metric
          algo must equal( "foo" )
          history.N mustBe ( pointsA.size + pointsB.size )
          trace( s"""   shape LAST= [${history.lastPoints.toPointTs.mkString( "," )}]""" )
          trace( s"""expectedAB LAST= [${expectedAB.lastPoints.toPointTs.mkString( "," )}]""" )
          assertHistoricalStats( history, expectedAB )
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar-AB" ) {
        case Envelope( m @ DetectUsing( _, algo, payload, history, properties ), _ ) ⇒ {
          m.topic mustBe metric
          algo must equal( "bar" )
          history.N mustBe ( pointsA.size + pointsB.size )
          assertHistoricalStats( history, expectedAB )
        }
      }
    }

    "evaluate multiple matching plans" in { f: Fixture ⇒
      import f._
      pending
    }
  }
}
