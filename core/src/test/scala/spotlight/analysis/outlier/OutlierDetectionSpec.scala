package spotlight.analysis.outlier

import akka.actor.{ActorRef, Props}
import org.apache.http.HttpEntityEnclosingRequest

import scalaz.Scalaz.{when => _, _}
import com.typesafe.config.ConfigFactory
import spotlight.model.timeseries._

import scala.concurrent.duration._
import akka.testkit._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.testkit.ParallelAkkaSpec


/**
 * Created by rolfsd on 10/20/15.
 */
class OutlierDetectionSpec extends ParallelAkkaSpec with MockitoSugar {
  class Fixture extends AkkaFixture { fixture =>
    val router = TestProbe()
    val isQuorumA = mock[IsQuorum]
    val reduceA = mock[ReduceOutliers]

    val metric = Topic( "metric.a" )

    trait TestConfigurationProvider extends OutlierDetection.ConfigurationProvider {
      override def router: ActorRef = fixture.router.ref
    }

    val detect = TestActorRef[OutlierDetection with OutlierDetection.ConfigurationProvider](
      Props(
        new OutlierDetection with TestConfigurationProvider {
          override def preStart(): Unit = { }
        }
      )
    )

    val grouping: Option[OutlierPlan.Grouping] = {
      val window = None
      window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
    }

    val plans: Seq[OutlierPlan] = Seq(
      OutlierPlan.forTopics(
        name = "plan-a",
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA,
        algorithms = Set( 'foo, 'bar ),
        grouping = grouping,
        planSpecification = ConfigFactory.empty,
        extractTopic = OutlierDetection.extractOutlierDetectionTopic,
        topics = Set( Topic("metric") )
      )
    )
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

  override def makeAkkaFixture(): Fixture = new Fixture

  "OutlierDetection" should {
    "apply default plan if no other plan is assigned" in { f: Fixture =>
      import f._

      val grouping: Option[OutlierPlan.Grouping] = {
        val window = None
        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = OutlierPlan.default(
        name = "DEFAULT_PLAN",
        timeout = 2.seconds,
        algorithms = Set( 'foo, 'bar ),
        grouping = grouping,
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      detect.underlyingActor.router mustBe f.router.ref

      val msg = OutlierDetectionMessage( TimeSeries( topic = "dummy", points = Seq.empty[DataPoint] ), defaultPlan ).toOption.get

      detect receive msg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe Topic("dummy")
          algo must equal('foo)
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe Topic("dummy")
          algo must equal('bar)
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "apply plan if assigned" in { f: Fixture =>
      import f._

      val grouping: Option[OutlierPlan.Grouping] = {
        val window = None
        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = OutlierPlan.default(
        name = "dummy",
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA,
        algorithms = Set( 'foo, 'bar ),
        grouping = grouping,
        planSpecification = ConfigFactory.empty
      )

      val msg = OutlierDetectionMessage( TimeSeries( topic = metric, points = Seq.empty[DataPoint] ), defaultPlan ).toOption.get

      detect receive msg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe metric
          algo must equal('foo)
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe metric
          algo must equal('bar)
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "apply default plan if nameExtractor does not apply and no other plan is assigned" in { f: Fixture =>
      import f._

      val grouping: Option[OutlierPlan.Grouping] = {
        val window = None
        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = OutlierPlan.default(
        name = "DEFAULT_PLAN",
        algorithms = Set( 'zed ),
        grouping = grouping,
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      val msgForDefault = OutlierDetectionMessage(
        TimeSeries( topic = "dummy", points = Seq.empty[DataPoint] ),
        defaultPlan
      ).toOption.get

      detect receive msgForDefault

      router.expectMsgPF( 2.seconds.dilated, "default-routed" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe Topic( "dummy" )
          algo must equal('zed)
          payload mustBe msgForDefault
          properties mustBe ConfigFactory.empty
        }
      }

      val metricMsg = OutlierDetectionMessage(
        TimeSeries( topic = metric, points = Seq.empty[DataPoint] ),
        defaultPlan
      ).toOption.get

      detect receive metricMsg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-2" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe metric
          algo must equal('zed)
          payload mustBe metricMsg
          properties mustBe ConfigFactory.empty
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "history is updated with each detect request" taggedAs (WIP) in { f: Fixture =>
      import f._


      import org.joda.{ time => joda }
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

      val grouping: Option[OutlierPlan.Grouping] = {
        val window = None
        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = OutlierPlan.default(
        name = "dummy",
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA,
        algorithms = Set( 'foo, 'bar ),
        grouping = grouping,
        planSpecification = ConfigFactory.empty
      )

      val expectedA = HistoricalStatistics.fromActivePoints( pointsA, false )

      val msgA = OutlierDetectionMessage( TimeSeries( topic = metric, points = pointsA ), defaultPlan ).toOption.get
      detect receive msgA

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo-A" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe metric
          algo must equal('foo)
          history.N mustBe pointsA.size
          assertHistoricalStats( history, expectedA )
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar-A" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe metric
          algo must equal('bar)
          history.N mustBe pointsA.size
          assertHistoricalStats( history, expectedA )
        }
      }

      val expectedAB = pointsB.foldLeft( expectedA.recordLastPoints(pointsA) ){ (h, dp) => h :+ dp }
      expectedAB.N mustBe ( pointsA.size + pointsB.size)
      trace( s"expectedAB = $expectedAB" )
      trace( s"""expectedAB LAST= [${expectedAB.lastPoints.toPointTs.mkString(",")}]""" )

      val msgB = OutlierDetectionMessage( TimeSeries( topic = metric, points = pointsB ), defaultPlan ).toOption.get
      detect receive msgB

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo-AB" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          trace( s"history = $history" )
          m.topic mustBe metric
          algo must equal('foo)
          history.N mustBe ( pointsA.size + pointsB.size)
          trace( s"""   history LAST= [${history.lastPoints.toPointTs.mkString(",")}]""" )
          trace( s"""expectedAB LAST= [${expectedAB.lastPoints.toPointTs.mkString(",")}]""" )
          assertHistoricalStats( history, expectedAB )
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar-AB" ) {
        case m @ DetectUsing( algo, _, payload, history, properties ) => {
          m.topic mustBe metric
          algo must equal('bar)
          history.N mustBe ( pointsA.size + pointsB.size)
          assertHistoricalStats( history, expectedAB )
        }
      }
    }

    "evaluate multiple matching plans" in { f: Fixture =>
      import f._
      pending
    }
  }
}
