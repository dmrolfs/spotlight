package lineup.analysis.outlier

import akka.actor.{ ActorRef }
import scalaz.Scalaz.{ when => _, _ }
import com.typesafe.config.ConfigFactory
import lineup.model.timeseries._
import scala.concurrent.duration._
import akka.testkit._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import lineup.model.outlier.{ ReduceOutliers, IsQuorum, OutlierPlan }
import lineup.testkit.ParallelAkkaSpec


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

    val plans: Seq[OutlierPlan] = Seq(
      OutlierPlan.forTopics(
        name = "plan-a",
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA,
        algorithms = Set( 'foo, 'bar ),
        specification = ConfigFactory.empty,
        extractTopic = OutlierDetection.extractOutlierDetectionTopic,
        topics = Set( Topic("metric") )
      )
    )
  }

  override def makeAkkaFixture(): Fixture = new Fixture

  "OutlierDetection" should {
    "apply default plan if no other plan is assigned" taggedAs (WIP) in { f: Fixture =>
      import f._

      val defaultPlan = OutlierPlan.default(
        name = "DEFAULT_PLAN",
        timeout = 2.seconds,
        algorithms = Set( 'foo, 'bar ),
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      val detect = TestActorRef[OutlierDetection with OutlierDetection.ConfigurationProvider](
        OutlierDetection.props {
          new OutlierDetection with TestConfigurationProvider {
            override def preStart(): Unit = { }
          }
        }
      )

      detect.underlyingActor.router mustBe f.router.ref

      val msg = OutlierDetectionMessage( TimeSeries( topic = "dummy", points = Row.empty[DataPoint] ), defaultPlan ).toOption.get

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

      val defaultPlan = OutlierPlan.default(
        name = "dummy",
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA,
        algorithms = Set( 'foo, 'bar ),
        specification = ConfigFactory.empty
      )

      val detect = TestActorRef[OutlierDetection](
        OutlierDetection.props {
          new OutlierDetection with TestConfigurationProvider {
            override def preStart(): Unit = { }
          }
        }
      )

      val msg = OutlierDetectionMessage( TimeSeries( topic = metric, points = Row.empty[DataPoint] ), defaultPlan ).toOption.get

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

      val defaultPlan = OutlierPlan.default(
        name = "DEFAULT_PLAN",
        algorithms = Set( 'zed ),
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      val detect = TestActorRef[OutlierDetection](
        OutlierDetection.props {
          new OutlierDetection with TestConfigurationProvider {
            override def preStart(): Unit = { }
          }
        }
      )

      val msgForDefault = OutlierDetectionMessage(
        TimeSeries( topic = "dummy", points = Row.empty[DataPoint] ),
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
        TimeSeries( topic = metric, points = Row.empty[DataPoint] ),
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

    "evaluate multiple matching plans" in { f: Fixture =>
      import f._
      pending
    }
  }
}
