package lineup.analysis.outlier

import akka.actor.{ActorRef, UnhandledMessage}
import com.typesafe.config.ConfigFactory
import lineup.analysis.outlier.OutlierDetection.UnrecognizedTopic
import lineup.model.timeseries._
import scala.concurrent.duration._
import akka.testkit._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import lineup.model.outlier.{ ReduceOutliers, IsQuorum, OutlierPlan }
import lineup.testkit.ParallelAkkaSpec

import scala.util.Try


/**
 * Created by rolfsd on 10/20/15.
 */
class OutlierDetectionSpec extends ParallelAkkaSpec with MockitoSugar {
  class Fixture extends AkkaFixture { fixture =>
    val router = TestProbe()
    val isQuorumA = mock[IsQuorum]
    val reduceA = mock[ReduceOutliers]

    val metric = Topic( "metric.a" )

    trait TestConfigurationProvider extends OutlierDetection.PlanConfigurationProvider {
      override def router: ActorRef = fixture.router.ref
      override def getPlans: () => Try[Seq[OutlierPlan]] = () => { Try{ fixture.plans } }
      override def invalidateCaches(): Unit = { }
      override def refreshInterval: FiniteDuration = 5.minutes
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
    "ignore detect message if no plan and no default" taggedAs (WIP) in { f: Fixture =>
      import f._
      val probe = TestProbe()
      system.eventStream.subscribe( probe.ref, classOf[UnhandledMessage] )
      val detect = TestActorRef[OutlierDetection]( OutlierDetection.props{ new OutlierDetection with TestConfigurationProvider } )

      val msg = mock[OutlierDetectionMessage]
      when( msg.topic ) thenReturn Topic("dummy")

      detect.receive( msg, probe.ref )

      probe.expectMsgPF( 2.seconds.dilated, "unrecognized message" ) {
        case UnrecognizedTopic( t ) => {
          t mustBe Topic("dummy")
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "apply default plan if no other plan is assigned" in { f: Fixture =>
      import f._

      val defaultPlan = OutlierPlan.default(
        name = "DEFAULT_PLAN",
        timeout = 2.seconds,
        algorithms = Set( 'foo, 'bar ),
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      val detect = TestActorRef[OutlierDetection](
        OutlierDetection.props {
          new OutlierDetection with TestConfigurationProvider {
            override def preStart(): Unit = { }
            override def getPlans: () => Try[Seq[OutlierPlan]] = () => { Try{ Seq(defaultPlan) } }
          }
        }
      )

      val msg = OutlierDetectionMessage( TimeSeries( topic = "dummy", points = Row.empty[DataPoint] ) )

      detect receive msg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe Topic("dummy")
          algo must equal('foo)
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
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
            override def getPlans: () => Try[Seq[OutlierPlan]] = () => { Try{ f.plans :+ defaultPlan } }
          }
        }
      )

      val msg = OutlierDetectionMessage( TimeSeries( topic = metric, points = Row.empty[DataPoint] ) )

      detect receive msg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe metric
          algo must equal('foo)
          payload mustBe msg
          properties mustBe ConfigFactory.empty
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
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
            override def getPlans: () => Try[Seq[OutlierPlan]] = () => { Try{ f.plans :+ defaultPlan } }
          }
        }
      )

      val msgForDefault = OutlierDetectionMessage( TimeSeries( topic = "dummy", points = Row.empty[DataPoint] ) )

      detect receive msgForDefault

      router.expectMsgPF( 2.seconds.dilated, "default-routed" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe Topic( "dummy" )
          algo must equal('zed)
          payload mustBe msgForDefault
          properties mustBe ConfigFactory.empty
        }
      }

      val metricMsg = OutlierDetectionMessage( TimeSeries( topic = metric, points = Row.empty[DataPoint] ) )

      detect receive metricMsg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-2" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe metric
          algo must equal('zed)
          payload mustBe metricMsg
          properties mustBe ConfigFactory.empty
        }
      }

      router expectNoMsg 1.second.dilated
    }

  }
}
