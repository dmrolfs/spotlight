package lineup.analysis.outlier

import akka.actor.UnhandledMessage
import lineup.model.timeseries.Topic
import scala.concurrent.duration._
import akka.testkit._
import demesne.testkit.ParallelAkkaSpec
import org.mockito.Mockito._
import org.scalatest.{ Tag, Outcome }
import org.scalatest.mock.MockitoSugar
import peds.commons.log.Trace
import lineup.model.outlier.{ ReduceOutliers, IsQuorum, OutlierPlan }


/**
 * Created by rolfsd on 10/20/15.
 */
class OutlierDetectionSpec extends ParallelAkkaSpec with MockitoSugar {
  val trace = Trace[OutlierDetectionSpec]

  class Fixture extends AkkaFixture {
    def before(): Unit = { }
    def after(): Unit = { }

    val router = TestProbe()
    val isQuorumA = mock[IsQuorum]
    val reduceA = mock[ReduceOutliers]

    val metric = Topic( "metric.a" )

    val plans: Map[Topic, OutlierPlan] = Map(
      metric -> OutlierPlan(
        name = "plan-a",
        algorithms = Set( 'foo, 'bar ),
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA
      )
    )
  }

  override def createAkkaFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = trace.block( s"withFixture($test" ) {
    val f = createAkkaFixture()

    try {
      f.before()
      test( f )
    } finally {
      f.after()
      f.system.shutdown()
    }
  }

  case object WIP extends Tag( "wip" )

  "OutlierDetection" should {
    "ignore detect message if no plan and no default" in { f: Fixture =>
      import f._
      val probe = TestProbe()
      system.eventStream.subscribe( probe.ref, classOf[UnhandledMessage] )
      val detect = TestActorRef[OutlierDetection]( OutlierDetection.props( router.ref, plans ) )

      val msg = mock[OutlierDetectionMessage]
      when( msg.topic ) thenReturn Topic("dummy")

      detect receive msg

      probe.expectMsgPF( 2.seconds.dilated, "unhandled message" ) {
        case UnhandledMessage( m, _, r ) => {
          m mustBe msg
          r mustBe detect
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "apply default plan if no other plan is assigned" in { f: Fixture =>
      import f._

      val defaultPlan = OutlierPlan(
        name = "DEFAULT_PLAN",
        algorithms = Set( 'foo, 'bar ),
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      val detect = TestActorRef[OutlierDetection]( OutlierDetection.props( router.ref, plans, Some(defaultPlan) ) )

      val msg = mock[OutlierDetectionMessage]
      when( msg.topic ) thenReturn "dummy"

      detect receive msg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe Topic("dummy")
          algo must equal('foo)
          payload mustBe msg
          properties mustBe Map.empty[String, Any]
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe Topic("dummy")
          algo must equal('bar)
          payload mustBe msg
          properties mustBe Map.empty[String, Any]
        }
      }

      router expectNoMsg 1.second.dilated
    }

    "apply plan if assigned" in { f: Fixture =>
      import f._

      val defaultPlan = mock[OutlierPlan]

      val detect = TestActorRef[OutlierDetection]( OutlierDetection.props( router.ref, plans, Some(defaultPlan) ) )

      val msg = mock[OutlierDetectionMessage]
      when( msg.topic ) thenReturn metric

      detect receive msg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-foo" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe metric
          algo must equal('foo)
          payload mustBe msg
          properties mustBe Map.empty[String, Any]
        }
      }

      router.expectMsgPF( 2.seconds.dilated, "default-routed-bar" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe metric
          algo must equal('bar)
          payload mustBe msg
          properties mustBe Map.empty[String, Any]
        }
      }

      router expectNoMsg 1.second.dilated

      verify( defaultPlan, never() ).algorithms
    }

    "apply default plan if nameExtractor does not apply and no other plan is assigned" taggedAs(WIP) in { f: Fixture =>
      import f._

      val defaultPlan = OutlierPlan(
        name = "DEFAULT_PLAN",
        algorithms = Set( 'zed ),
        timeout = 2.seconds,
        isQuorum = isQuorumA,
        reduce = reduceA
      )

      val emptyExtractId: OutlierDetection.ExtractId = peds.commons.util.emptyBehavior[Any, Topic]

      val detect = TestActorRef[OutlierDetection](
        OutlierDetection.props( router.ref, plans, Some(defaultPlan), emptyExtractId )
      )

      val msgForDefault = mock[OutlierDetectionMessage]
      when( msgForDefault.topic ) thenReturn "dummy"

      detect receive msgForDefault

      router.expectMsgPF( 2.seconds.dilated, "default-routed" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe Topic( "dummy" )
          algo must equal('zed)
          payload mustBe msgForDefault
          properties mustBe Map.empty[String, Any]
        }
      }

      val metricMsg = mock[OutlierDetectionMessage]
      when( metricMsg.topic ) thenReturn metric

      detect receive metricMsg

      router.expectMsgPF( 2.seconds.dilated, "default-routed-2" ) {
        case m @ DetectUsing( algo, _, payload, properties ) => {
          m.topic mustBe metric
          algo must equal('zed)
          payload mustBe metricMsg
          properties mustBe Map.empty[String, Any]
        }
      }

      router expectNoMsg 1.second.dilated
    }

  }
}
