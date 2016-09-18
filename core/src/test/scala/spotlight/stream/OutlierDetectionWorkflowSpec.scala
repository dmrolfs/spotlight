package spotlight.stream

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import akka.actor.{Actor, ActorContext, ActorRef, Props}
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import com.typesafe.config.Config
import org.scalatest.Tag
import org.scalatest.mockito.MockitoSugar
import peds.akka.supervision.OneForOneStrategyFactory
import peds.commons.log.Trace
import peds.akka.supervision.IsolatedLifeCycleSupervisor.{ChildStarted, Started, WaitForStart}
import spotlight.testkit.ParallelAkkaSpec
import spotlight.protocol.GraphiteSerializationProtocol


/**
  * Created by rolfsd on 1/7/16.
  */
object OutlierDetectionWorkflowSpec {
  val sysId = new AtomicInteger()
}

class OutlierDetectionWorkflowSpec extends ParallelAkkaSpec with MockitoSugar with ScalaFutures {
  import OutlierDetectionBootstrap._

  override val trace = Trace[OutlierDetectionWorkflowSpec]

  class Fixture extends AkkaFixture { fixture =>
    import scalaz.Scalaz._

    val protocol = mock[GraphiteSerializationProtocol]
    val config2 = mock[Config]

    val rateLimiter = TestProbe()
    val publisher = TestProbe()
    val planRouter = TestProbe()
    val dbscan = TestProbe()
    val detector = TestProbe()

    val workflow = TestActorRef[OutlierDetectionBootstrap](
      new OutlierDetectionBootstrap with OneForOneStrategyFactory with ConfigurationProvider {
        override def sourceAddress: InetSocketAddress = new InetSocketAddress( "example.com", 2004 )
        override def maxFrameLength: Int = 1000
        override def protocol: GraphiteSerializationProtocol = fixture.protocol
        override def windowDuration: FiniteDuration = 2.minutes
//        override def makePlans: PlanConfigurationProvider.Creator = fixture.makePlans
        override def configuration: Config = fixture.config2
        override def detectionBudget: FiniteDuration = 3.seconds
        override def bufferSize: Int = 1000
        override def maxInDetectionCpuFactor: Double = 1.0

        override def makeAlgorithmRouter()(implicit context: ActorContext): ActorRef = planRouter.ref
        override def makeOutlierDetector(rateLimiter: ActorRef)(implicit context: ActorContext): ActorRef = detector.ref
        override def makeAlgorithmWorkers(router: ActorRef)(implicit context: ActorContext): Map[Symbol, ActorRef] = {
          Map( 'dbscan -> dbscan.ref )
        }

        override def makePlanDetectionRouter(
          detector: ActorRef,
          detectionBudget: FiniteDuration,
          bufferSize: Int,
          maxInDetectionCpuFactor: Double
        )(
          implicit ctx: ActorContext
        ): ActorRef = planRouter.ref
      }
    )

    val sender = TestProbe()

    def makeProps( actor: => Actor ): Props = Props( actor )
    def propMaker(result: Props ): MakeProps = (ActorRef) => {result }
  }

  def createAkkaFixture( test: OneArgTest ): Fixture = new Fixture

  val DONE = Tag( "done" )

  "OutlierDetectionBootstrap" should {
    "start" in  { f: Fixture =>
      import f._
      workflow.receive( WaitForStart, sender.ref )
      sender.expectMsg( 400.millis.dilated,  "start", Started )
    }

    "create outlier detector" in { f: Fixture =>
      import f._
      workflow.receive( GetOutlierDetector, sender.ref )
      sender.expectMsgPF( 400.millis.dilated, "detector" ) {
        case ChildStarted( actual ) => actual mustBe detector.ref
      }
    }

    "get child actors" taggedAs (WIP) in { f: Fixture =>
      import f._
      import system.dispatcher
      implicit val to = Timeout( 1.second.dilated )
      val actual = for {
        _ <- workflow ? WaitForStart
        ChildStarted( d ) <- ( workflow ? GetOutlierDetector ).mapTo[ChildStarted]
        ChildStarted( pr ) <- ( workflow ? GetOutlierPlanDetectionRouter ).mapTo[ChildStarted]
      } yield ( d, pr )

      whenReady( actual ) { a =>
        val (d, pr) = a
        d mustBe detector.ref
        pr mustBe planRouter.ref
      }
    }
  }
}
