package lineup.stream

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import peds.akka.supervision.IsolatedLifeCycleSupervisor.{ChildStarted, Started, WaitForStart}

import scala.concurrent.duration._
import akka.actor.{ActorContext, Actor, ActorRef, Props}
import akka.pattern.ask
import akka.testkit._
import com.typesafe.config.Config
import lineup.model.outlier.OutlierPlan
import lineup.testkit.ParallelAkkaSpec
import org.scalatest.Tag
import org.scalatest.mock.MockitoSugar
import peds.akka.supervision.OneForOneStrategyFactory
import peds.commons.log.Trace

import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Try}


/**
  * Created by rolfsd on 1/7/16.
  */
object OutlierDetectionWorkflowSpec {
  val sysId = new AtomicInteger()
}

class OutlierDetectionWorkflowSpec extends ParallelAkkaSpec with MockitoSugar with ScalaFutures {
  import OutlierDetectionWorkflow._

  override val trace = Trace[OutlierDetectionWorkflowSpec]

  class Fixture extends AkkaFixture { fixture =>
    val protocol = mock[GraphiteSerializationProtocol]
    val makePlans: () => Try[Seq[OutlierPlan]] = () => { Success( Seq.empty[OutlierPlan] ) }
    val config = mock[Config]

    val rateLimiter = TestProbe()
    val publisher = TestProbe()
    val planRouter = TestProbe()
    val dbscan = TestProbe()
    val detector = TestProbe()

    val workflow = TestActorRef[OutlierDetectionWorkflow](
      new OutlierDetectionWorkflow with OneForOneStrategyFactory with ConfigurationProvider {
        def sourceAddress: InetSocketAddress = new InetSocketAddress( "example.com", 2004 )
        def maxFrameLength: Int = 1000
        def protocol: GraphiteSerializationProtocol = fixture.protocol
        def windowDuration: FiniteDuration = 2.minutes
        def graphiteAddress: Option[InetSocketAddress] = Some( new InetSocketAddress("example.com", 20400) )
        def makePlans: () => Try[Seq[OutlierPlan]] = fixture.makePlans
        def configuration: Config = fixture.config

        override def makePublishRateLimiter()(implicit context: ActorContext): ActorRef = rateLimiter.ref
        override def makePublisher(publisherProps: Props)(implicit context: ActorContext): ActorRef = publisher.ref
        override def makePlanRouter()(implicit context: ActorContext): ActorRef = planRouter.ref
        override def makeOutlierDetector(rateLimiter: ActorRef)(implicit context: ActorContext): ActorRef = detector.ref
        override def makeAlgorithmWorkers(router: ActorRef)(implicit context: ActorContext): Map[Symbol, ActorRef] = {
          Map( 'dbscan -> dbscan.ref )
        }
      }
    )

    val sender = TestProbe()

    def makeProps( actor: => Actor ): Props = Props( actor )
    def propMaker(result: Props ): MakeProps = (ActorRef) => {result }
  }

  def makeAkkaFixture(): Fixture = new Fixture

  val DONE = Tag( "done" )

  "OutlierDetectionWorkflow" should {
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

    "create publish rate limiter" in { f: Fixture =>
      import f._
      workflow.receive( GetPublishRateLimiter, sender.ref )
      sender.expectMsgPF( 400.millis.dilated, "limiter" ) {
        case ChildStarted( actual ) => actual mustBe rateLimiter.ref
      }
    }

    "create publisher" in { f: Fixture =>
      import f._
      workflow.receive( GetPublisher, sender.ref )
      sender.expectMsgPF( 400.millis.dilated, "publisher" ) {
        case ChildStarted( actual ) => actual mustBe publisher.ref
      }
    }

    "get child actors" in { f: Fixture =>
      import f._
      import system.dispatcher
      implicit val to = Timeout( 1.second.dilated )
      val actual = for {
        _ <- workflow ? WaitForStart
        ChildStarted( d ) <- ( workflow ? GetOutlierDetector ).mapTo[ChildStarted]
        ChildStarted( l ) <- ( workflow ? GetPublishRateLimiter ).mapTo[ChildStarted]
        ChildStarted( p ) <- ( workflow ? GetPublisher ).mapTo[ChildStarted]
      } yield (d, l, p)

      whenReady( actual) { a =>
        val (d, l, p) = a
        d mustBe detector.ref
        l mustBe rateLimiter.ref
        p mustBe publisher.ref
      }
    }
  }
}
