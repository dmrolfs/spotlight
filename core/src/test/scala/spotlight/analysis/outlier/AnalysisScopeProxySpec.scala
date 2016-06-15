package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
import akka.actor.{ActorContext, ActorRef}
import akka.testkit._
import demesne.{AggregateRootType, DomainModel}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import spotlight.model.outlier.OutlierPlan
import spotlight.model.outlier.OutlierPlan.Scope
import spotlight.model.timeseries.Topic
import spotlight.testkit.ParallelAkkaSpec



/**
  * Created by rolfsd on 6/14/16.
  */
class AnalysisScopeProxySpec extends ParallelAkkaSpec with ScalaFutures with MockitoSugar {
  class Fixture extends AkkaFixture { fixture =>
    val router = TestProbe()
    val pid = OutlierPlan.nextId()
    val scope = OutlierPlan.Scope( "TestPlan", "TestTopic", pid )

    val model = mock[DomainModel]

    val plan = mock[OutlierPlan]
    when( plan.name ).thenReturn( scope.plan )

    val rootType = mock[AggregateRootType]

    val algorithmActor = TestProbe()
    val detector = TestProbe()

    trait TestProxyProvider extends AnalysisScopeProxy.Provider {
      override def scope: Scope = fixture.scope
      override def plan: OutlierPlan = fixture.plan
      override def model: DomainModel = fixture.model
      override def highWatermark: Int = 10
      override def bufferSize: Int = 1000
      override def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType] = Some( rootType )

      override def makeRouter()( implicit context: ActorContext ): ActorRef = router.ref

      override def makeAlgorithms(
        routerRef: ActorRef
      )(
        implicit context: ActorContext,
        ec: ExecutionContext
      ): Future[Set[ActorRef]] = {
        Future successful Set( algorithmActor.ref )
      }

      override def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = detector.ref
    }
  }

  override def makeAkkaFixture(): Fixture = new Fixture


  "AnalysisScopeProxy" should {
    "make topic workers" in { f: Fixture =>
      import f._
      val topic = Topic( "WORKER-TEST-TOPIC" )
      val proxy = TestActorRef( new AnalysisScopeProxy with TestProxyProvider )
      import scala.concurrent.ExecutionContext.Implicits.global
      val workers = proxy.underlyingActor.makeTopicWorkers( topic )
      whenReady( workers ) { actual =>
        actual.detector mustBe f.detector.ref
        actual.algorithms mustBe Set( f.algorithmActor.ref )
        actual.router mustBe f.router.ref
      }
    }
  }
}
