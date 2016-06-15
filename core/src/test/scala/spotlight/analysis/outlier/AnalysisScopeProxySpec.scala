package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.NotUsed
import akka.actor.{ActorContext, ActorRef}
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit._
import org.joda.{time => joda}
import demesne.{AggregateRootType, DomainModel}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import spotlight.model.outlier.OutlierPlan
import spotlight.model.outlier.OutlierPlan.Scope
import spotlight.model.timeseries._
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

    "batch series" taggedAs WIP in { f: Fixture =>
      import f._
      val testGrouping = OutlierPlan.Grouping( limit = 4, window = 1.second )

      val now = joda.DateTime.now

      val data = Seq(
        TimeSeries( "foo", Seq( DataPoint(now, 1.1), DataPoint(now.plusSeconds(1), 2.2), DataPoint(now.plusSeconds(2), 3.3) ) ),
        TimeSeries( "bar", Seq( DataPoint(now, 9.9), DataPoint(now.plusSeconds(1), 8.8), DataPoint(now.plusSeconds(2), 7.7) ) ),
        TimeSeries( "bar", Seq( DataPoint(now.plusSeconds(3), 6.6), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 4.4) ) ),
        TimeSeries( "foo", Seq( DataPoint(now.plusSeconds(3), 4.4), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 6.6) ) ),
        TimeSeries( "foo", Seq( DataPoint(now.plusSeconds(6), 7.7), DataPoint(now.plusSeconds(7), 8.8), DataPoint(now.plusSeconds(8), 9.9) ) ),
        TimeSeries( "bar", Seq( DataPoint(now.plusSeconds(6), 3.3), DataPoint(now.plusSeconds(7), 2.2), DataPoint(now.plusSeconds(8), 1.1) ) )
      )

      val flowUnderTest: Flow[TimeSeries, TimeSeries, NotUsed] = {
        TestActorRef( new AnalysisScopeProxy with TestProxyProvider ).underlyingActor.batchSeries( testGrouping )
      }
      val (pub, sub) = {
        TestSource.probe[TimeSeries]
        .via( flowUnderTest )
        .toMat( TestSink.probe[TimeSeries] )( Keep.both )
        .run()
      }
      val ps = pub.expectSubscription()
      val ss = sub.expectSubscription()

      data foreach { ps.sendNext }

      ss.request( 2 )

      val e1: Map[String, Seq[DataPoint]] = Map(
        "foo" -> Seq(
                      DataPoint(now, 1.1), DataPoint(now.plusSeconds(1), 2.2), DataPoint(now.plusSeconds(2), 3.3),
                      DataPoint(now.plusSeconds(3), 4.4), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 6.6)
                    ),
        "bar" -> Seq(
                      DataPoint(now, 9.9), DataPoint(now.plusSeconds(1), 8.8), DataPoint(now.plusSeconds(2), 7.7),
                      DataPoint(now.plusSeconds(3), 6.6), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 4.4)
                    )
      )

      Seq.fill( 2 ){ sub.expectNext() } foreach { actual =>
        actual.topic match {
          case Topic(t) => (t, actual.points) mustBe (t, e1(t))
        }
      }

      ss.request( 2 )

      val e2: Map[String, Seq[DataPoint]] = Map(
        "foo" -> Seq( DataPoint(now.plusSeconds(6), 7.7), DataPoint(now.plusSeconds(7), 8.8), DataPoint(now.plusSeconds(8), 9.9) ),
        "bar" -> Seq( DataPoint(now.plusSeconds(6), 3.3), DataPoint(now.plusSeconds(7), 2.2), DataPoint(now.plusSeconds(8), 1.1) )
      )
      Seq.fill( 2 ){ sub.expectNext() } foreach { actual =>
        actual.topic match {
          case Topic(t) => (t, actual.points) mustBe (t, e2(t))
        }
      }
    }

    "detect in flow" in { f: Fixture => pending }

    "make a workable plan flow" in { f: Fixture => pending }

    "process via plan-specific workflow" in { f: Fixture => pending }
  }
}
