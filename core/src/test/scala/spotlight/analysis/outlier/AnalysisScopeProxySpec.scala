package spotlight.analysis.outlier

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.NotUsed
import akka.actor.{ActorContext, ActorRef}
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestActor.AutoPilot
import akka.testkit._
import org.joda.{time => joda}
import demesne.{AggregateRootType, DomainModel}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import peds.commons.V
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.model.outlier._
import spotlight.model.outlier.OutlierPlan.Scope
import spotlight.model.timeseries._
import spotlight.testkit.ParallelAkkaSpec



/**
  * Created by rolfsd on 6/14/16.
  */
class AnalysisScopeProxySpec extends ParallelAkkaSpec with ScalaFutures with MockitoSugar {
  class Fixture extends AkkaFixture { fixture =>
    val algo = 'TestAlgorithm
    val router = TestProbe()
    val pid = OutlierPlan.nextId()
    val scope = OutlierPlan.Scope( "TestPlan", "TestTopic", pid )

    val testActorRef = TestActorRef( new AnalysisScopeProxy with TestProxyProvider )

    val model = mock[DomainModel]

    val plan = mock[OutlierPlan]
    when( plan.algorithms ).thenReturn( Set(algo) )
    when( plan.name ).thenReturn( scope.plan )
    when( plan.appliesTo ).thenReturn( appliesToAll )

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

    def addDetectorAutoPilot(
      detector: TestProbe,
      extractOutliers: OutlierDetectionMessage => Seq[DataPoint] = (m: OutlierDetectionMessage) => Seq.empty[DataPoint]
    ): Unit = {
      detector.setAutoPilot(
        new AutoPilot {
          override def run( dest: ActorRef, msg: Any ): AutoPilot = {
            log.info( "DETECTOR AUTOPILOT" )
            msg match {
              case m: OutlierDetectionMessage => {
                val results = DetectionResult(
                  Outliers.forSeries(
                    algorithms = Set( algo ),
                    plan = m.plan,
                    source = m.source,
                    outliers = extractOutliers( m ),
                    thresholdBoundaries = Map.empty[Symbol, Seq[ThresholdBoundary]]
                  ).toOption.get
                )

                log.info( "Detector AutoPilot\n + received: [{}]\n + sending:[{}]\n", m, results )
                dest ! results
                TestActor.KeepRunning
              }

              case m => {
                log.error( "Detector AutoPilot received unknown result: [{}]", m )
                TestActor.NoAutoPilot
              }
            }
          }
        }
      )
    }

    lazy val appliesToAll: OutlierPlan.AppliesTo = {
      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification(0, 0)
      val reduce: ReduceOutliers = new ReduceOutliers {
        import scalaz._
        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: OutlierPlan
        ): V[Outliers] = Validation.failureNel[Throwable, Outliers]( new IllegalStateException("should not use" ) ).disjunction
      }

      import scala.concurrent.duration._
      val grouping: Option[OutlierPlan.Grouping] = {
        val window = None
        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
      }

      OutlierPlan.default( "", 1.second, isQuorun, reduce, Set.empty[Symbol], grouping ).appliesTo
    }

  }

  override def makeAkkaFixture(): Fixture = new Fixture


  "AnalysisScopeProxy" should {
    "make topic workers" in { f: Fixture =>
      import f._
      val topic = Topic( "WORKER-TEST-TOPIC" )
      import scala.concurrent.ExecutionContext.Implicits.global
      val workers = testActorRef.underlyingActor.makeTopicWorkers( topic )
      whenReady( workers ) { actual =>
        actual.detector mustBe f.detector.ref
        actual.algorithms mustBe Set( f.algorithmActor.ref )
        actual.router mustBe f.router.ref
      }
    }

    "batch series" in { f: Fixture =>
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

      val flowUnderTest: Flow[TimeSeries, TimeSeries, NotUsed] = testActorRef.underlyingActor.batchSeries( testGrouping )

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

    "detect in flow" taggedAs WIP in { f: Fixture =>
      import f._
      import scala.concurrent.ExecutionContext.Implicits.global

      val now = joda.DateTime.now
      val topic = Topic( "test.topic" )
      val data = Seq(
        TimeSeries( "foo", Seq( DataPoint(now, 1.1), DataPoint(now.plusSeconds(1), 2.2), DataPoint(now.plusSeconds(2), 3.3) ) ),
        TimeSeries( "foo", Seq( DataPoint(now.plusSeconds(3), 4.4), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 6.6) ) ),
        TimeSeries( "foo", Seq( DataPoint(now.plusSeconds(6), 7.7), DataPoint(now.plusSeconds(7), 8.8), DataPoint(now.plusSeconds(8), 9.9) ) )
      )

      val findAndExtract: OutlierDetectionMessage => Seq[DataPoint] = (m: OutlierDetectionMessage) => {
        val i = data indexOf m.source
        data( i ).points.take( i )
      }

      addDetectorAutoPilot( detector, findAndExtract )
      val workers = Await.result( testActorRef.underlyingActor.makeTopicWorkers( topic ), 2.seconds )

      val flowUnderTest: Flow[TimeSeries, Outliers, NotUsed] = testActorRef.underlyingActor.detectionFlow( plan, workers )

      val (pub, sub) = {
        TestSource.probe[TimeSeries]
        .via( flowUnderTest )
        .toMat( TestSink.probe[Outliers] )( Keep.both )
        .run()
      }

      val ps = pub.expectSubscription()
      val ss = sub.expectSubscription()

      data foreach { ps.sendNext }

      def validateNext( ts: TimeSeries, i: Int ): Unit = {
        ss.request( 1 )
        val actual = sub.expectNext()
        if ( i == 0 ) actual mustBe a [NoOutliers]
        else  actual mustBe a [SeriesOutliers]

        actual match{
          case m @ NoOutliers( as, s, p, tbs ) if i == 0 => {
            as mustBe Set( algo )
            s mustBe data( i )
            p mustBe plan
          }

          case m @ SeriesOutliers( as, s, p, os, _ ) if i != 0 => {
            as mustBe Set( algo )
            s mustBe data(i)
            p mustBe plan
            os.size mustBe i
            os mustBe data(i).points.take(i)
          }
        }
      }

      data.indices foreach { i => validateNext(data(i), i ) }
    }

    "make a workable plan flow" in { f: Fixture => pending }

    "process via plan-specific workflow" in { f: Fixture => pending }
  }
}
