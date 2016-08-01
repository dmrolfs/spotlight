package spotlight.analysis.outlier

import scala.concurrent.duration._
import akka.NotUsed
import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestActor.AutoPilot
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.joda.{time => joda}
import com.github.nscala_time.time.Imports.{richDateTime, richSDuration}
import demesne.{AggregateRootType, DomainModel}
import org.apache.commons.math3.random.RandomDataGenerator
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import peds.commons.V
import peds.akka.envelope._
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
    router.setAutoPilot(
      new TestActor.AutoPilot {
        override def run( sender: ActorRef, msg: Any ): AutoPilot = {
          val ctx = mock[ActorContext]
          algorithmActor.ref.forwardEnvelope( msg )( ctx )
          TestActor.KeepRunning
        }
      }
    )

    val subscriber = TestProbe()

    val pid = OutlierPlan.outlierPlanIdentifying.safeNextId
    val scope = OutlierPlan.Scope( "TestPlan", "TestTopic" )
    val plan = mock[OutlierPlan]
    when( plan.algorithms ).thenReturn( Set(algo) )
    when( plan.name ).thenReturn( scope.plan )
    when( plan.appliesTo ).thenReturn( appliesToAll )
    when( plan.grouping ).thenReturn( None )

    val testActorRef = TestActorRef( new AnalysisScopeProxy with TestProxyProvider )

    val model = mock[DomainModel]

    val rootType = mock[AggregateRootType]

    val algorithmActor = TestProbe()
    val detector = TestProbe()

    trait TestProxyProvider extends AnalysisScopeProxy.Provider { provider: Actor with ActorLogging =>
      override def scope: Scope = fixture.scope
      override def plan: OutlierPlan = fixture.plan
      override def model: DomainModel = fixture.model
      override def highWatermark: Int = 10
      override def bufferSize: Int = 1000
      override def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType] = Some( rootType )

      override def makeRouter()( implicit context: ActorContext ): ActorRef = router.ref
      override def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = detector.ref
    }

    def addDetectorAutoPilot(
      detector: TestProbe,
      extractOutliers: OutlierDetectionMessage => Seq[DataPoint] = (m: OutlierDetectionMessage) => m.source.points.take(1)
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
                subscriber.ref ! results
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

    def makePlan( name: String, g: Option[OutlierPlan.Grouping] ): OutlierPlan = {
      OutlierPlan.default(
        name = name,
        algorithms = Set( algo ),
        grouping = g,
        timeout = 500.millis,
        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = 1, triggerPoint = 1 ),
        reduce = ReduceOutliers.byCorroborationPercentage(50),
        planSpecification = ConfigFactory.parseString(
          s"""
          |algorithm-config.${algo.name}.seedEps: 5.0
          |algorithm-config.${algo.name}.minDensityConnectedPoints: 3
          """.stripMargin
        )
      )
    }

    def spike(
      data: Seq[DataPoint],
      topic: Topic = "test.series",
      value: Double = 1000D
    )(
      position: Int = data.size - 1
    ): TimeSeries = {
      val (front, last) = data.sortBy{ _.timestamp.getMillis }.splitAt( position )
      val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
      TimeSeries( topic, spiked )
    }

    def makeDataPoints(
      values: Seq[Double],
      start: joda.DateTime = joda.DateTime.now,
      period: FiniteDuration = 1.second,
      wiggleFactor: (Double, Double) = (1.0, 1.0)
    ): Seq[DataPoint] = {
      val secs = start.getMillis / 1000L
      val epochStart = new joda.DateTime( secs * 1000L )
      val random = new RandomDataGenerator
      def nextFactor: Double = {
        if ( wiggleFactor._1 == wiggleFactor._2 ) wiggleFactor._1
        else random.nextUniform( wiggleFactor._1, wiggleFactor._2 )
      }

      values.zipWithIndex map { vi =>
        val (v, i) = vi
        val adj = (i * nextFactor) * period
        val ts = epochStart + adj.toJodaDuration
        DataPoint( timestamp = ts, value = v )
      }
    }
  }


  override def makeAkkaFixture(): Fixture = new Fixture


  "AnalysisScopeProxy" should {
    "make topic workers" in { f: Fixture =>
      import f._
      val topic = Topic( "WORKER-TEST-TOPIC" )
      val actual = testActorRef.underlyingActor.makeTopicWorkers( topic )
      actual.detector mustBe f.detector.ref
      actual.router mustBe f.router.ref
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
      val workers = testActorRef.underlyingActor.makeTopicWorkers( topic )

      val sinkUnderTest: Sink[TimeSeries, NotUsed] = testActorRef.underlyingActor.detectionFlow( plan, subscriber.ref, workers )

      val probe = {
        TestSource.probe[TimeSeries]
        .toMat( sinkUnderTest )( Keep.left )
        .run()
      }

      data foreach { probe.sendNext }

      def validateNext( ts: TimeSeries, i: Int ): Unit = {
        probe.sendNext( ts )
        detector.expectMsgType[OutlierDetectionMessage]
        subscriber.expectMsgPF( hint = s"detect-${i}" ) {
          case DetectionResult(NoOutliers(as, s, p, tbs)) => {
            log.info( "TEST: NO-OUTLIERS" )
            i mustBe 0
            as mustBe Set( algo )
            s mustBe data( i )
            p mustBe plan
          }

          case DetectionResult(SeriesOutliers( as, s, p, os, _ )) => {
            log.info( "TEST: SERIES-OUTLIERS" )
            i must not be (0)
            as mustBe Set( algo )
            s mustBe data(i)
            p mustBe plan
            os.size mustBe i
            os mustBe data(i).points.take(i)
          }
        }
      }

      data.zipWithIndex foreach { case (ts, i) => validateNext( ts, i ) }
    }

    "make functioning plan stream" in { f: Fixture =>
      import f._
      val actual = testActorRef.underlyingActor.makePlanStream( subscriber.ref )
      log.info( "actual = [{}]", actual )
      addDetectorAutoPilot( detector )
      actual.ingressRef ! TimeSeries( "dummy", Seq() )
      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
      subscriber.expectMsgClass( classOf[DetectionResult] )
    }

    "make resuable plan stream ingress once" in { f: Fixture =>
      import f._
      addDetectorAutoPilot( detector )
      val a1 = testActorRef.underlyingActor.streamIngressFor( subscriber.ref )
      log.info( "first actual = [{}]", a1 )
      val msg1 = TimeSeries( "dummy", Seq() )
      a1 ! msg1
      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
      subscriber.expectMsgClass( classOf[DetectionResult] )

      val a2 = testActorRef.underlyingActor.streamIngressFor( subscriber.ref )
      log.info( "second actual = [{}]", a2 )
      val now = joda.DateTime.now
      val msg2 = TimeSeries( "dummy", Seq(DataPoint(now, 3.14159), DataPoint(now.plusSeconds(1), 2.191919)) )
      a1 mustBe a2
      a2 ! msg2
      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
      subscriber.expectMsgClass( classOf[DetectionResult] )
    }

    "make plan-dependent streams" in { f: Fixture =>
      import f._

      val now = joda.DateTime.now

      val p1 = makePlan( "p1", Some( OutlierPlan.Grouping(10, 300.seconds) ) )
      val p2 = makePlan( "p2", None )

      //      val m1 = OutlierDetectionMessage( TimeSeries("dummy", Seq()), p1 ).toOption.get
      //      val m2 = OutlierDetectionMessage( TimeSeries("dummy", Seq()), p2 ).toOption.get
      val m1 = TimeSeries( "dummy", Seq() )
      val m2 = TimeSeries( "dummy", Seq(DataPoint(now, 3.14159), DataPoint(now.plusSeconds(1), 2.191919191)) )

      addDetectorAutoPilot( detector )

      val proxy1 = TestActorRef(
        new AnalysisScopeProxy with TestProxyProvider {
          override def plan: OutlierPlan = p1
          override def scope: Scope = OutlierPlan.Scope( p1, "dummy" )
          override def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = detector.ref
        }
      )


      val proxy2 = TestActorRef(
        new AnalysisScopeProxy with TestProxyProvider {
          override def plan: OutlierPlan = p2
          override def scope: Scope = OutlierPlan.Scope( p2, "dummy" )
          override def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = detector.ref
        }
      )

      val a1 = proxy1.underlyingActor.streamIngressFor( subscriber.ref )
      log.info( "first actual ingress = [{}]", a1 )
      log.info( "first actual graph = [{}]", proxy1.underlyingActor.streams( subscriber.ref ) )
      a1 ! m1
      detector.expectNoMsg( 1.second.dilated ) // due to empty timeseries
      subscriber.expectNoMsg( 1.second.dilated ) //dur to empty timeseries

      val a2 = proxy2.underlyingActor.streamIngressFor( subscriber.ref )
      log.info( "second actual ingress = [{}]", a2 )
      log.info( "second actual graph = [{}]", proxy2.underlyingActor.streams( subscriber.ref ) )
      a1 must not be a2
      a2 ! m2
      detector.expectMsgClass( 1.second.dilated, classOf[DetectOutliersInSeries] )
      subscriber.expectMsgClass( 1.second.dilated, classOf[DetectionResult] )
    }

    "router receive uses plan-dependent streams" in { f: Fixture =>
      import f._

      // needing grouping time window to be safely less than expectMsg max, since demand isn't propagated until grouping window
      // expires
      val p1 = makePlan( "p1", Some( OutlierPlan.Grouping(10, 300.millis) ) )
      val p2 = makePlan( "p2", None )

      addDetectorAutoPilot( detector )

      val proxy1 = TestActorRef(
        new AnalysisScopeProxy with TestProxyProvider {
          override def plan: OutlierPlan = p1
          override def scope: Scope = OutlierPlan.Scope( p1, "dummy" )
          override def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = detector.ref
        }
      )


      val points = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
      val ts = spike( data = points, topic = "dummy" )( points.size - 1 )

      val m1p = (ts, p1)
      val m2s = (ts, OutlierPlan.Scope(p2, "dummy"))

      proxy1.receive( m1p, subscriber.ref )
      detector.expectMsgClass( 5.seconds.dilated, classOf[DetectOutliersInSeries] )
      subscriber.expectMsgClass( 5.seconds.dilated, classOf[DetectionResult] )

      proxy1.receive( m2s, subscriber.ref )
      detector.expectNoMsg( 1.second.dilated )
      subscriber.expectNoMsg( 1.second.dilated )

      val proxy2 = TestActorRef(
        new AnalysisScopeProxy with TestProxyProvider {
          override def plan: OutlierPlan = p2
          override def scope: Scope = OutlierPlan.Scope( p2, "dummy" )
          override def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = detector.ref
        }
      )

      proxy2.receive( m1p, subscriber.ref )
      detector.expectNoMsg( 1.second.dilated )
      subscriber.expectNoMsg( 1.second.dilated )

      proxy2.receive( m2s, subscriber.ref )
      detector.expectMsgClass( 1.second.dilated, classOf[DetectOutliersInSeries] )
      subscriber.expectMsgClass( 1.second.dilated, classOf[DetectionResult] )
    }


    "messaging uses plan-dependent streams" in { f: Fixture =>
      import f._

      // needing grouping time window to be safely less than expectMsg max, since demand isn't propagated until grouping window
      // expires
      val p1 = makePlan( "p1", Some( OutlierPlan.Grouping(10, 300.millis) ) )
      val p2 = makePlan( "p2", None )

      addDetectorAutoPilot( detector )

      val proxyProps = Props(
        new AnalysisScopeProxy with TestProxyProvider {
          override def plan: OutlierPlan = p1
          override def scope: Scope = OutlierPlan.Scope( p1, "dummy" )
          override def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = detector.ref
        }
      )

      val proxy = system.actorOf( proxyProps, "test-proxy" )

      val points = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
      val ts = spike( data = points, topic = "dummy" )( points.size - 1 )

      val m1p = (ts, p1)
      val m2s = (ts, OutlierPlan.Scope(p2, "dummy"))

      proxy.tell( m1p, subscriber.ref )
      detector.expectMsgClass( 5.seconds.dilated, classOf[DetectOutliersInSeries] )
      subscriber.expectMsgPF( 5.seconds.dilated, "results" ) {
        case DetectionResult( m ) => m mustBe a [SeriesOutliers]
        case m => m mustBe a [SeriesOutliers]
      }

      proxy.tell( m2s, subscriber.ref )
      detector.expectNoMsg( 1.second.dilated )
      subscriber.expectNoMsg( 1.second.dilated )
    }
  }
}
