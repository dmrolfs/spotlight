//package spotlight.analysis
//
//import scala.concurrent.duration._
//import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, ActorSystem, Props}
//import akka.stream.{ActorMaterializer, Materializer}
//import akka.stream.scaladsl.Keep
//import akka.stream.testkit.scaladsl.{TestSink, TestSource}
//import akka.testkit.TestActor.AutoPilot
//import akka.testkit._
//import com.typesafe.config.{Config, ConfigFactory}
//import org.joda.{time => joda}
//import com.github.nscala_time.time.Imports.{richDateTime, richSDuration}
//import demesne.{AggregateRootType, DomainModel}
//import org.apache.commons.math3.random.RandomDataGenerator
//import org.scalatest.concurrent.ScalaFutures
//import org.scalatest.mockito.MockitoSugar
//import org.mockito.Mockito._
//import omnibus.commons.V
//import omnibus.akka.envelope._
//import spotlight.analysis.AnalysisPlanProtocol.AcceptTimeSeries
//import spotlight.analysis.OutlierDetection.DetectionResult
//import spotlight.model.outlier._
//import spotlight.model.timeseries._
//import spotlight.testkit.{ParallelAkkaSpec, TestCorrelatedSeries}
//
//
///**
//  * Created by rolfsd on 6/14/16.
//  */
//class AnalysisProxySpec extends ParallelAkkaSpec with ScalaFutures with MockitoSugar {
//
//  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
//    new Fixture( config, system, slug )
//  }
//
//  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AkkaFixture( _config, _system, _slug ) {
//    fixture =>
//
//    val algo = 'TestAlgorithm
//    val router = TestProbe()
//    router.setAutoPilot(
//      new TestActor.AutoPilot {
//        override def run( sender: ActorRef, msg: Any ): AutoPilot = {
//          val ctx = mock[ActorContext]
//          algorithmActor.ref.forwardEnvelope( msg )( ctx )
//          TestActor.KeepRunning
//        }
//      }
//    )
//
//    implicit val materializer: Materializer = ActorMaterializer()
//
//    val subscriber = TestProbe()
//
//    val pid = AnalysisPlan.AnalysisPlanIdentifying.safeNextId
//    val scope = AnalysisPlan.Scope( "TestPlan", "TestTopic" )
//    val plan = mock[AnalysisPlan]
//    when( plan.algorithms ).thenReturn( Set(algo) )
//    when( plan.name ).thenReturn( scope.plan )
//    when( plan.appliesTo ).thenReturn( appliesToAll )
//    when( plan.grouping ).thenReturn( None )
//
//    val testActorRef = TestActorRef( new TestProxy( fixture.plan ) { } )
//
//    val model = mock[DomainModel]
//
//    val rootType = mock[AggregateRootType]
//
//    val algorithmActor = TestProbe()
//    val detector = TestProbe()
//
//    val workId = WorkId()
//
//    class TestProxy( override var plan: AnalysisPlan = fixture.plan )
//    extends AnalysisProxy
//    with AnalysisProxy.Provider {
//      provider: Actor with ActorLogging =>
//
//      override def model: DomainModel = fixture.model
//      override def highWatermark: Int = 10
//      override def bufferSize: Int = 1000
//      override def makeRouter()( implicit context: ActorContext ): ActorRef = router.ref
//      override def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = detector.ref
//    }
//
//    def addDetectorAutoPilot(
//      detector: TestProbe,
//      extractOutliers: OutlierDetectionMessage => Seq[DataPoint] = (m: OutlierDetectionMessage) => m.source.points.take(1)
//    ): Unit = {
//      detector.setAutoPilot(
//        new AutoPilot {
//          override def run( dest: ActorRef, msg: Any ): AutoPilot = {
//            log.info( "DETECTOR AUTOPILOT" )
//            msg match {
//              case m: OutlierDetectionMessage => {
//                val results = DetectionResult(
//                  Outliers.forSeries(
//                    algorithms = Set( algo ),
//                    plan = m.plan,
//                    source = m.source,
//                    outliers = extractOutliers( m ),
//                    thresholdBoundaries = Map.empty[Symbol, Seq[ThresholdBoundary]]
//                  ).toOption.get,
//                  m.correlationIds
//                )
//
//                log.info( "Detector AutoPilot\n + received: [{}]\n + sending:[{}]\n", m, results )
//                subscriber.ref ! results
//                TestActor.KeepRunning
//              }
//
//              case m => {
//                log.error( "Detector AutoPilot received unknown result: [{}]", m )
//                TestActor.NoAutoPilot
//              }
//            }
//          }
//        }
//      )
//    }
//
//    lazy val appliesToAll: AnalysisPlan.AppliesTo = {
//      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification(0, 0)
//      val reduce: ReduceOutliers = new ReduceOutliers {
//        import scalaz._
//        override def apply(
//          results: OutlierAlgorithmResults,
//          source: TimeSeriesBase,
//          plan: AnalysisPlan
//        ): V[Outliers] = Validation.failureNel[Throwable, Outliers]( new IllegalStateException("should not use" ) ).disjunction
//      }
//
//      import scala.concurrent.duration._
//      val grouping: Option[AnalysisPlan.Grouping] = {
//        val window = None
//        window map { w => AnalysisPlan.Grouping( limit = 10000, w ) }
//      }
//
//      AnalysisPlan.default( "", 1.second, isQuorun, reduce, Set.empty[Symbol], grouping ).appliesTo
//    }
//
//    def makePlan( name: String, g: Option[AnalysisPlan.Grouping] ): AnalysisPlan = {
//      AnalysisPlan.default(
//        name = name,
//        algorithms = Set( algo ),
//        grouping = g,
//        timeout = 500.millis,
//        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = 1, triggerPoint = 1 ),
//        reduce = ReduceOutliers.byCorroborationPercentage(50),
//        planSpecification = ConfigFactory.parseString(
//          s"""
//          |algorithm-config.${algo.name}.seedEps: 5.0
//          |algorithm-config.${algo.name}.minDensityConnectedPoints: 3
//          """.stripMargin
//        )
//      )
//    }
//
//    def spike(
//      data: Seq[DataPoint],
//      topic: Topic = "test.series",
//      value: Double = 1000D
//    )(
//      position: Int = data.size - 1
//    ): TimeSeries = {
//      val (front, last) = data.sortBy{ _.timestamp.getMillis }.splitAt( position )
//      val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
//      TimeSeries( topic, spiked )
//    }
//
//    def makeDataPoints(
//      values: Seq[Double],
//      start: joda.DateTime = joda.DateTime.now,
//      period: FiniteDuration = 1.second,
//      wiggleFactor: (Double, Double) = (1.0, 1.0)
//    ): Seq[DataPoint] = {
//      val secs = start.getMillis / 1000L
//      val epochStart = new joda.DateTime( secs * 1000L )
//      val random = new RandomDataGenerator
//      def nextFactor: Double = {
//        if ( wiggleFactor._1 == wiggleFactor._2 ) wiggleFactor._1
//        else random.nextUniform( wiggleFactor._1, wiggleFactor._2 )
//      }
//
//      values.zipWithIndex map { vi =>
//        val (v, i) = vi
//        val adj = (i * nextFactor) * period
//        val ts = epochStart + adj.toJodaDuration
//        DataPoint( timestamp = ts, value = v )
//      }
//    }
//  }
//
//
//  "AnalysisProxy" should {
//    "make workers" in { f: Fixture =>
//      import f._
//      val actual = testActorRef.underlyingActor.workers
//      actual.detector mustBe f.detector.ref
//      actual.router mustBe f.router.ref
//    }
//
//    "batch series" in { f: Fixture =>
//      import f._
//      val testGrouping = AnalysisPlan.Grouping( limit = 4, window = 1.second )
//
//      val now = joda.DateTime.now
//
//      val data = Seq(
//        TimeSeries( "foo", Seq( DataPoint(now, 1.1), DataPoint(now.plusSeconds(1), 2.2), DataPoint(now.plusSeconds(2), 3.3) ) ),
//        TimeSeries( "bar", Seq( DataPoint(now, 9.9), DataPoint(now.plusSeconds(1), 8.8), DataPoint(now.plusSeconds(2), 7.7) ) ),
//        TimeSeries( "bar", Seq( DataPoint(now.plusSeconds(3), 6.6), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 4.4) ) ),
//        TimeSeries( "foo", Seq( DataPoint(now.plusSeconds(3), 4.4), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 6.6) ) ),
//        TimeSeries( "foo", Seq( DataPoint(now.plusSeconds(6), 7.7), DataPoint(now.plusSeconds(7), 8.8), DataPoint(now.plusSeconds(8), 9.9) ) ),
//        TimeSeries( "bar", Seq( DataPoint(now.plusSeconds(6), 3.3), DataPoint(now.plusSeconds(7), 2.2), DataPoint(now.plusSeconds(8), 1.1) ) )
//      )
//
//      val workIds = Seq.fill( data.size ){ Set( WorkId() ) }
//      val flowUnderTest = testActorRef.underlyingActor.batchSeries( testGrouping )
//
//      val (pub, sub) = {
//        TestSource.probe[CorrelatedSeries]
//        .via( flowUnderTest )
//        .toMat( TestSink.probe[CorrelatedSeries] )( Keep.both )
//        .run()
//      }
//      val ps = pub.expectSubscription()
//      val ss = sub.expectSubscription()
//
//      for {
//        tsCids <- data zip workIds
//        (ts, cids) = tsCids
//        m = AcceptTimeSeries( null, cids, ts )
//      } { ps sendNext m }
////      data.zip( workIds ) map { case (ts, cids) => AcceptTimeSeries} foreach { ps.sendNext }
//
//      ss.request( 2 )
//
//      val expectedData1: Map[String, Seq[DataPoint]] = Map(
//        "foo" -> Seq(
//                      DataPoint(now, 1.1), DataPoint(now.plusSeconds(1), 2.2), DataPoint(now.plusSeconds(2), 3.3),
//                      DataPoint(now.plusSeconds(3), 4.4), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 6.6)
//                    ),
//        "bar" -> Seq(
//                      DataPoint(now, 9.9), DataPoint(now.plusSeconds(1), 8.8), DataPoint(now.plusSeconds(2), 7.7),
//                      DataPoint(now.plusSeconds(3), 6.6), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 4.4)
//                    )
//      )
//
//
//      val expectedWorkIds1: Map[Topic, Set[WorkId]] = Map(
//        "foo".toTopic -> (workIds(0) ++ workIds(3)),
//        "bar".toTopic -> (workIds(1) ++ workIds(2))
//      )
//
//      Seq.fill( 2 ){ sub.expectNext() } foreach { actual =>
//        actual.correlationIds mustBe expectedWorkIds1( actual.data.topic )
//        actual.data.topic match {
//          case Topic(t) => (t, actual.data.points) mustBe (t, expectedData1(t))
//        }
//      }
//
//      ss.request( 2 )
//
//      val expectedData2: Map[String, Seq[DataPoint]] = Map(
//        "foo" -> Seq( DataPoint(now.plusSeconds(6), 7.7), DataPoint(now.plusSeconds(7), 8.8), DataPoint(now.plusSeconds(8), 9.9) ),
//        "bar" -> Seq( DataPoint(now.plusSeconds(6), 3.3), DataPoint(now.plusSeconds(7), 2.2), DataPoint(now.plusSeconds(8), 1.1) )
//      )
//
//      val expectedWorkIds2: Map[Topic, Set[WorkId]] = Map( "foo".toTopic -> workIds(4), "bar".toTopic -> workIds(5) )
//
//      Seq.fill( 2 ){ sub.expectNext() } foreach { actual =>
//        actual.correlationIds mustBe expectedWorkIds2( actual.data.topic )
//        actual.correlationIds.size mustBe 1
//        actual.data.topic match {
//          case Topic(t) => (t, actual.data.points) mustBe (t, expectedData2(t))
//        }
//      }
//    }
//
//    "detect in flow" in { f: Fixture =>
//      import f._
//      import scala.concurrent.ExecutionContext.Implicits.global
//
//      val now = joda.DateTime.now
//      val topic = Topic( "test.topic" )
//      val data = Seq(
//        TimeSeries( "foo", Seq( DataPoint(now, 1.1), DataPoint(now.plusSeconds(1), 2.2), DataPoint(now.plusSeconds(2), 3.3) ) ),
//        TimeSeries( "foo", Seq( DataPoint(now.plusSeconds(3), 4.4), DataPoint(now.plusSeconds(4), 5.5), DataPoint(now.plusSeconds(5), 6.6) ) ),
//        TimeSeries( "foo", Seq( DataPoint(now.plusSeconds(6), 7.7), DataPoint(now.plusSeconds(7), 8.8), DataPoint(now.plusSeconds(8), 9.9) ) )
//      )
//
//      val workIds = Seq.fill( data.size ){ Set( WorkId() ) }
//
//      val findAndExtract: OutlierDetectionMessage => Seq[DataPoint] = (m: OutlierDetectionMessage) => {
//        val i = data indexOf m.source
//        data( i ).points.take( i )
//      }
//
//      addDetectorAutoPilot( detector, findAndExtract )
//      val workers = testActorRef.underlyingActor.workers
//      val sinkUnderTest = testActorRef.underlyingActor.detectionFlow( plan, subscriber.ref, workers )
//      val probe = {
//        TestSource.probe[AcceptTimeSeries]
//        .toMat( sinkUnderTest )( Keep.left )
//        .run()
//      }
//
////      data.zip(workIds) foreach { probe.sendNext }
//
//      def validateNext( ts: TimeSeries, wids: Set[WorkId], i: Int ): Unit = {
//        log.info( "validateNext: i:[{}] wids:[{}] ts:[{}]", i, wids.mkString(", "), ts )
//        probe.sendNext( AcceptTimeSeries(null, wids, ts) )
//        detector.expectMsgType[OutlierDetectionMessage]
//        subscriber.expectMsgPF( hint = s"detect-${i}" ) {
//          case DetectionResult(NoOutliers(as, s, p, tbs), wids) => {
//            log.info( "TEST: NO-OUTLIERS[{}] all-wids:[{}]", i.toString, workIds.mkString(", ") )
//            log.info( "TEST: wids=[{}] workIds({})=[{}] match=[{}]", wids.mkString(", "), i, workIds(i).mkString(", "), wids == workIds(i))
//            wids mustBe workIds( i )
//            i mustBe 0
//            as mustBe Set( algo )
//            s mustBe data( i )
//            p mustBe plan
//          }
//
//          case DetectionResult(SeriesOutliers( as, s, p, os, _ ), wids) => {
//            log.info( "TEST: SERIES-OUTLIERS[{}] all-wids:[{}]", i.toString, workIds.mkString(", ") )
//            wids mustBe workIds( i )
//            i must not be (0)
//            as mustBe Set( algo )
//            s mustBe data(i)
//            p mustBe plan
//            os.size mustBe i
//            os mustBe data(i).points.take(i)
//          }
//        }
//      }
//
//      data.zip(workIds).zipWithIndex foreach { case ((ts, wids), i) =>
//        log.debug( "TEST: validating workIds:[{}]", wids )
//        validateNext( ts, wids, i )
//      }
//    }
//
//    "make functioning plan stream" taggedAs WIP in { f: Fixture =>
//      import f._
//      val (actualIngress, actualGraph) = testActorRef.underlyingActor.makeGraph( subscriber.ref )
//      log.info( "actualIngress = [{}]", actualIngress )
//      log.info( "actualGraph = [{}]", actualGraph )
//      addDetectorAutoPilot( detector )
//      actualIngress ! TestCorrelatedSeries( TimeSeries("dummy", Seq() ) )
//      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
//      subscriber.expectMsgClass( classOf[DetectionResult] )
//    }
//
//    "make resuable plan stream ingress once" in { f: Fixture =>
//      import f._
//      addDetectorAutoPilot( detector )
//      val a1 = testActorRef.underlyingActor.graphIngressFor( subscriber.ref )
//      log.info( "first actual = [{}]", a1 )
//      val msg1 = TimeSeries( "dummy", Seq() )
//      a1 ! TestCorrelatedSeries(msg1)
//      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
//      subscriber.expectMsgClass( classOf[DetectionResult] )
//
//      val a2 = testActorRef.underlyingActor.graphIngressFor( subscriber.ref )
//      log.info( "second actual = [{}]", a2 )
//      val now = joda.DateTime.now
//      val msg2 = TimeSeries( "dummy", Seq(DataPoint(now, 3.14159), DataPoint(now.plusSeconds(1), 2.191919)) )
//      a1 mustBe a2
//      a2 ! TestCorrelatedSeries(msg2)
//      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
//      subscriber.expectMsgClass( classOf[DetectionResult] )
//    }
//
//    "make plan-dependent streams" in { f: Fixture =>
//      import f._
//
//      val now = joda.DateTime.now
//
//      val p1 = makePlan( "p1", Some( AnalysisPlan.Grouping(10, 300.seconds) ) )
//      val p2 = makePlan( "p2", None )
//
//      //      val m1 = OutlierDetectionMessage( TimeSeries("dummy", Seq()), p1 ).toOption.get
//      //      val m2 = OutlierDetectionMessage( TimeSeries("dummy", Seq()), p2 ).toOption.get
//      val m1 = TimeSeries( "dummy", Seq() )
//      val m2 = TimeSeries( "dummy", Seq(DataPoint(now, 3.14159), DataPoint(now.plusSeconds(1), 2.191919191)) )
//
//      addDetectorAutoPilot( detector )
//
//      val proxy1 = TestActorRef( new TestProxy(p1) )
//      val a1 = proxy1.underlyingActor.graphIngressFor( subscriber.ref )
//      log.info( "first actual ingress = [{}]", a1 )
//      log.info( "first actual graph = [{}]", proxy1.underlyingActor.graphs( subscriber.ref ) )
//      a1 ! TestCorrelatedSeries(m1)
//      detector.expectNoMsg( 1.second.dilated ) // due to empty timeseries
//      subscriber.expectNoMsg( 1.second.dilated ) //dur to empty timeseries
//
//      val proxy2 = TestActorRef( new TestProxy(p2) )
//      val a2 = proxy2.underlyingActor.graphIngressFor( subscriber.ref )
//      log.info( "second actual ingress = [{}]", a2 )
//      log.info( "second actual graph = [{}]", proxy2.underlyingActor.graphs( subscriber.ref ) )
//      a1 must not be a2
//      a2 ! TestCorrelatedSeries(m2)
//      detector.expectMsgClass( 1.second.dilated, classOf[DetectOutliersInSeries] )
//      subscriber.expectMsgClass( 1.second.dilated, classOf[DetectionResult] )
//    }
//
//    "router receive uses plan-dependent streams" in { f: Fixture =>
//      import f._
//
//      // needing grouping time window to be safely less than expectMsg max, since demand isn't propagated until grouping window
//      // expires
//      val p1 = makePlan( "p1", Some( AnalysisPlan.Grouping(10, 300.millis) ) )
//      val p2 = makePlan( "p2", None )
//
//      addDetectorAutoPilot( detector )
//
//      val proxy1 = TestActorRef( new TestProxy(p1) )
//      val points = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
//      val ts = spike( data = points, topic = "dummy" )( points.size - 1 )
//
//      val m1p = TestCorrelatedSeries(ts, scope = Some(AnalysisPlan.Scope(p1, ts.topic)))
//      val m2s = TestCorrelatedSeries(ts, scope = Some(AnalysisPlan.Scope(p2, "dummy")))
//
//      proxy1.receive( m1p, subscriber.ref )
//      detector.expectMsgClass( 5.seconds.dilated, classOf[DetectOutliersInSeries] )
//      subscriber.expectMsgClass( 5.seconds.dilated, classOf[DetectionResult] )
//
//      proxy1.receive( m2s, subscriber.ref )
//      detector.expectNoMsg( 1.second.dilated )
//      subscriber.expectNoMsg( 1.second.dilated )
//
//      val proxy2 = TestActorRef( new TestProxy(p2) )
//
//      proxy2.receive( m1p, subscriber.ref )
//      detector.expectNoMsg( 1.second.dilated )
//      subscriber.expectNoMsg( 1.second.dilated )
//
//      proxy2.receive( m2s, subscriber.ref )
//      detector.expectMsgClass( 1.second.dilated, classOf[DetectOutliersInSeries] )
//      subscriber.expectMsgClass( 1.second.dilated, classOf[DetectionResult] )
//    }
//
//
//    "messaging uses plan-dependent streams" in { f: Fixture =>
//      import f._
//
//      // needing grouping time window to be safely less than expectMsg max, since demand isn't propagated until grouping window
//      // expires
//      val p1 = makePlan( "p1", Some( AnalysisPlan.Grouping(10, 300.millis) ) )
//      val p2 = makePlan( "p2", None )
//
//      addDetectorAutoPilot( detector )
//
//      val proxyProps = Props( new TestProxy(p1) )
//
//      val proxy = system.actorOf( proxyProps, "test-proxy" )
//
//      val points = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
//      val ts = spike( data = points, topic = "dummy" )( points.size - 1 )
//
//      val m1p = TestCorrelatedSeries(ts, scope = Some(AnalysisPlan.Scope(p1, ts.topic)))
//      val m2s = TestCorrelatedSeries(ts, scope = Some(AnalysisPlan.Scope(p2, "dummy")))
//
//      proxy.tell( m1p, subscriber.ref )
//      detector.expectMsgClass( 5.seconds.dilated, classOf[DetectOutliersInSeries] )
//      subscriber.expectMsgPF( 5.seconds.dilated, "results" ) {
//        case DetectionResult( m, _ ) => m mustBe a [SeriesOutliers]
//        case m => m mustBe a [SeriesOutliers]
//      }
//
//      proxy.tell( m2s, subscriber.ref )
//      detector.expectNoMsg( 1.second.dilated )
//      subscriber.expectNoMsg( 1.second.dilated )
//    }
//  }
//}
