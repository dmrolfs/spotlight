//package spotlight.analysis.outlier
//
//import java.util.concurrent.atomic.AtomicInteger
//import scala.concurrent.{Await, ExecutionContext}
//import scala.concurrent.duration._
//import akka.NotUsed
//import akka.actor.{ActorRef, Props}
//import akka.stream.scaladsl._
//import akka.stream.testkit.scaladsl.{TestSink, TestSource}
//import akka.testkit.TestActor.AutoPilot
//import akka.testkit._
//import akka.util.Timeout
//import com.typesafe.config.ConfigFactory
//import org.scalatest.Tag
//import org.apache.commons.math3.random.RandomDataGenerator
//import org.joda.{time => joda}
//import com.github.nscala_time.time.Imports.{richDateTime, richSDuration}
//import demesne.{AggregateRootType, BoundedContext, DomainModel}
//import peds.commons.log.Trace
//import spotlight.analysis.outlier.OutlierDetection.DetectionResult
//import spotlight.analysis.outlier.algorithm.skyline.SimpleMovingAverageAnalyzer
//import spotlight.testkit.ParallelAkkaSpec
//import spotlight.model.outlier._
//import spotlight.model.timeseries.{DataPoint, ThresholdBoundary, TimeSeries}
//import spotlight.stream.OutlierScoringModel
//
//
///**
// * Created by rolfsd on 10/28/15.
// */
//class OutlierPlanDetectionRouterSpec extends ParallelAkkaSpec {
//  import OutlierPlanDetectionRouterSpec._
//
//  class Fixture extends AkkaFixture { fixture =>
//    def status[T]( label: String ): Flow[T, T, NotUsed] = {
//      Flow[T] map { e => logger info s"\n$label:${e.toString}"; e }
//    }
//
//    val algo = SimpleMovingAverageAnalyzer.Algorithm
//
////    def rootTypes: Set[AggregateRootType] = Set( AnalysisPlanModule.module.rootType )
////    lazy val boundedContext: BoundedContext = trace.block( "boundedContext" ) {
////      import ExecutionContext.Implicits.global
////      implicit val actorTimeout = Timeout( 5.seconds.dilated )
////      val bc = {
////        for {
////          made <- BoundedContext.make( Symbol(s"Parallel-${fixtureId}"), config, rootTypes )
////          started <- made.start()
////        } yield started
////      }
////      Await.result( bc, 5.seconds )
////    }
////
////    implicit lazy val model: DomainModel = trace.block( "model" ) { Await.result( boundedContext.futureModel, 5.seconds ) }
//
//    val detector = TestProbe( "detector-probe" )
//    detector.setAutoPilot(
//      new AutoPilot {
//        override def run( dest: ActorRef, msg: Any ): AutoPilot = {
//          log.info( "DETECTOR AUTOPILOT" )
//          msg match {
//            case m: OutlierDetectionMessage => {
//              val results = DetectionResult(
//                Outliers.forSeries(
//                  algorithms = Set( algo ),
//                  plan = m.plan,
//                  source = m.source,
//                  outliers = Seq.empty[DataPoint],
//                  thresholdBoundaries = Map.empty[Symbol, Seq[ThresholdBoundary]]
//                ).toOption.get
//              )
//
//              log.info( "Detector AutoPilot\n + received: [{}]\n + sending:[{}]\n", m, results )
//              dest ! results
//              TestActor.KeepRunning
//            }
//
//            case m => {
//              log.error( "Detector AutoPilot received unknown result: [{}]", m )
//              TestActor.NoAutoPilot
//            }
//          }
//        }
//      }
//    )
//
//    val subscriber = TestProbe( "subscriber-probe" )
//
//    val grouping: Option[OutlierPlan.Grouping] = {
//      val window = None
//      window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
//    }
//
//    val plan = makePlan( "MyFixturePlan", grouping )
//
//    val plans = Set( plan )
//
//    def makePlanRouter( ps: Set[OutlierPlan] = fixture.plans ): TestActorRef[OutlierPlanDetectionRouter] = {
//      TestActorRef[OutlierPlanDetectionRouter](
//        Props(
//          new OutlierPlanDetectionRouter with OutlierPlanDetectionRouter.ConfigurationProvider {
//            override def detector: ActorRef = fixture.detector.ref
//            override def detectionBudget: FiniteDuration = 500.millis
//            override def bufferSize: Int = 100
//            override def maxInDetectionCpuFactor: Double = 1.0
////            override def plans: Set[OutlierPlan] = ps
//          }
//        ),
//        "FixturePlanRouter"
//      )
//    }
//
//    def makePlan( name: String, g: Option[OutlierPlan.Grouping] ): OutlierPlan = {
//      OutlierPlan.default(
//        name = name,
//        algorithms = Set( algo ),
//        grouping = g,
//        timeout = 500.millis,
//        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = 1, triggerPoint = 1 ),
//        reduce = ReduceOutliers.byCorroborationPercentage(50),
//        planSpecification = ConfigFactory.parseString(
//          s"""
//             |algorithm-config.${algo.name}.seedEps: 5.0
//             |algorithm-config.${algo.name}.minDensityConnectedPoints: 3
//          """.stripMargin
//        )
//      )
//    }
//  }
//
//  object Fixture {
//    case class TickA( topic: String = "[default]", values: Seq[Int] = Seq(TickA.tickId.incrementAndGet()) )
//
//    object TickA {
//      val tickId = new AtomicInteger()
//      def merge( lhs: TickA, rhs: TickA ): TickA = lhs.copy( values = lhs.values ++ rhs.values )
//    }
//  }
//
//  override def createAkkaFixture( test: OneArgTest ): Fixture = new Fixture
//
//  val NEXT = Tag( "next" )
//  val DONE = Tag( "done" )
//
//  "OutlierPlanDetectionRouter" should {
////    "make functioning plan stream" in { f: Fixture =>
////      import f._
////      val actual = makePlanRouter().underlyingActor.makePlanStream( plan, subscriber.ref )
////      log.info( "actual = [{}]", actual )
////      actual.ingressRef ! TimeSeries( "dummy", Seq() )
////      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
////      subscriber.expectMsgClass( classOf[NoOutliers] )
////    }
//
////    "make resuable plan stream ingress once" in { f: Fixture =>
////      import f._
////      val pr = makePlanRouter()
////      val a1 = pr.underlyingActor.streamIngressFor( plan, subscriber.ref )
////      log.info( "first actual = [{}]", a1 )
////      val msg = TimeSeries( "dummy", Seq() )
////      a1 ! msg
////      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
////      subscriber.expectMsgClass( classOf[NoOutliers] )
////
////      val a2 = pr.underlyingActor.streamIngressFor( plan, subscriber.ref )
////      log.info( "second actual = [{}]", a2 )
////      a1 mustBe a2
////      a2 ! msg
////      detector.expectMsgClass( classOf[DetectOutliersInSeries] )
////      subscriber.expectMsgClass( classOf[NoOutliers] )
////    }
//
////    "make plan-dependent streams" in { f: Fixture =>
////      import f._
////
////      val p1 = makePlan( "p1", Some( OutlierPlan.Grouping(10, 300.seconds) ) )
////      val p2 = makePlan( "p2", None )
////
//////      val m1 = OutlierDetectionMessage( TimeSeries("dummy", Seq()), p1 ).toOption.get
//////      val m2 = OutlierDetectionMessage( TimeSeries("dummy", Seq()), p2 ).toOption.get
////      val m1 = TimeSeries( "dummy", Seq() )
////      val m2 = TimeSeries( "dummy", Seq() )
////
////      val pr = makePlanRouter( Set(p1, p2) )
////      val a1 = pr.underlyingActor.streamIngressFor( p1, subscriber.ref )
////      log.info( "first actual ingress = [{}]", a1 )
////      log.info( "first actual graph = [{}]", pr.underlyingActor.planStreams( OutlierPlanDetectionRouter.Key( p1.id, subscriber.ref ) ) )
////      a1 ! m1
////      detector.expectNoMsg( 500.millis.dilated )
////      subscriber.expectNoMsg( 500.millis.dilated )
////
////      val a2 = pr.underlyingActor.streamIngressFor( p2, subscriber.ref )
////      log.info( "second actual ingress = [{}]", a2 )
////      log.info( "second actual graph = [{}]", pr.underlyingActor.planStreams( OutlierPlanDetectionRouter.Key( p2.id, subscriber.ref ) ) )
////      a1 must not be a2
////      a2 ! m2
////      detector.expectMsgClass( 200.millis.dilated, classOf[DetectOutliersInSeries] )
////      subscriber.expectMsgClass( 200.millis.dilated, classOf[NoOutliers] )
////    }
//
////    "router receive uses plan-dependent streams" taggedAs (WIP) in { f: Fixture =>
////      import f._
////
////      val p1 = makePlan( "p1", Some( OutlierPlan.Grouping(10, 300.seconds) ) )
////      val p2 = makePlan( "p2", None )
////
////      val points = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
////      val ts = spike( points )( points.size - 1 )
////
////      val m1 = (ts, p1)
////      val m2 = (ts, p2)
////
////      val pr = makePlanRouter( Set(p1, p2) )
////      pr.receive( m1, subscriber.ref )
////      detector.expectNoMsg( 500.millis.dilated )
////      subscriber.expectNoMsg( 500.millis.dilated )
////
////      pr.receive( m2, subscriber.ref )
////      detector.expectMsgClass( 200.millis.dilated, classOf[DetectOutliersInSeries] )
////      subscriber.expectMsgClass( 200.millis.dilated, classOf[NoOutliers] )
////    }
//
//
////    "router actor uses plan-dependent streams" in { f: Fixture =>
////      import f._
////
////      val p1 = makePlan( "p1", Some( OutlierPlan.Grouping(10, 300.seconds) ) )
////      val p2 = makePlan( "p2", None )
////
////      val points = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
////      val ts = spike( points )( points.size - 1 )
////
////      val m1 = ( ts, OutlierPlan.Scope(p1, ts.topic) )
////      val m2 = ( ts, OutlierPlan.Scope(p2, ts.topic) )
////
////      val routerProps = OutlierPlanDetectionRouter.props(
////        detectorRef = detector.ref,
////        detectionBudget = 2.seconds,
////        bufferSize = 100,
////        maxInDetectionCpuFactor = 1.0,
////        plans = Set( p1, p2)
////      )
////
////      val router = system.actorOf( routerProps, "test-plan-router" )
////
////      router.tell( m1, subscriber.ref )
////      detector.expectNoMsg( 500.millis.dilated )
////      subscriber.expectNoMsg( 500.millis.dilated )
////
////      router.tell( m2, subscriber.ref )
////      detector.expectMsgClass( 200.millis.dilated, classOf[DetectOutliersInSeries] )
////      subscriber.expectMsgClass( 200.millis.dilated, classOf[NoOutliers] )
////    }
//
//    "plan router flow routes to all isApplicable plans" in { f: Fixture =>
//      import f._
//
//      val p1 = makePlan( "p1", None )
//      val p2 = makePlan( "p2", None )
//      val testPlans = Set( p1 /*, p2*/ )
//
//      val planRouterRef = system.actorOf(
//        OutlierPlanDetectionRouter.props(
//          detectorRef = detector.ref,
//          detectionBudget = 2.seconds,
//          bufferSize = 100,
//          maxInDetectionCpuFactor = 1.0
//        ),
//        "PlanRouter"
//      )
//      logger.debug( "TEST: planRouterRef=[{}]", planRouterRef)
//
//      val planDetectionFlow = OutlierPlanDetectionRouter.flow( planRouterRef )
//      logger.debug( "TEST: planDetectionFlow=[{}]", planDetectionFlow )
//
//      val preFlow = {
//        Flow[TimeSeries]
//        .map { ts =>
//          logger.debug( "PLAN NAMES:[{}]", testPlans.map{_.name}.mkString(",") )
//          testPlans collect { case p if p appliesTo ts =>
//            logger.debug( "plan [{}] applies to ts [{}]", p.name, ts.topic )
//            (ts, OutlierPlan.Scope(p, ts.topic))
//          }
//        }
//        .mapConcat { identity }
//        .map { m => logger.debug( "FLOW: [{}]", m ); m }
//        .via( OutlierScoringModel.batchSeriesByPlan(100) )
//        .map { m =>
//          logger.info( "passing message onto plan router: [{}]", m )
//          m
//        }
//        .named( "preFlow" )
//      }
//
//      val flowUnderTest = preFlow via planDetectionFlow
//
//      val pts1 = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
//      val ts1 = spike( pts1 )( pts1.size - 1 )
//      val data = Seq( ts1 )
//
//      val (pub, sub) = {
//        TestSource.probe[TimeSeries]
//        .via( flowUnderTest )
//        .toMat( TestSink.probe[Outliers] )( Keep.both )
//        .run()
//      }
//
//      val ps = pub.expectSubscription()
//      val ss = sub.expectSubscription()
//
//      ss.request( 2 )
//
//      data foreach { ps.sendNext }
//
//      val a1 = sub.expectNext()
//      testPlans.contains( a1.plan ) mustBe true
//      a1.source mustBe ts1
//
//      val a2 = sub.expectNext()
//      testPlans.contains( a2.plan ) mustBe true
//      a1.plan must not be a2.plan
//      a2.source mustBe ts1
//    }
//
//    //    "framed flow convert graphite pickle into TimeSeries" in { f: Fixture =>
////      import f._
////      val now = joda.DateTime.now
////      val dp = makeDataPoints( points, start = now ).take( 5 )
////      val expected = TimeSeries( "foobar", dp )
////
////      val flowUnderTest = Flow[ByteString]
////        .via( protocol.framingFlow() )
////        .via( protocol.unmarshalTimeSeriesData )
////
////      val future = Source( List(withHeader(pickledWithDefaultTopic(dp))) ).via( flowUnderTest ).runWith( Sink.head )
////      val result = Await.result( future, 1.second.dilated )
////      result mustBe expected
////    }
//
////    "convert pickles from framed ByteStream" in { f: Fixture =>
////      import f._
////      val now = joda.DateTime.now
////      val dp1 = makeDataPoints( points, start = now ).take( 5 )
////      val dp2 = makeDataPoints( pointsA, start = now+7 ).take( 5 )
////
//////      val expected = TimeSeries.seriesMerging.merge( TimeSeries("foobar", dp1), TimeSeries("foobar", dp2) ).toOption.get
////      val expected = TimeSeries( "foobar", dp1 ++ dp2 )
////      trace( s"expected = $expected" )
////
////      val flowUnderTest = Flow[ByteString]
////        .via( protocol.framingFlow() )
////        .via( protocol.unmarshalTimeSeriesData )
////
////      val pickles = withHeader( pickled( Seq(dp1, dp2) map { ("foobar", _) } ) )
////      trace( s"pickles = ${pickles.utf8String}" )
////      trace( s"byte-pickles = ${pickles}" )
////      val future = Source( List(pickles) ).via( flowUnderTest ).runWith( Sink.head )
////      val result = Await.result( future, 1.second.dilated )
////      result mustBe expected
////    }
//
////    "read sliding window" in { f: Fixture =>
////      import f._
////      import system.dispatcher
////
////      val now = joda.DateTime.now
////      val dp1 = makeDataPoints( points, start = now ).take( 3 )
////      val dp2 = makeDataPoints( pointsA, start = now + 2L ).take( 3 )
////      val dp3 = makeDataPoints( pointsB, start = now + 3L ).take( 3 )
////
////      val expected = Set(
////        TimeSeries( "bar", dp2 ),
////        TimeSeries.seriesMerging.merge( TimeSeries("foo", dp1), TimeSeries("foo", dp3) ).toOption.get
////      )
////
////
////      val flowUnderTest: Flow[TimeSeries, TimeSeries, Unit] = OutlierScoringModel.batchSeries( windowSize = 1.second, parallelism = 4 )
////      val topics = List( "foo", "bar", "foo" )
////      val data: List[TimeSeries] = topics.zip(List(dp1, dp2, dp3)).map{ case (t,p) => TimeSeries(t, p) }
////      trace( s"""data=[${data.mkString(",\n")}]""")
////
////      val future = Source( data )
////                   .via( flowUnderTest )
////                   .grouped( 10 )
////                   .runWith( Sink.head )
////
////      val result = Await.result( future, 1.second.dilated )
////      result.toSet mustBe expected
////    }
//
//
////    "detect Outliers" taggedAs (WIP) in { f: Fixture =>
////      import f._
////      import system.dispatcher
////      import com.github.nscala_time.time.OrderingImplicits._
////
////      val algos = Set( algo )
////      val grouping: Option[OutlierPlan.Grouping] = {
////        val window = None
////        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
////      }
////
////      val defaultPlan = OutlierPlan.default(
////        name = "DEFAULT_PLAN",
////        algorithms = algos,
////        grouping = grouping,
////        timeout = 5000.millis,
////        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = algos.size, triggerPoint = 1 ),
////        reduce = Settings.makeOutlierReducer,
////        planSpecification = ConfigFactory.parseString(
////          algos
////          .map { a =>
////            s"""
////               |algorithm-config.${a.name} {
////               |  tolerance: 1.043822701 // eps:0.75
////               |  seedEps: 0.75
////               |  minDensityConnectedPoints: 3
////               |  distance: Mahalanobis // Euclidean
////               |}
////            """.stripMargin
////          }
////          .mkString( "\n" )
////        )
////      )
////
////      val routerRef = system.actorOf( DetectionAlgorithmRouter.props, "router" )
////      val dbscan = system.actorOf( SeriesDensityAnalyzer.props( routerRef ), "dbscan" )
////      val detector = system.actorOf( OutlierDetection.props( routerRef = routerRef ), "detectOutliers" )
////
////      val now = new joda.DateTime( 2016, 3, 25, 10, 38, 40, 81 ) // new joda.DateTime( joda.DateTime.now.getMillis / 1000L * 1000L )
////      logger.debug( "USE NOW = {}", now )
////
////      val dp1 = makeDataPoints( points, start = now, period = 1.seconds )
//////      val dp2 = makeDataPoints( pointsA, start = joda.DateTime.now )
//////      val dp3 = makeDataPoints( pointsB, start = joda.DateTime.now )
////
////      val expectedValues = Set( 31.5, 39.2 )
////      val expectedPoints = dp1 filter { expectedValues contains _.value } sortBy { _.timestamp }
////
////      val expected = SeriesOutliers(
////        algorithms = algos,
////        source = TimeSeries("foo", dp1),
////        outliers = expectedPoints,
////        plan = defaultPlan
////      )
//////      val expected = TimeSeries( "foo", (dp1 ++ dp3).sortBy( _.timestamp ) )
////
////      val graphiteFlow = OutlierScoringModel.batchSeries( parallelism = 4, windowSize = 20.millis )
////      val detectFlow = OutlierDetection.detectionFlow(
////        detector = detector,
////        maxInDetectionCpuFactor = 1,
////        maxAllowedWait = 2.seconds,
////        plans = Seq( defaultPlan )
////      )
////
////      val flowUnderTest = graphiteFlow via detectFlow
////
////      val (pub, sub) = {
////        TestSource.probe[TimeSeries]
////        .via( flowUnderTest )
////        .toMat( TestSink.probe[Outliers] )( Keep.both )
////        .run()
////      }
////
////      val ps = pub.expectSubscription()
////      val ss = sub.expectSubscription()
////
////      ss.request( 1 )
////
////      val topics = List( "foo", "bar", "foo" )
////      val data: List[TimeSeries] = topics.zip(List(dp1)).map{ case (t,p) => TimeSeries(t, p) }
////
////      data foreach { ps.sendNext }
////      val actual = sub.expectNext()
////      actual.algorithms mustBe expected.algorithms
////      actual mustBe a [SeriesOutliers]
////      actual.asInstanceOf[SeriesOutliers].outliers mustBe expected.outliers
////      actual.anomalySize mustBe expected.anomalySize
////      actual.hasAnomalies mustBe expected.hasAnomalies
////      actual.size mustBe expected.size
////      actual.source mustBe expected.source
////      actual.topic mustBe expected.topic
////      actual mustBe expected
////////////////////////////////////////////////////////
//////      pub.sendNext( data(0) )
//////
//////      Source( data )
//////      .via( flowUnderTest )
//////      .runWith( TestSink.probe[Outliers] )
//////      .request( 1 )
//////      .expectEventPF[Outliers] {
//////        case OnNext( actual ) => {
//////          val foo: Int = actual
//////        }
//////      }
//////      .expectNext( )
////
//////      val future: Int = Source( data )
//////                   .via( flowUnderTest )
//////                   .runWith( sinkProbe )
//////      logger.debug( "STREAM future = {}", future )
//////      val result = Await.result( future, 2.seconds.dilated )
//////      trace( s"result class   [${result.hashCode}] = ${result.getClass}")
//////      trace( s"expected class [${expected.hashCode}] = ${expected.getClass}")
//////      result.algorithms mustBe expected.algorithms
//////      result mustBe a [SeriesOutliers]
//////      result.asInstanceOf[SeriesOutliers].outliers mustBe expected.outliers
//////      result.anomalySize mustBe expected.anomalySize
//////      result.hasAnomalies mustBe expected.hasAnomalies
//////      result.size mustBe expected.size
//////      result.source mustBe expected.source
//////      result.topic mustBe expected.topic
//////      result mustBe expected
////    }
//
////    "grouped Example" in { f: Fixture =>
////      import f._
////
////      val topics = IndexedSeq( "a", "b", "b", "b", "c" )
////
////      val tickFn = () => {
////        val next = Fixture.TickA.tickId.incrementAndGet()
////        val topic = topics( next % topics.size )
////        Fixture.TickA( topic, Seq(next) )
////      }
////
////      def conflateFlow[T](): Flow[T, T, Unit] = {
////        Flow[T]
////        .conflate( _ => List.empty[T] ){ (l, u) => u :: l }
////        .mapConcat(identity)
////      }
////
////      val source = Source.tick( 0.second, 50.millis, tickFn ).map { t => t() }
////
////      val flowUnderTest: Flow[Fixture.TickA, Fixture.TickA, Unit] = {
////        Flow[Fixture.TickA]
////        .groupedWithin( n = 10000, d = 210.millis )
////        .map {
////          _.groupBy( _.topic )
////          .map {case (topic, es) => es.tail.foldLeft( es.head ) {(acc, e) => Fixture.TickA.merge( acc, e ) } }
////        }
////        .mapConcat { identity }
////      }
////
////      val future = source
////                   .via( flowUnderTest )
//////                   .grouped( 5 )
////                   .runWith( Sink.head )
////
////      val result = Await.result( future, 5.seconds.dilated )
////      result mustBe Fixture.TickA("b", Seq(1,2,3))
////    }
////
////    "ex1" in { f: Fixture =>
////      import f._
////      val sinkUnderTest = Flow[Int].map{ _ * 2 }.toMat{ Sink.fold( 0 ){ _ + _ } }( Keep.right )
////      val future = Source( 1 to 4 ) runWith sinkUnderTest
////      val result = Await.result( future, 2.seconds.dilated )
////      result mustBe 20
////    }
////
////    "ex2" in { f: Fixture =>
////      import f._
////      val sourceUnderTest = Source.repeat(1).map(_ * 2)
////      val future = sourceUnderTest.grouped(10).runWith(Sink.head)
////      val result = Await.result( future, 2.second.dilated )
////      result mustBe Seq.fill(10)(2)
////    }
////
////    "ex3" in { f: Fixture =>
////      import f._
////      val flowUnderTest = Flow[Int].takeWhile(_ < 5)
////      val future = Source( 1 to 10 ).via( flowUnderTest ).runWith( Sink.fold(Seq.empty[Int]){ _ :+ _ } )
////      val result = Await.result( future, 2.seconds.dilated )
////      result mustBe (1 to 4)
////    }
////
////    "ex4" in { f: Fixture =>
////      import f._
////      import f.system.dispatcher
////      import akka.pattern.pipe
////
////      val sourceUnderTest = Source( 1 to 4 ).grouped(2)
////      val probe = TestProbe()
////      sourceUnderTest.grouped(2).runWith(Sink.head).pipeTo(probe.ref)
////      probe.expectMsg( 2.seconds.dilated, Seq(Seq(1,2), Seq(3,4)))
////    }
////
////    "ex5" in { f: Fixture =>
////      import f._
////      case object Tick
////      val sourceUnderTest = Source.tick( 0.seconds, 200.millis, Tick )
////      val probe = TestProbe()
////      val cancellable = sourceUnderTest.to( Sink.actorRef(probe.ref, "completed") ).run()
////
////      probe.expectMsg( 1.second, Tick )
////      probe.expectNoMsg( 175.millis )
////      probe.expectMsg( 200.millis.dilated, Tick )
////      cancellable.cancel()
////      probe.expectMsg( 200.millis.dilated, "completed" )
////    }
////
////    "ex6" in { f: Fixture =>
////      import f._
////      val sinkUnderTest = Flow[Int].map(_.toString).toMat(Sink.fold("")(_ + _))(Keep.right)
////      val (ref, future) = Source.actorRef( 8, OverflowStrategy.fail ).toMat(sinkUnderTest)(Keep.both).run()
////
////      ref ! 1
////      ref ! 2
////      ref ! 3
////      ref ! akka.actor.Status.Success("done")
////
////      val result = Await.result( future, 1.second.dilated )
////      result mustBe "123"
////    }
////
////    "ex7" in { f: Fixture =>
////      import f._
////      val sourceUnderTest = Source( 1 to 4 ).filter(_ % 2 == 0).map(_ * 2)
////      sourceUnderTest
////        .runWith( TestSink.probe[Int] )
////        .request(2)
////        .expectNext(4, 8)
////        .expectComplete()
////    }
////
////    "ex8" in { f: Fixture =>
////      import f._
////      val sinkUnderTest = Sink.cancelled
////      TestSource.probe[Int]
////        .toMat( sinkUnderTest )( Keep.left )
////        .run()
////        .expectCancellation()
////    }
////
////    "ex9" in { f: Fixture =>
////      import f._
////      val sinkUnderTest = Sink.head[Int]
////      val (probe, future) = TestSource.probe[Int].toMat( sinkUnderTest )( Keep.both ).run()
////      probe.sendError( new Exception("BOOM") )
////      Await.ready( future, 100.millis )
////      val Failure( exception ) = future.value.get
////      exception.getMessage mustBe "BOOM"
////    }
////
////    "ex10" in { f: Fixture =>
////      pending
//////      akka docs seem to req updating from 1.x
////      import f._
////      import system.dispatcher
////      val flowUnderTest = Flow[Int].mapAsyncUnordered(2) { sleep =>
////        pattern.after( 10.millis * sleep, using = system.scheduler )( Future.successful(sleep) )
////      }
////
////      val ( pub, sub ) = TestSource.probe[Int]
////        .via( flowUnderTest )
////        .toMat( TestSink.probe[Int] )( Keep.both )
////        .run()
////
////      sub.request( n = 3 )
////      pub.sendNext( 3 )
////      pub.sendNext( 2 )
////      pub.sendNext( 1 )
////      sub.expectNextUnordered( 1, 2, 3 )
////
////      pub.sendError( new Exception("Power surge in the linear subroutine C-47!") )
////      val ex = sub.expectError
////      ex.getMessage.contains( "C-47" ) mustBe true
////    }
//  }
//}
//
//object OutlierPlanDetectionRouterSpec {
//  val trace = Trace[OutlierPlanDetectionRouterSpec.type]
//
//  def spike( data: Seq[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
//    val (front, last) = data.sortBy{ _.timestamp.getMillis }.splitAt( position )
//    val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
//    TimeSeries( "test-series", spiked )
//  }
//
//  def makeDataPoints(
//    values: Seq[Double],
//    start: joda.DateTime = joda.DateTime.now,
//    period: FiniteDuration = 1.second,
//    wiggleFactor: (Double, Double) = (1.0, 1.0)
//  ): Seq[DataPoint] = {
//    val secs = start.getMillis / 1000L
//    val epochStart = new joda.DateTime( secs * 1000L )
//    val random = new RandomDataGenerator
//    def nextFactor: Double = {
//      if ( wiggleFactor._1 == wiggleFactor._2 ) wiggleFactor._1
//      else random.nextUniform( wiggleFactor._1, wiggleFactor._2 )
//    }
//
//    values.zipWithIndex map { vi =>
//      val (v, i) = vi
//      val adj = (i * nextFactor) * period
//      val ts = epochStart + adj.toJodaDuration
//      DataPoint( timestamp = ts, value = v )
//    }
//  }
//
//  val points: Seq[Double] = Seq(
//    9.46,
//    9.9,
//    11.6,
//    14.5,
//    17.3,
//    19.2,
//    18.4,
//    14.5,
//    12.2,
//    10.8,
//    8.58,
//    8.36,
//    8.58,
//    7.5,
//    7.1,
//    7.3,
//    7.71,
//    8.14,
//    8.14,
//    7.1,
//    7.5,
//    7.1,
//    7.1,
//    7.3,
//    7.71,
//    8.8,
//    9.9,
//    14.2,
//    18.8,
//    25.2,
//    31.5,
//    22,
//    24.1,
//    39.2
//  )
//
//
//  val pointsA: Seq[Double] = Seq(
//    9.46,
//    9.9,
//    11.6,
//    14.5,
//    17.3,
//    19.2,
//    18.4,
//    14.5,
//    12.2,
//    10.8,
//    8.58,
//    8.36,
//    8.58,
//    7.5,
//    7.1,
//    7.3,
//    7.71,
//    8.14,
//    8.14,
//    7.1,
//    7.5,
//    7.1,
//    7.1,
//    7.3,
//    7.71,
//    8.8,
//    9.9,
//    14.2
//  )
//
//  val pointsB: Seq[Double] = Seq(
//    10.1,
//    10.1,
//    9.68,
//    9.46,
//    10.3,
//    11.6,
//    13.9,
//    13.9,
//    12.5,
//    11.9,
//    12.2,
//    13,
//    13.3,
//    13,
//    12.7,
//    11.9,
//    13.3,
//    12.5,
//    11.9,
//    11.6,
//    10.5,
//    10.1,
//    9.9,
//    9.68,
//    9.68,
//    9.9,
//    10.8,
//    11
//  )
//
//}
