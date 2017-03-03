package spotlight.analysis

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.Failure
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }
import akka.stream.{ ActorMaterializer, Materializer, OverflowStrategy }
import akka.testkit._
import akka.util.ByteString
import akka.{ NotUsed, pattern }
import com.github.nscala_time.time.Imports.{ richDateTime, richSDuration }
import com.typesafe.config.{ Config, ConfigFactory, ConfigOrigin }
import demesne.{ AggregateRootType, BoundedContext, DomainModel }
import org.apache.commons.math3.random.RandomDataGenerator
import org.joda.{ time ⇒ joda }
import org.scalatest.Tag
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import omnibus.commons.log.Trace
import spotlight.analysis.shard.{ CellShardModule, LookupShardModule }
import spotlight.analysis.{ AnalysisPlanProtocol ⇒ AP }
import spotlight.analysis.algorithm.statistical.SimpleMovingAverageAlgorithm
import spotlight.model.outlier._
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.model.timeseries.{ DataPoint, TimeSeries }
import spotlight.protocol.{ GraphiteSerializationProtocol, PythonPickleProtocol }
import spotlight.testkit.ParallelAkkaSpec
import spotlight.{ Settings, Spotlight }

/** Created by rolfsd on 10/28/15.
  */
class OutlierScoringModelSpec extends ParallelAkkaSpec with MockitoSugar {
  import OutlierScoringModelSpec._

  override def testConfiguration( test: OneArgTest, slug: String ): Config = {
    val tc = ConfigFactory.parseString(
      """
        |spotlight.workflow.detect.timeout: 10s
        |spotlight.source.buffer: 1000
        |spotlight.workflow.buffer: 1000
        |spotlight.source.host: "0.0.0.0"
        |spotlight.source.port: 2004
        |spotlight.detection-plans {
        |  skyline {
        |    majority: 50
        |    default: on
        |    algorithms.simple-moving-average.publish-threshold: yes
        |  }
        |}
      """.stripMargin
    )
    val c = spotlight.testkit.config( systemName = slug )
    import scala.collection.JavaConverters._
    logger.debug( "Test Config: akka.cluster.seed-nodes=[{}]", c.getStringList( "akka.cluster.seed-nodes" ).asScala.mkString( ", " ) )
    tc withFallback c
  }

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class TestSettingsWithPlans( override val plans: Set[AnalysisPlan], underlying: Settings ) extends Settings {
    override def sourceAddress: InetSocketAddress = underlying.sourceAddress
    override def clusterPort: Int = underlying.clusterPort
    override def maxFrameLength: Int = underlying.maxFrameLength
    override def protocol: GraphiteSerializationProtocol = underlying.protocol
    override def windowDuration: FiniteDuration = underlying.windowDuration
    override def graphiteAddress: Option[InetSocketAddress] = underlying.graphiteAddress
    override def detectionBudget: Duration = underlying.detectionBudget
    override def parallelismFactor: Double = underlying.parallelismFactor
    override def planOrigin: ConfigOrigin = underlying.planOrigin
    override def tcpInboundBufferSize: Int = underlying.tcpInboundBufferSize
    override def workflowBufferSize: Int = underlying.workflowBufferSize
    override def args: Seq[String] = underlying.args
    override def config: Config = underlying.config
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AkkaFixture( _config, _system, _slug ) {
    fixture ⇒

    //    logger.debug( "config:: akka.actor.provider=[{}] origin:[{}]", config.getValue("akka.actor.provider"), config.getValue("akka.actor.provider").origin() )

    implicit val materializer: Materializer = ActorMaterializer()

    val settings: Settings = {
      Settings( Array( "-c", "2552" ), _config ).disjunction match {
        case scalaz.\/-( s ) ⇒ s
        case scalaz.-\/( exs ) ⇒ {
          exs foreach { ex ⇒ logger.info( "Setting error: [{}]", ex ) }
          throw exs.head
        }
      }
    }

    val protocol = new PythonPickleProtocol
    val stringFlow: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString].via( protocol.framingFlow() )

    val bus = TestProbe()
    system.eventStream.subscribe( bus.ref, classOf[AP.Event] )

    //    val configurationReloader = Settings.reloader( Array.empty[String] )()()

    //    val algo = SeriesDensityAnalyzer.Algorithm
    val algo = SimpleMovingAverageAlgorithm.label
    val algoRef = TestProbe()
    val routingTable = Map( algo → algoRef.ref )

    val grouping: Option[AnalysisPlan.Grouping] = {
      val window = None
      window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
    }

    val emptyConfig = ConfigFactory.empty()

    val plan = AnalysisPlan.default(
      name = "DEFAULT_PLAN",
      algorithms = Map(
        algo →
          ConfigFactory.parseString(
            s"""
          |seedEps: 5.0
          |minDensityConnectedPoints: 3
          """.stripMargin
          )
      ),
      grouping = grouping,
      timeout = 500.millis,
      isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = 1, triggerPoint = 1 ),
      reduce = ReduceOutliers.byCorroborationPercentage( 50 ),
      planSpecification = ConfigFactory.parseString(
        s"""
          |algorithms.${algo}.seedEps: 5.0
          |algorithms.${algo}.minDensityConnectedPoints: 3
        """.stripMargin
      )
    )

    def rootTypes: Set[AggregateRootType] = Set(
      AnalysisPlanModule.module.rootType,
      LookupShardModule.rootType,
      CellShardModule.module.rootType,
      SimpleMovingAverageAlgorithm.module.rootType
    )

    lazy val boundedContext: BoundedContext = trace.block( "boundedContext" ) {
      implicit val actorTimeout = akka.util.Timeout( 5.seconds.dilated )
      import scala.concurrent.ExecutionContext.Implicits.global
      val bc = {
        for {
          made ← BoundedContext.make( Symbol( slug ), config, rootTypes, startTasks = Set() )
          started ← made.start()
        } yield started
      }
      Await.result( bc, 5.seconds )
    }

    implicit lazy val model: DomainModel = trace.block( "model" ) { Await.result( boundedContext.futureModel, 5.seconds ) }

    def makePlan( name: String, g: Option[AnalysisPlan.Grouping] ): AnalysisPlan = {
      AnalysisPlan.default(
        name = name,
        algorithms = Map(
          algo →
            ConfigFactory.parseString(
              s"""
            |seedEps: 5.0
            |minDensityConnectedPoints: 3
            """.stripMargin
            )
        ),
        grouping = g,
        timeout = 500.millis,
        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = 1, triggerPoint = 1 ),
        reduce = ReduceOutliers.byCorroborationPercentage( 50 ),
        planSpecification = ConfigFactory.parseString(
          s"""
             |algorithms.${algo}.seedEps: 5.0
             |algorithms.${algo}.minDensityConnectedPoints: 3
          """.stripMargin
        )
      )
    }

    val plans = Seq( plan )
  }

  object Fixture {
    case class TickA( topic: String = "[default]", values: Seq[Int] = Seq( TickA.tickId.incrementAndGet() ) )

    object TickA {
      val tickId = new AtomicInteger()
      def merge( lhs: TickA, rhs: TickA ): TickA = lhs.copy( values = lhs.values ++ rhs.values )
    }
  }

  val NEXT = Tag( "next" )
  val DONE = Tag( "done" )

  "GraphiteModel" should {
    "convert pickle to TimeSeries" in { f: Fixture ⇒
      import f._
      val now = joda.DateTime.now
      val dp = makeDataPoints( points, start = now ).take( 5 )
      val expected = List( TimeSeries( "foobar", dp ) )
      val actual = protocol.toTimeSeries( pickledWithDefaultTopic( dp ) )
      actual mustBe expected
    }

    "flow convert graphite pickle into TimeSeries" in { f: Fixture ⇒
      import f._
      val now = joda.DateTime.now
      val dp = makeDataPoints( points, start = now ).take( 5 )
      val expected = TimeSeries( "foobar", dp )

      val flowUnderTest = protocol.unmarshalTimeSeriesData
      //      val flowUnderTest = Flow[ByteString].mapConcat( PythonPickleProtocol.toTimeSeries )
      val future = Source( List( pickledWithDefaultTopic( dp ) ) ).via( flowUnderTest ).runWith( Sink.head )
      val result = Await.result( future, 200.millis.dilated )
      result mustBe expected
    }

    "framed flow convert graphite pickle into TimeSeries" in { f: Fixture ⇒
      import f._
      val now = joda.DateTime.now
      val dp = makeDataPoints( points, start = now ).take( 5 )
      val expected = TimeSeries( "foobar", dp )

      val flowUnderTest = Flow[ByteString]
        .via( protocol.framingFlow() )
        .via( protocol.unmarshalTimeSeriesData )

      val future = Source( List( withHeader( pickledWithDefaultTopic( dp ) ) ) ).via( flowUnderTest ).runWith( Sink.head )
      val result = Await.result( future, 1.second.dilated )
      result mustBe expected
    }

    "convert pickles from framed ByteStream" in { f: Fixture ⇒
      import f._
      val now = joda.DateTime.now
      val dp1 = makeDataPoints( points, start = now ).take( 5 )
      val dp2 = makeDataPoints( pointsA, start = now + 7 ).take( 5 )

      //      val expected = TimeSeries.seriesMerging.merge( TimeSeries("foobar", dp1), TimeSeries("foobar", dp2) ).toOption.get
      val expected = TimeSeries( "foobar", dp1 ++ dp2 )
      trace( s"expected = $expected" )

      val flowUnderTest = Flow[ByteString]
        .via( protocol.framingFlow() )
        .via( protocol.unmarshalTimeSeriesData )

      val pickles = withHeader( pickled( Seq( dp1, dp2 ) map { ( "foobar", _ ) } ) )
      trace( s"pickles = ${pickles.utf8String}" )
      trace( s"byte-pickles = ${pickles}" )
      val future = Source( List( pickles ) ).via( flowUnderTest ).runWith( Sink.head )
      val result = Await.result( future, 1.second.dilated )
      result mustBe expected
    }

    "read sliding window" in { f: Fixture ⇒
      import f._

      val now = joda.DateTime.now
      val dp1 = makeDataPoints( points, start = now ).take( 3 )
      val dp2 = makeDataPoints( pointsA, start = now + 2L ).take( 3 )
      val dp3 = makeDataPoints( pointsB, start = now + 3L ).take( 3 )

      val expected = Set(
        TimeSeries( "bar", dp2 ),
        TimeSeries.seriesMerging.merge( TimeSeries( "foo", dp1 ), TimeSeries( "foo", dp3 ) ).toOption.get
      )

      val flowUnderTest: Flow[TimeSeries, TimeSeries, NotUsed] = {
        OutlierScoringModel.batchSeriesByWindow( windowSize = 1.second, parallelism = 4 )
      }

      val topics = List( "foo", "bar", "foo" )
      val data: List[TimeSeries] = topics.zip( List( dp1, dp2, dp3 ) ).map { case ( t, p ) ⇒ TimeSeries( t, p ) }
      trace( s"""data=[${data.mkString( ",\n" )}]""" )

      val future = Source( data )
        .via( flowUnderTest )
        .grouped( 10 )
        .runWith( Sink.head )

      val result = Await.result( future, 1.second.dilated )
      result.toSet mustBe expected
    }

    "batch series by plan passthru without merging with demand" in { f: Fixture ⇒
      import OutlierScoringModelSpec._
      import f._

      val p1 = makePlan( "p1", None )
      val p2 = makePlan( "p2", None )
      val myPlans = Set( p1, p2 )

      val flowUnderTest = {
        Flow[TimeSeries]
          .map { ts ⇒
            myPlans collect {
              case p if p appliesTo ts ⇒
                logger.debug( "plan [{}] applies to ts [{}]", p.name, ts.topic )
                //            (ts, AnalysisPlan.Scope(p, ts.topic))
                ts
            }
          }
          .mapConcat { identity }
          //        .via( OutlierScoringModel.batchSeriesByPlan(100) )
          .via( OutlierScoringModel.regulateByTopic( 100 ) )
          .map { m ⇒
            logger.info( "passing message onto plan router: [{}]", m )
            m
          }
          .named( "preFlow" )
      }

      val pts1 = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
      val ts1 = spike( pts1 )( pts1.size - 1 )
      val pts2 = makeDataPoints(
        values = Seq.fill( 9 )( 1.0 ),
        start = pts1.last.timestamp.plusSeconds( 1 )
      )
      val ts2 = spike( pts2 )( pts2.size - 1 )
      val expectedTs = implicitly[Merging[TimeSeries]].merge( ts1, ts2 ).toOption.get

      val data = Seq( ts1, ts2 )

      val ( pub, sub ) = TestSource.probe[TimeSeries].via( flowUnderTest ).toMat( TestSink.probe[TimeSeries] )( Keep.both ).run()

      val ps = pub.expectSubscription()
      val ss = sub.expectSubscription()

      ss.request( 4 ) // request before sendNext creates demand

      data foreach { ps.sendNext }

      sub.expectNext() mustBe ts1
      sub.expectNext() mustBe ts2
    }

    "batch series by plan with merging if backpressured" in { f: Fixture ⇒
      import OutlierScoringModelSpec._
      import f._

      val p1 = makePlan( "p1", None )
      val p2 = makePlan( "p2", None )
      val myPlans = Set( p1, p2 )

      val flowUnderTest = {
        Flow[TimeSeries]
          .map { ts ⇒
            myPlans collect {
              case p if p appliesTo ts ⇒
                logger.debug( "plan [{}] applies to ts [{}]", p.name, ts.topic )
                //            (ts, AnalysisPlan.Scope(p, ts.topic))
                ts
            }
          }
          .mapConcat { identity }
          //        .via( OutlierScoringModel.batchSeriesByPlan(100) )
          .via( OutlierScoringModel.regulateByTopic( 100 ) )
          .map { m ⇒
            logger.info( "passing message onto plan router: [{}]", m )
            m
          }
          .named( "preFlow" )
      }

      val pts1 = makeDataPoints( values = Seq.fill( 9 )( 1.0 ) )
      val ts1 = spike( pts1 )( pts1.size - 1 )
      val pts2 = makeDataPoints(
        values = Seq.fill( 9 )( 1.0 ),
        start = pts1.last.timestamp.plusSeconds( 1 )
      )
      val ts2 = spike( pts2 )( pts2.size - 1 )
      val expectedTs = implicitly[Merging[TimeSeries]].merge( ts1, ts2 ).toOption.get

      val data = Seq( ts1, ts2 )

      val ( pub, sub ) = TestSource.probe[TimeSeries].via( flowUnderTest ).toMat( TestSink.probe[TimeSeries] )( Keep.both ).run()

      val ps = pub.expectSubscription()
      val ss = sub.expectSubscription()

      data foreach { ps.sendNext }

      ss.request( 2 ) // request after sendNext creates backpressure

      sub.expectNext() mustBe expectedTs
    }

    "detect Outliers" taggedAs WIP in { f: Fixture ⇒
      import com.github.nscala_time.time.OrderingImplicits._
      import f._
      import system.dispatcher
      import omnibus.akka.envelope._

      val algos = Map(
        algo → ConfigFactory.parseString(
          s"""
          |#tolerance: 1.043822701 // eps:0.75
          |tolerance: 3
          |tail-average: 3
          |minimum-population: 2
          |seedEps: 0.75
          |minDensityConnectedPoints: 3
          |distance: Mahalanobis // Euclidean
          """.stripMargin
        )
      )

      val grouping: Option[AnalysisPlan.Grouping] = {
        val window = None
        window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
      }

      val defaultPlan = AnalysisPlan.default(
        name = "DEFAULT_PLAN",
        algorithms = algos,
        grouping = grouping,
        timeout = 5000.millis,
        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = algos.size, triggerPoint = 1 ),
        reduce = ReduceOutliers.byCorroborationPercentage( 50 ),
        planSpecification = ConfigFactory.parseString(
          algos
            .map {
              case ( a, _ ) ⇒
                logger.warn( "a:[{}]", a )
                s"""
               |algorithms.${a} {
               |#  tolerance: 1.043822701 // eps:0.75
               |  tolerance: 3
               |  tail-average: 3
               |  minimum-population: 2
               |  seedEps: 0.75
               |  minDensityConnectedPoints: 3
               |  distance: Mahalanobis // Euclidean
               |}
            """.stripMargin
            }
            .mkString( "\n" )
        )
      )

      val testSettings = new TestSettingsWithPlans( Set( defaultPlan ), settings )

      logger.debug( "default plan = [{}]", defaultPlan )
      implicit val timeout = akka.util.Timeout( 5.seconds )
      logger.info( "BOOTSTRAP:BEFORE BoundedContext roottypes = [{}]", boundedContext.unsafeModel.rootTypes )
      val catalogRef = Await.result( Spotlight.makeCatalog( testSettings )( boundedContext ), 30.seconds )
      logger.info( "Catalog ref = [{}]", catalogRef )

      import PlanCatalogProtocol.{ CatalogFlow, MakeFlow, WaitForStart }
      val catalogFlow = {
        for {
          _ ← catalogRef ? WaitForStart
          CatalogFlow( f ) ← ( catalogRef ? MakeFlow( 2, system, timeout, materializer ) ).mapTo[CatalogFlow]
        } yield f
      }

      val flowUnderTest = Await.result( catalogFlow, 5.seconds )

      val now = new joda.DateTime( 2016, 3, 25, 10, 38, 40, 81 ) // new joda.DateTime( joda.DateTime.now.getMillis / 1000L * 1000L )
      logger.debug( "USE NOW = {}", now )

      val dp1 = makeDataPoints( points, start = now, period = 1.seconds )
      //      val dp2 = makeDataPoints( pointsA, start = joda.DateTime.now )
      //      val dp3 = makeDataPoints( pointsB, start = joda.DateTime.now )

      //      val expectedPoints = Seq( 1, 30 ).map { dp1.apply }.sortBy { _.timestamp }
      val expectedPoints = Seq( 30 ).map { dp1.apply }.sortBy { _.timestamp }

      val expected = SeriesOutliers(
        algorithms = algos.keySet,
        source = TimeSeries( "foo", dp1 ),
        outliers = expectedPoints,
        plan = defaultPlan
      )
      //      val expected = TimeSeries( "foo", (dp1 ++ dp3).sortBy( _.timestamp ) )

      ////      val graphiteFlow = OutlierScoringModel.batchSeriesByPlan( max = 1000 )
      //      val graphiteFlow = OutlierScoringModel.regulateByTopic( max = 1000 )
      ////      val detectFlow = AnalysisPlanDetectionRouter.flow( planRouter )
      //      val detectFlow = PlanCatalog.flow2( catalogRef )

      //      val flowUnderTest = {
      //        Flow[TimeSeries]
      ////        .map{ ts => (ts, AnalysisPlan.Scope(defaultPlan, ts.topic)) }
      //        .via( graphiteFlow )
      //        .via( detectFlow )
      //      }

      val ( pub, sub ) = {
        TestSource.probe[TimeSeries]
          .via( flowUnderTest )
          .toMat( TestSink.probe[Outliers] )( Keep.both )
          .run()
      }

      val ps = pub.expectSubscription()
      val ss = sub.expectSubscription()

      ss.request( 1 )

      val topics = List( "foo", "bar", "foo" )
      val data: List[TimeSeries] = topics.zip( List( dp1 ) ).map { case ( t, p ) ⇒ TimeSeries( t, p ) }

      logger.debug( "waiting to start....." )
      Thread.sleep( 1000 )
      logger.debug( "....starting" )
      data foreach { ps.sendNext }
      val actual = sub.expectNext()
      actual.algorithms mustBe expected.algorithms
      actual mustBe a[SeriesOutliers]
      actual.asInstanceOf[SeriesOutliers].outliers mustBe expected.outliers
      actual.anomalySize mustBe expected.anomalySize
      actual.hasAnomalies mustBe expected.hasAnomalies
      actual.size mustBe expected.size
      actual.source mustBe expected.source
      actual.topic mustBe expected.topic
      actual mustBe expected
    }

    "grouped Example" in { f: Fixture ⇒
      import f._

      val topics = IndexedSeq( "a", "b", "b", "b", "c" )

      val tickFn = () ⇒ {
        val next = Fixture.TickA.tickId.incrementAndGet()
        val topic = topics( next % topics.size )
        Fixture.TickA( topic, Seq( next ) )
      }

      //      def conflateFlow[T](): Flow[T, T, Unit] = {
      //        Flow[T]
      //        .conflate( _ => List.empty[T] ){ (l, u) => u :: l }
      //        .mapConcat(identity)
      //      }

      val source = Source.tick( 0.second, 50.millis, tickFn ).map { t ⇒ t() }

      val flowUnderTest: Flow[Fixture.TickA, Fixture.TickA, NotUsed] = {
        Flow[Fixture.TickA]
          .groupedWithin( n = 10000, d = 210.millis )
          .map {
            _.groupBy( _.topic )
              .map { case ( topic, es ) ⇒ es.tail.foldLeft( es.head ) { ( acc, e ) ⇒ Fixture.TickA.merge( acc, e ) } }
          }
          .mapConcat { identity }
      }

      val future = {
        source
          .via( flowUnderTest )
          //        .grouped( 5 )
          .runWith( Sink.head )
      }

      val result = Await.result( future, 5.seconds.dilated )
      result mustBe Fixture.TickA( "b", Seq( 1, 2, 3 ) )
    }

    "ex1" in { f: Fixture ⇒
      import f._
      val sinkUnderTest = Flow[Int].map { _ * 2 }.toMat { Sink.fold( 0 ) { _ + _ } }( Keep.right )
      val future = Source( 1 to 4 ) runWith sinkUnderTest
      val result = Await.result( future, 2.seconds.dilated )
      result mustBe 20
    }

    "ex2" in { f: Fixture ⇒
      import f._
      val sourceUnderTest = Source.repeat( 1 ).map( _ * 2 )
      val future = sourceUnderTest.grouped( 10 ).runWith( Sink.head )
      val result = Await.result( future, 2.second.dilated )
      result mustBe Seq.fill( 10 )( 2 )
    }

    "ex3" in { f: Fixture ⇒
      import f._
      val flowUnderTest = Flow[Int].takeWhile( _ < 5 )
      val future = Source( 1 to 10 ).via( flowUnderTest ).runWith( Sink.fold( Seq.empty[Int] ) { _ :+ _ } )
      val result = Await.result( future, 2.seconds.dilated )
      result mustBe ( 1 to 4 )
    }

    "ex4" in { f: Fixture ⇒
      import akka.pattern.pipe
      import f._
      import f.system.dispatcher

      val sourceUnderTest = Source( 1 to 4 ).grouped( 2 )
      val probe = TestProbe()
      sourceUnderTest.grouped( 2 ).runWith( Sink.head ).pipeTo( probe.ref )
      probe.expectMsg( 2.seconds.dilated, Seq( Seq( 1, 2 ), Seq( 3, 4 ) ) )
    }

    "ex5" in { f: Fixture ⇒
      import f._
      case object Tick
      val sourceUnderTest = Source.tick( 0.seconds, 200.millis, Tick )
      val probe = TestProbe()
      val cancellable = sourceUnderTest.to( Sink.actorRef( probe.ref, "completed" ) ).run()

      probe.expectMsg( 1.second, Tick )
      probe.expectNoMsg( 175.millis )
      probe.expectMsg( 200.millis.dilated, Tick )
      cancellable.cancel()
      probe.expectMsg( 200.millis.dilated, "completed" )
    }

    "ex6" in { f: Fixture ⇒
      import f._
      val sinkUnderTest = Flow[Int].map( _.toString ).toMat( Sink.fold( "" )( _ + _ ) )( Keep.right )
      val ( ref, future ) = Source.actorRef( 8, OverflowStrategy.fail ).toMat( sinkUnderTest )( Keep.both ).run()

      ref ! 1
      ref ! 2
      ref ! 3
      ref ! akka.actor.Status.Success( "done" )

      val result = Await.result( future, 1.second.dilated )
      result mustBe "123"
    }

    "ex7" in { f: Fixture ⇒
      import f._
      val sourceUnderTest = Source( 1 to 4 ).filter( _ % 2 == 0 ).map( _ * 2 )
      sourceUnderTest
        .runWith( TestSink.probe[Int] )
        .request( 2 )
        .expectNext( 4, 8 )
        .expectComplete()
    }

    "ex8" in { f: Fixture ⇒
      import f._
      val sinkUnderTest = Sink.cancelled
      TestSource.probe[Int]
        .toMat( sinkUnderTest )( Keep.left )
        .run()
        .expectCancellation()
    }

    "ex9" in { f: Fixture ⇒
      import f._
      val sinkUnderTest = Sink.head[Int]
      val ( probe, future ) = TestSource.probe[Int].toMat( sinkUnderTest )( Keep.both ).run()
      probe.sendError( new Exception( "BOOM" ) )
      Await.ready( future, 100.millis )
      val Failure( exception ) = future.value.get
      exception.getMessage mustBe "BOOM"
    }

    "ex10" in { f: Fixture ⇒
      pending
      //      akka docs seem to req updating from 1.x
      import f._
      import system.dispatcher
      val flowUnderTest = Flow[Int].mapAsyncUnordered( 2 ) { sleep ⇒
        pattern.after( 10.millis * sleep, using = system.scheduler )( Future.successful( sleep ) )
      }

      val ( pub, sub ) = TestSource.probe[Int]
        .via( flowUnderTest )
        .toMat( TestSink.probe[Int] )( Keep.both )
        .run()

      sub.request( n = 3 )
      pub.sendNext( 3 )
      pub.sendNext( 2 )
      pub.sendNext( 1 )
      sub.expectNextUnordered( 1, 2, 3 )

      pub.sendError( new Exception( "Power surge in the linear subroutine C-47!" ) )
      val ex = sub.expectError
      ex.getMessage.contains( "C-47" ) mustBe true
    }
  }
}

object OutlierScoringModelSpec {
  val trace = Trace[OutlierScoringModelSpec.type]

  def withHeader( body: ByteString ): ByteString = {
    val result = ByteBuffer.allocate( 4 + body.size )
    result putInt body.size
    result put body.toArray
    result.flip()
    ByteString( result )
  }

  def pickledWithDefaultTopic( dp: Seq[DataPoint] ): ByteString = pickled( Seq( ( "foobar", dp ) ) )

  def pickled( metrics: Seq[( String, Seq[DataPoint] )] ): ByteString = trace.block( s"pickled($metrics)" ) {
    import net.razorvine.pickle.Pickler

    val data = new java.util.LinkedList[AnyRef]
    for {
      metric ← metrics
      ( topic, points ) = metric
      p ← points
    } {
      val dp: Array[Any] = Array( p.timestamp.getMillis / 1000L, p.value )
      val metric: Array[AnyRef] = Array( topic, dp )
      data add metric
    }
    trace( s"data = $data" )

    val pickler = new Pickler( false )
    val out = pickler dumps data

    trace( s"""payload[${out.size}] = ${ByteString( out ).decodeString( "ISO-8859-1" )}""" )
    ByteString( out )
  }

  def makeDataPoints(
    values: Seq[Double],
    start: joda.DateTime = joda.DateTime.now,
    period: FiniteDuration = 1.second,
    wiggleFactor: ( Double, Double ) = ( 1.0, 1.0 )
  ): Seq[DataPoint] = {
    val secs = start.getMillis / 1000L
    val epochStart = new joda.DateTime( secs * 1000L )
    val random = new RandomDataGenerator
    def nextFactor: Double = {
      if ( wiggleFactor._1 == wiggleFactor._2 ) wiggleFactor._1
      else random.nextUniform( wiggleFactor._1, wiggleFactor._2 )
    }

    values.zipWithIndex map { vi ⇒
      val ( v, i ) = vi
      val adj = ( i * nextFactor ) * period
      val ts = epochStart + adj.toJodaDuration
      DataPoint( timestamp = ts, value = v )
    }
  }

  def spike( data: Seq[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
    val ( front, last ) = data.sortBy { _.timestamp.getMillis }.splitAt( position )
    val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
    TimeSeries( "test-series", spiked )
  }

  val points: Seq[Double] = Seq(
    9.46,
    9.9,
    11.6,
    14.5,
    17.3,
    19.2,
    18.4,
    14.5,
    12.2,
    10.8,
    8.58,
    8.36,
    8.58,
    7.5,
    7.1,
    7.3,
    7.71,
    8.14,
    8.14,
    7.1,
    7.5,
    7.1,
    7.1,
    7.3,
    7.71,
    8.8,
    9.9,
    14.2,
    18.8,
    25.2,
    31.5,
    22,
    24.1,
    39.2
  )

  val pointsA: Seq[Double] = Seq(
    9.46,
    9.9,
    11.6,
    14.5,
    17.3,
    19.2,
    18.4,
    14.5,
    12.2,
    10.8,
    8.58,
    8.36,
    8.58,
    7.5,
    7.1,
    7.3,
    7.71,
    8.14,
    8.14,
    7.1,
    7.5,
    7.1,
    7.1,
    7.3,
    7.71,
    8.8,
    9.9,
    14.2
  )

  val pointsB: Seq[Double] = Seq(
    10.1,
    10.1,
    9.68,
    9.46,
    10.3,
    11.6,
    13.9,
    13.9,
    12.5,
    11.9,
    12.2,
    13,
    13.3,
    13,
    12.7,
    11.9,
    13.3,
    12.5,
    11.9,
    11.6,
    10.5,
    10.1,
    9.9,
    9.68,
    9.68,
    9.9,
    10.8,
    11
  )

}
