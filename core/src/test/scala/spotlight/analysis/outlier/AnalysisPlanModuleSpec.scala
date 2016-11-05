package spotlight.analysis.outlier

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import akka.actor.{ActorContext, ActorRef, ActorSystem, PoisonPill, Props}
import demesne.index.StackableIndexBusPublisher
import demesne.module.{AggregateEnvironment, LocalAggregate}
import demesne.{AggregateRootType, DomainModel}
import demesne.repository.AggregateRootProps
import demesne.module.entity.{EntityAggregateModule, messages => EntityMessages}
import org.scalatest.{OptionValues, Tag}
import peds.akka.publish.StackableStreamPublisher
import peds.commons.V
import peds.commons.identifier.{ShortUUID, TaggedID}
import peds.commons.log.Trace
import shapeless.Lens
import spotlight.analysis.outlier.AnalysisPlanModule.AggregateRoot.OutlierPlanActor
import spotlight.analysis.outlier.AnalysisPlanModule.AggregateRoot.OutlierPlanActor.ProxyProvider
import spotlight.analysis.outlier.algorithm.statistical.SimpleMovingAverageAlgorithm
import spotlight.model.outlier._
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.outlier.{AnalysisPlanProtocol => P}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/15/16.
  */
class AnalysisPlanModuleSpec extends EntityModuleSpec[OutlierPlan] with OptionValues { outer =>
  override type ID = AnalysisPlanModule.module.ID
  override type Protocol = AnalysisPlanProtocol.type
  override val protocol: Protocol = AnalysisPlanProtocol


  abstract class FixtureModule extends EntityAggregateModule[OutlierPlan] { testModule =>
    private val trace: Trace[_] = Trace[FixtureModule]
    override val idLens: Lens[OutlierPlan, TaggedID[ShortUUID]] = OutlierPlan.idLens
    override val nameLens: Lens[OutlierPlan, String] = OutlierPlan.nameLens
    //      override def aggregateRootPropsOp: AggregateRootProps = testProps( _, _ )( proxy )
    override def environment: AggregateEnvironment = LocalAggregate
  }


  override def testConfiguration( test: OneArgTest, slug: String ): Config = AnalysisPlanModuleSpec.config( systemName = slug )

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    test.tags match {
      case ts if ts.contains( WORKFLOW.name ) => new WorkflowFixture( config, system, slug )
      case _ => new DefaultFixture( config, system, slug )
    }
  }

  abstract class Fixture( _config: Config, _system: ActorSystem, _slug: String )
  extends EntityFixture( _config, _system, _slug ) {
    protected val trace: Trace[_]

    val identifying = AnalysisPlanModule.identifying
    override def nextId(): module.TID = identifying.safeNextId
    lazy val plan = makePlan( "TestPlan", None )
    override lazy val tid: TID = plan.id

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

    override def rootTypes: Set[AggregateRootType] = trace.block( "rootTypes" ) {
      super.rootTypes ++ Set( SimpleMovingAverageAlgorithm.rootType )
    }

    lazy val algo: Symbol = SimpleMovingAverageAlgorithm.algorithm.label


    def stateFrom( ar: ActorRef, tid: module.TID ): OutlierPlan = {
      import scala.concurrent.ExecutionContext.Implicits.global
      import akka.pattern.ask
      Await.result(
        ( ar ? P.GetPlan(tid) ).mapTo[Envelope].map{ _.payload }.mapTo[P.PlanInfo].map{ _.info },
        2.seconds.dilated
      )
    }

    def proxiesFrom( ar: ActorRef, tid: module.TID ): Map[Topic, ActorRef] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      import akka.pattern.ask

      Await.result(
        ( ar ? P.GetProxies(tid) ).mapTo[P.Proxies].map{ _.scopeProxies },
        2.seconds.dilated
      )
    }
  }

  class DefaultFixture( _config: Config, _system: ActorSystem, _slug: String ) extends Fixture( _config, _system, _slug ) {
    override protected val trace = Trace[DefaultFixture]

    val proxyProbe = TestProbe()

    def testProps( model: DomainModel, rootType: AggregateRootType )( proxy: ActorRef ): Props = {
      Props( new TestOutlierPlanActor( model, rootType, proxy ) )
    }

    class TestOutlierPlanActor( model: DomainModel, rootType: AggregateRootType, proxy: ActorRef )
    extends OutlierPlanActor( model, rootType )
    with ProxyProvider
    with StackableStreamPublisher
    with StackableIndexBusPublisher {
      override def makeProxy(
        topic: Topic,
        plan: OutlierPlan
      )(
        implicit model: DomainModel,
        context: ActorContext
      ): ActorRef = {
        proxy
      }
    }

    class Module extends FixtureModule {
      override def aggregateRootPropsOp: AggregateRootProps = testProps( _, _ )( proxyProbe.ref )
    }

    override val module: Module = new Module
  }

  class WorkflowFixture( _config: Config, _system: ActorSystem, _slug: String ) extends Fixture( _config, _system, _slug ) {
    override protected val trace = Trace[WorkflowFixture]

    var proxyProbes = Map.empty[Topic, TestProbe]

    class FixtureOutlierPlanActor( model: DomainModel, rootType: AggregateRootType)
    extends AnalysisPlanModule.AggregateRoot.OutlierPlanActor( model, rootType )
    with AnalysisPlanModule.AggregateRoot.OutlierPlanActor.ProxyProvider
    with StackableStreamPublisher {
      override def makeProxy(topic: Topic, plan: OutlierPlan)( implicit model: DomainModel, context: ActorContext ): ActorRef = {
        val probe = TestProbe( topic.name )
        logger.debug( "TEST: making proxy probe: [{}]", probe )
        proxyProbes += topic -> probe
        probe.ref
      }

      override def active: Receive = {
        case m if super.active isDefinedAt m => {
          log.debug( "TEST: IN WORKFLOW FIXTURE ACTOR!!!" )
          super.active( m )
        }
      }

      override protected def onPersistRejected( cause: Throwable, event: Any, seqNr: Long ): Unit = {
        log.error(
          "Rejected to persist event type [{}] with sequence number [{}] for persistenceId [{}] due to [{}].",
          event.getClass.getName, seqNr, persistenceId, cause
        )
        throw cause
      }
    }

    class Module extends FixtureModule {
      override def aggregateRootPropsOp: AggregateRootProps = {
        ( m: DomainModel,rt: AggregateRootType ) => Props( new FixtureOutlierPlanActor( m, rt ) )
      }
    }

    override val module: Module = new Module
//    new EntityAggregateModule[OutlierPlan] {
//      override val environment: AggregateEnvironment = LocalAggregate
//      override def idLens: Lens[OutlierPlan, TaggedID[ShortUUID]] = OutlierPlan.idLens
//      override def nameLens: Lens[OutlierPlan, String] = OutlierPlan.nameLens
//      override def aggregateRootPropsOp: AggregateRootProps = {
//        ( model: DomainModel, rootType: AggregateRootType ) => Props( new FixtureOutlierPlanActor( model, rootType ) )
//      }
//    }
  }


  object WORKFLOW extends Tag( "workflow" )


  "AnalysisPlanModule" should {
    "make proxy for topic" in { f: Fixture =>
      import f._
      val planRef = TestActorRef[AnalysisPlanModule.AggregateRoot.OutlierPlanActor](
        AnalysisPlanModule.AggregateRoot.OutlierPlanActor.props( model, module.rootType )
      )

      planRef.underlyingActor.state = plan
      val actual = planRef.underlyingActor.proxyFor( "test" )
      actual must not be null
      val a2 = planRef.underlyingActor.proxyFor( "test" )
      actual must be theSameInstanceAs a2
    }

    "dead proxy must be cleared" taggedAs WIP in { f: Fixture =>
      import f._

      entity ! EntityMessages.Add( tid, Some(plan) )
      bus.expectMsgType[EntityMessages.Added]
      proxiesFrom( entity, tid ) mustBe empty

      entity ! P.AcceptTimeSeries( tid, Set.empty[WorkId], TimeSeries( "test" ) )
      val p1 = proxiesFrom( entity, tid )
      p1.keys must contain ( "test".toTopic )

      val proxy = p1( "test" )
      proxy ! PoisonPill
      logger.debug( "TEST: waiting a bit for Terminated to propagate..." )
      Thread.sleep( 5000 )
      logger.info( "TEST: checking proxy..." )

      val p2 = proxiesFrom( entity, tid )
      logger.debug( "TEST: p2 = [{}]", p2.mkString(", ") )
      p2 mustBe empty

      entity ! P.AcceptTimeSeries( tid, Set.empty[WorkId], TimeSeries( "test" ) )
      val p3 = proxiesFrom( entity, tid )
      p3.keys must contain ( "test".toTopic )
    }

    "handle workflow" taggedAs( WORKFLOW, WIP ) in { f: Fixture =>
      import f._
      logger.debug( "TEST: fixture class: [{}]", f.getClass )

      entity !+ EntityMessages.Add( tid, Some(plan) )

      proxiesFrom( entity, tid ) mustBe empty

      val wid1 = Set( WorkId() )
      entity ! P.AcceptTimeSeries( tid, wid1, TimeSeries( "test" ) )
      val p1 = proxiesFrom( entity, tid )
      p1.keys must contain ( "test".toTopic )

      f.asInstanceOf[WorkflowFixture].proxyProbes must have size 1
      val testProbe = f.asInstanceOf[WorkflowFixture].proxyProbes( "test".toTopic )
      testProbe.expectMsgPF( hint = "accept test time series" ) {
        case Envelope( P.AcceptTimeSeries(pid, cids, ts: TimeSeries, s), _ ) => {
          pid mustBe tid
          cids mustBe wid1
          s.value.plan mustBe f.plan.name
          s.value.topic mustBe "test".toTopic
          ts.topic mustBe "test".toTopic
        }
      }

      val wid2 = wid1 + WorkId()
      entity ! P.AcceptTimeSeries( tid, wid2, TimeSeries( "test" ) )
      val p2 = proxiesFrom( entity, tid )
      p2.keys must contain ( "test".toTopic )
      f.asInstanceOf[WorkflowFixture].proxyProbes must have size 1
      testProbe.expectMsgPF( hint = "accept test time series" ) {
        case Envelope( P.AcceptTimeSeries(pid, cids, ts: TimeSeries, s), _ ) => {
          pid mustBe tid
          cids mustBe wid2
          s.value.plan mustBe f.plan.name
          s.value.topic mustBe "test".toTopic
          ts.topic mustBe "test".toTopic
        }
      }

      val wid3 = wid2 + WorkId()
      entity ! P.AcceptTimeSeries( tid, wid3, TimeSeries( "foo" ) )
      val p3 = proxiesFrom( entity, tid )
      p3.keys must contain allOf ( "test".toTopic, "foo".toTopic )
      f.asInstanceOf[WorkflowFixture].proxyProbes must have size 2
      val fooProbe = f.asInstanceOf[WorkflowFixture].proxyProbes( "foo".toTopic )
      fooProbe.expectMsgPF( hint = "accept foo time series" ) {
        case Envelope( P.AcceptTimeSeries(pid, cids, ts: TimeSeries, s), _ ) => {
          pid mustBe tid
          cids mustBe wid3
          s.value.plan mustBe f.plan.name
          s.value.topic mustBe "foo".toTopic
          ts.topic mustBe "foo".toTopic
        }
      }
      testProbe.expectNoMsg()
    }

    "add OutlierPlan" in { f: Fixture =>
      import f._

      entity ! EntityMessages.Add( tid, Some(plan) )
      bus.expectMsgPF( max = 5.seconds.dilated, hint = "add plan" ) {
        case p: EntityMessages.Added => {
          logger.info( "ADD PLAN: p.sourceId[{}]=[{}]   id[{}]=[{}]", p.sourceId.getClass.getCanonicalName, p.sourceId, tid.getClass.getCanonicalName, tid)
          p.sourceId mustBe plan.id
          assert( p.info.isDefined )
          p.info.get mustBe an [OutlierPlan]
          val actual = p.info.get.asInstanceOf[OutlierPlan]
          actual.name mustBe "TestPlan"
          actual.algorithms mustBe Set( algo )
        }
      }
    }

    "must not respond before add" in { f: Fixture =>
      import f._
      val info = ( entity ?+ P.UseAlgorithms( tid, Set( 'foo, 'bar ), ConfigFactory.empty() ) ).mapTo[P.PlanInfo]
      bus.expectNoMsg()
    }

    "change appliesTo" in { f: Fixture =>
      import f._

      entity !+ EntityMessages.Add( tid, Some(plan) )
      bus.expectMsgType[EntityMessages.Added]

      //anonymous partial functions extend Serialiazble
      val extract: OutlierPlan.ExtractTopic = {
        case m => Some("TestTopic")
      }

      val testApplies: OutlierPlan.AppliesTo = OutlierPlan.AppliesTo.topics( Set("foo", "bar"), extract )

      entity !+ AnalysisPlanProtocol.ApplyTo( tid, testApplies )
      bus.expectMsgPF( max = 3.seconds.dilated, hint = "applies to" ) {
        case AnalysisPlanProtocol.ScopeChanged(id, app) => {
          id mustBe tid
          app must be theSameInstanceAs testApplies
        }
      }

      val actual = stateFrom( entity, tid )
      actual mustBe plan
      actual.appliesTo must be theSameInstanceAs testApplies
    }

    "change algorithms" in { f: Fixture =>
      import f._

      entity !+ EntityMessages.Add( tid, Some(plan) )
      bus.expectMsgType[EntityMessages.Added]

      val testConfig = ConfigFactory.parseString(
        """
          |foo=bar
          |zed=gerry
        """.stripMargin
      )

      entity !+ AnalysisPlanProtocol.UseAlgorithms( tid, Set('foo, 'bar, 'zed), testConfig )
      bus.expectMsgPF( max = 3.seconds.dilated, hint = "use algorithms" ) {
        case AnalysisPlanProtocol.AlgorithmsChanged(id, algos, config) => {
          id mustBe tid
          algos mustBe Set('foo, 'bar, 'zed)
          config mustBe testConfig
        }
      }

      val actual = stateFrom( entity, tid )
      actual mustBe plan
      actual.algorithms mustBe Set('foo, 'bar, 'zed)
      actual.algorithmConfig mustBe testConfig
    }

    "change resolveVia" in { f: Fixture =>
      import f._

      entity !+ EntityMessages.Add( tid, Some(plan) )
      bus.expectMsgType[EntityMessages.Added]

      //anonymous partial functions extend Serialiazble
      val reduce: ReduceOutliers = new ReduceOutliers {
        import scalaz._
        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: OutlierPlan
        ): V[Outliers] =  {
          Validation.failureNel[Throwable, Outliers]( new IllegalStateException( "dummy" ) ).disjunction
        }
      }

      val isq: IsQuorum = new IsQuorum {
        override def totalIssued: Int = 3
        override def apply(results: OutlierAlgorithmResults): Boolean = true
      }


      entity !+ AnalysisPlanProtocol.ResolveVia( tid, isq, reduce )
      bus.expectMsgPF( max = 3.seconds.dilated, hint = "resolve via" ) {
        case AnalysisPlanProtocol.AnalysisResolutionChanged(id, i, r) => {
          id mustBe tid
          i must be theSameInstanceAs isq
          r must be theSameInstanceAs reduce
        }
      }

      val actual = stateFrom( entity, tid )
      actual mustBe plan
      actual.isQuorum must be theSameInstanceAs isq
      actual.reduce must be theSameInstanceAs reduce
    }

    "accept time series" in { f: Fixture =>
      val df = f.asInstanceOf[DefaultFixture]
      import df._

      import demesne.module.entity.{ messages => EntityMessages }

      val p = makePlan( "TestPlan", None )
      val t = Topic( "test-topic" )
      val planTid = p.id
      entity !+ EntityMessages.Add( p.id, Some(p) )

      val flatline = makeDataPoints( values = Seq.fill( 5 ){ 1.0 }, timeWiggle = (0.97, 1.03) )
      val series = spike( t, flatline, 1000 )()

      val accepted = entity ?+ P.AcceptTimeSeries( planTid, Set.empty[WorkId], series )
//todo removed since unnecessary
//      whenReady( accepted ) { actual =>
//        actual match {
//          case Envelope( a, _ ) => a mustBe P.Accepted
//          case a => a mustBe P.Accepted
//        }
//      }
      proxyProbe.expectMsgPF( 1.second.dilated, "proxy received" ) {
        case Envelope( P.AcceptTimeSeries( pid, cids, ts, scope), _ ) => {
          pid mustBe planTid
          cids mustBe empty
          ts mustBe series
          scope.value mustBe OutlierPlan.Scope( p, t )
        }

        case P.AcceptTimeSeries( pid, cids, ts, scope) => {
          pid mustBe planTid
          cids mustBe empty
          ts mustBe series
          scope.value mustBe OutlierPlan.Scope( p, t )
        }
      }
    }
  }
}

object AnalysisPlanModuleSpec {
  def config( systemName: String ): Config = {
    val planConfig: Config = ConfigFactory.parseString(
      """
        |in-flight-dispatcher {
        |  type = Dispatcher
        |  executor = "fork-join-executor"
        |  fork-join-executor {
        |#    # Min number of threads to cap factor-based parallelism number to
        |#    parallelism-min = 2
        |#    # Parallelism (threads) ... ceil(available processors * factor)
        |#    parallelism-factor = 2.0
        |#    # Max number of threads to cap factor-based parallelism number to
        |#    parallelism-max = 10
        |  }
        |  # Throughput defines the maximum number of messages to be
        |  # processed per actor before the thread jumps to the next actor.
        |  # Set to 1 for as fair as possible.
        |#  throughput = 100
        |}
      """.stripMargin
    )

    planConfig withFallback spotlight.testkit.config( "core", systemName )
  }
}