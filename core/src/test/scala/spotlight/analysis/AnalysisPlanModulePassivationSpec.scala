package spotlight.analysis

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import akka.actor.{ActorRef, ActorSystem, Props}
import demesne.index.StackableIndexBusPublisher
import demesne.{AggregateRootType, DomainModel}
import demesne.module.{AggregateEnvironment, LocalAggregate}
import demesne.module.entity.{EntityAggregateModule, EntityProtocol}
import demesne.repository.AggregateRootProps
import peds.akka.publish.StackableStreamPublisher
import peds.archetype.domain.model.core.EntityIdentifying
import peds.commons.identifier.{ShortUUID, TaggedID}
import peds.commons.log.Trace
import shapeless.Lens
import spotlight.analysis.AnalysisPlanModule.AggregateRoot.PlanActor
import spotlight.analysis.AnalysisPlanModule.AggregateRoot.PlanActor.{FlowConfigurationProvider, WorkerProvider}
import spotlight.analysis.algorithm.statistical.SimpleMovingAverageAlgorithm
import spotlight.model.outlier.{IsQuorum, AnalysisPlan, ReduceOutliers}
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.{AnalysisPlanProtocol => P}


/**
  * Created by rolfsd on 6/15/16.
  */
class AnalysisPlanModulePassivationSpec extends EntityModuleSpec[AnalysisPlan] { outer =>

  val trace = Trace[AnalysisPlanModulePassivationSpec]

  override type ID = AnalysisPlan#ID
  override type Protocol = AnalysisPlanProtocol.type
  override val protocol: Protocol = AnalysisPlanProtocol


  override def testConfiguration( test: OneArgTest, slug: String ): Config = {
    AnalysisPlanModulePassivationSpec.config( systemName = slug )
  }

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends EntityFixture( _config, _system, _slug ) {
    class Module extends EntityAggregateModule[AnalysisPlan] { testModule =>
      private val trace: Trace[_] = Trace[Module]

      override def passivateTimeout: Duration = Duration( 2, SECONDS )
      override def snapshotPeriod: Option[FiniteDuration] = Some( 1.second )

      override val idLens: Lens[AnalysisPlan, TaggedID[ShortUUID]] = AnalysisPlan.idLens
      override val nameLens: Lens[AnalysisPlan, String] = AnalysisPlan.nameLens
      override def aggregateRootPropsOp: AggregateRootProps = testProps( _, _ )
      override type Protocol = outer.Protocol
      override val protocol: Protocol = outer.protocol
      override def environment: AggregateEnvironment = LocalAggregate
    }

    override val module: Module = new Module

    def testProps( model: DomainModel, rootType: AggregateRootType ): Props = {
      Props( new TestAnalysisPlanModule( model, rootType ) )
    }

    val proxyProbe = TestProbe()

    private class TestAnalysisPlanModule( model: DomainModel, rootType: AggregateRootType )
    extends PlanActor( model, rootType )
    with WorkerProvider
    with FlowConfigurationProvider
    with StackableStreamPublisher
    with StackableIndexBusPublisher {
      override val bufferSize: Int = 10
    }


    override val identifying: EntityIdentifying[AnalysisPlan] = AnalysisPlanModule.identifying
    override def nextId(): module.TID = identifying.safeNextId

    val algo: Symbol = SimpleMovingAverageAlgorithm.algorithm.label

    def makePlan( name: String, g: Option[AnalysisPlan.Grouping] ): AnalysisPlan = {
      AnalysisPlan.default(
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

    def stateFrom( ar: ActorRef, tid: module.TID ): AnalysisPlan = trace.block( s"stateFrom($ar)" ) {
      import scala.concurrent.ExecutionContext.Implicits.global
      import akka.pattern.ask
      Await.result(
        ( ar ? P.GetPlan(tid) ).mapTo[Envelope].map{ _.payload }.mapTo[P.PlanInfo].map{ _.info },
        2.seconds.dilated
      )
    }
  }


  "AnalysisPlanModule" should {
    "add AnalysisPlan" in { f: Fixture =>
      import f._

      val planInfo = makePlan("TestPlan", None)
      entity ! protocol.Add( planInfo.id, Some(planInfo) )
      bus.expectMsgPF( max = 5.seconds.dilated, hint = "add plan" ) {
        case p: protocol.Added => {
          logger.info( "ADD PLAN: p.sourceId[{}]=[{}]   id[{}]=[{}]", p.sourceId.getClass.getCanonicalName, p.sourceId, tid.getClass.getCanonicalName, tid)
          p.sourceId mustBe planInfo.id
          assert( p.info.isDefined )
          p.info.get mustBe an [AnalysisPlan]
          val actual = p.info.get.asInstanceOf[AnalysisPlan]
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

    "recover and continue after passivation" in { f: Fixture =>
      import f._

      val p1 = makePlan( "TestPlan", None )
      val planTid = p1.id
      entity ! protocol.Add( p1.id, Some(p1) )

      stateFrom( entity, planTid ) mustBe p1

      logger.info( "TEST:SLEEPING..." )
      Thread.sleep( 10000 )
      logger.info( "TEST:AWAKE...")

      bus.expectMsgClass( classOf[protocol.Added] )

      stateFrom( entity, planTid ) mustBe p1

      entity !+ P.UseAlgorithms( planTid, Set( 'stella, 'otis, 'apollo ), ConfigFactory.empty() )
      bus.expectMsgPF( 1.second.dilated, "change algos" ) {
        case P.AlgorithmsChanged(pid, algos, c) => {
          pid mustBe planTid
          algos mustBe Set( 'stella, 'otis, 'apollo )
          assert( c.isEmpty )
        }
      }
    }

    "take and recover from snapshots" taggedAs WIP in { f: Fixture =>
      import f._

      val p1 = makePlan( "TestPlan", None )
      entity ! protocol.Add( p1.id, Some(p1) )
      bus.expectMsgClass( classOf[protocol.Added] )
      stateFrom( entity, p1.id ) mustBe p1

      logger.info( "TEST:taking Snapshot..." )

      EventFilter.debug( start = "aggregate snapshot successfully saved:", occurrences = 1 ) intercept {
        module.rootType.snapshot foreach { ss => entity !+ ss.saveSnapshotCommand(p1.id) }
      }

      entity !+ P.UseAlgorithms( p1.id, Set( 'stella, 'otis, 'apollo ), ConfigFactory.empty() )
      bus.expectMsgPF( 1.second.dilated, "change algos" ) {
        case P.AlgorithmsChanged(pid, algos, c) => {
          pid mustBe p1.id
          algos mustBe Set( 'stella, 'otis, 'apollo )
          assert( c.isEmpty )
        }
      }

      Thread.sleep( 3000 )
      logger.info( "TEST:Snapshot done...")

      bus.expectNoMsg()

      stateFrom( entity, p1.id ) mustBe p1
    }
  }
}

object AnalysisPlanModulePassivationSpec {
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