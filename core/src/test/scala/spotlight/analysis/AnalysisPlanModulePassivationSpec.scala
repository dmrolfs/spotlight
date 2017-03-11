package spotlight.analysis

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.{ Config, ConfigFactory }
import omnibus.akka.envelope._
import omnibus.akka.envelope.pattern.ask
import akka.actor.{ ActorRef, ActorSystem, Props }
import com.typesafe.scalalogging.StrictLogging
import demesne.{ AggregateRootType, DomainModel }
import demesne.module.{ AggregateEnvironment, LocalAggregate }
import demesne.module.entity.EntityAggregateModule
import demesne.repository.AggregateRootProps
import omnibus.akka.publish.StackableStreamPublisher
import omnibus.archetype.domain.model.core.EntityIdentifying
import omnibus.commons.TryV
import omnibus.commons.identifier.Identifying
import omnibus.commons.log.Trace
import shapeless.Lens
import spotlight.analysis.AnalysisPlanModule.AggregateRoot.PlanActor
import spotlight.analysis.AnalysisPlanModule.AggregateRoot.PlanActor.{ FlowConfigurationProvider, WorkerProvider }
import spotlight.analysis.algorithm.statistical.SimpleMovingAverageAlgorithm
import spotlight.model.outlier.{ AnalysisPlan, IsQuorum, ReduceOutliers }
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.{ AnalysisPlanProtocol ⇒ P }

/** Created by rolfsd on 6/15/16.
  */
class AnalysisPlanModulePassivationSpec extends EntityModuleSpec[AnalysisPlanState] { outer ⇒

  val trace = Trace[AnalysisPlanModulePassivationSpec]

  override type Protocol = AnalysisPlanProtocol.type
  override val protocol: Protocol = AnalysisPlanProtocol

  implicit val moduleIdentifying: Identifying.Aux[AnalysisPlanState, AnalysisPlanState#ID] = AnalysisPlanModule.identifying

  override def testConfiguration( test: OneArgTest, slug: String ): Config = {
    AnalysisPlanModulePassivationSpec.config( systemName = slug )
  }

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends EntityFixture( _config, _system, _slug ) {
    class Module extends EntityAggregateModule[AnalysisPlanState] { testModule ⇒
      private val trace: Trace[_] = Trace[Module]

      override def passivateTimeout: Duration = Duration( 2, SECONDS )
      override def snapshotPeriod: Option[FiniteDuration] = Some( 1.second )

      override val idLens: Lens[AnalysisPlanState, AnalysisPlanState#TID] = AnalysisPlanModule.idLens
      override val nameLens: Lens[AnalysisPlanState, String] = AnalysisPlanModule.nameLens
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
        with StackableStreamPublisher {
      override val bufferSize: Int = 10
    }

    override val identifying: EntityIdentifying[AnalysisPlanState] = AnalysisPlanModule.identifying
    override def nextId(): module.TID = TryV.unsafeGet( identifying.nextTID )

    val algo: String = SimpleMovingAverageAlgorithm.label

    val emptyConfig = ConfigFactory.empty()

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
        reduce = ReduceOutliers.byCorroborationPercentage( 50 )
      )
    }

    def stateFrom( ar: ActorRef, tid: module.TID ): AnalysisPlan = trace.block( s"stateFrom($ar)" ) {
      import scala.concurrent.ExecutionContext.Implicits.global
      import akka.pattern.ask
      Await.result(
        ( ar ? P.GetPlan( tid ) ).mapTo[Envelope].map { _.payload }.mapTo[P.PlanInfo].map { _.info },
        2.seconds.dilated
      )
    }
  }

  "AnalysisPlanModule" should {
    "add AnalysisPlan" in { f: Fixture ⇒
      import f._

      val planInfo = makePlan( "TestPlan", None )
      entity ! protocol.Add( planInfo.id, Some( planInfo ) )
      bus.expectMsgPF( max = 5.seconds.dilated, hint = "add plan" ) {
        case p: protocol.Added ⇒ {
          logger.info( "ADD PLAN: p.sourceId[{}]=[{}]   id[{}]=[{}]", p.sourceId.getClass.getCanonicalName, p.sourceId, tid.getClass.getCanonicalName, tid )
          p.sourceId mustBe planInfo.id
          assert( p.info.isDefined )
          p.info.get mustBe an[AnalysisPlan]
          val actual = p.info.get.asInstanceOf[AnalysisPlan]
          actual.name mustBe "TestPlan"
          actual.algorithmKeys mustBe Set( algo )
        }
      }
    }

    "must not respond before add" in { f: Fixture ⇒
      import f._
      val info = ( entity ?+ P.UseAlgorithms( tid, Map( "foo" → emptyConfig, "bar" → emptyConfig ) ) ).mapTo[P.PlanInfo]
      bus.expectNoMsg()
    }

    "recover and continue after passivation" in { f: Fixture ⇒
      import f._

      val p1 = makePlan( "TestPlan", None )
      val planTid = p1.id
      entity ! protocol.Add( p1.id, Some( p1 ) )

      stateFrom( entity, planTid ) mustBe p1

      logger.info( "TEST:SLEEPING..." )
      Thread.sleep( 10000 )
      logger.info( "TEST:AWAKE..." )

      bus.expectMsgClass( classOf[protocol.Added] )

      stateFrom( entity, planTid ) mustBe p1

      entity !+ P.UseAlgorithms( planTid, Map( "stella" → emptyConfig, "otis" → emptyConfig, "apollo" → emptyConfig ) )
      bus.expectMsgPF( 1.second.dilated, "change algos" ) {
        case P.AlgorithmsChanged( pid, algos, added, dropped ) ⇒ {
          pid mustBe planTid
          algos.keySet mustBe Set( "stella", "otis", "apollo" )
          added mustBe Set( "stella", "otis", "apollo" )
          dropped mustBe Set( SimpleMovingAverageAlgorithm.label )
          assert( algos forall { _._2.isEmpty } )
        }
      }
    }

    "take and recover from snapshots" taggedAs WIP in { f: Fixture ⇒
      import f._

      val p1 = makePlan( "TestPlan", None )
      entity ! protocol.Add( p1.id, Some( p1 ) )
      countDown await 3.seconds

      bus.expectMsgClass( classOf[protocol.Added] )
      logger.info( "TEST:looking at entity..." )
      stateFrom( entity, p1.id ) mustBe p1

      logger.info( "TEST:taking Snapshot..." )
      module.rootType.snapshot foreach { ss ⇒
        logger.warn( "commanding entity:[{}] to save snapshot: [{}]", entity.path.name, ss.saveSnapshotCommand( p1.id ).toString )
        entity !+ ss.saveSnapshotCommand( p1.id )
      }
      //      countDown await 1.second

      //      EventFilter.debug( pattern = """Using serializer \[.+\] for message \[demesne.module.entity.EntityProtocol$Added\]""" ) intercept {
      //        intercept {
      //          logger.warn( "In snapshot intercept..." )
      //          module.rootType.snapshot foreach { ss ⇒
      //            logger.warn( "commanding entity:[{}] to save snapshot: [{}]", entity.path.name, ss.saveSnapshotCommand( p1.id ).toString )
      //            entity !+ ss.saveSnapshotCommand( p1.id )
      //          }
      //        }
      //      }
      //      EventFilter.debug( start = "aggregate snapshot successfully saved:", occurrences = 1 ) intercept {
      //      EventFilter.debug( start = s"saving snapshot for pid:[${p1.id}]", occurrences = 1 ) intercept {
      //        logger.warn( "In snapshot intercept..." )
      //        module.rootType.snapshot foreach { ss ⇒
      //          logger.warn( "commanding entity:[{}] to save snapshot: [{}]", entity.path.name, ss.saveSnapshotCommand( p1.id ).toString )
      //          entity !+ ss.saveSnapshotCommand( p1.id )
      //        }
      //      }

      //      countDown await 1.second
      logger.info( "TEST: setting algorithms..." )
      entity !+ P.UseAlgorithms( p1.id, Map( "stella" → emptyConfig, "otis" → emptyConfig, "apollo" → emptyConfig ) )
      bus.expectMsgPF( 3.seconds.dilated, "change algos" ) {
        case P.AlgorithmsChanged( pid, algos, added, dropped ) ⇒ {
          pid mustBe p1.id
          algos.keySet mustBe Set( "stella", "otis", "apollo" )
          added mustBe Set( "stella", "otis", "apollo" )
          dropped mustBe Set( SimpleMovingAverageAlgorithm.label )
          assert( algos forall { _._2.isEmpty } )
        }
      }

      Thread.sleep( 3000 )
      logger.info( "TEST:Snapshot done..." )

      bus.expectNoMsg()

      stateFrom( entity, p1.id ) mustBe p1
    }
  }
}

object AnalysisPlanModulePassivationSpec extends StrictLogging {
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

    val c = planConfig withFallback spotlight.testkit.config( "core", systemName )
    logger.warn( "#TEST making config... akka.actor.kryo[{}]:[\n{}\n]", c.hasPath( "akka.actor.kryo" ).toString, c.getConfig( "akka.actor.kryo" ).root.render() )
    c
  }
}