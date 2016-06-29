package spotlight.analysis.outlier

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import demesne.{AggregateRootType, DomainModel, PassivationSpecification}
import demesne.module.entity.{messages => EntityMessage}
import demesne.DomainModel.DomainModelImpl
import spotlight.analysis.outlier.algorithm.skyline.SimpleMovingAverageModule
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.outlier.{ AnalysisPlanProtocol => P }
import demesne.module.entity.{ messages => EntityMessages }


/**
  * Created by rolfsd on 6/15/16.
  */
class AnalysisPlanModuleSpec extends EntityModuleSpec[OutlierPlan] {
  override type Module = AnalysisPlanModule.AggregateRoot.module.type
  override val module: Module = AnalysisPlanModule.AggregateRoot.module
  class Fixture extends EntityFixture( config = AnalysisPlanModuleSpec.config ) {
    val identifying = AnalysisPlanModule.analysisPlanIdentifying
    override def nextId(): module.TID = identifying.safeNextId

    val algo: Symbol = SimpleMovingAverageModule.algorithm.label

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
  }

  override def createAkkaFixture(): Fixture = new Fixture

  s"${module.rootType.name}" should {
    "add OutlierPlan" in { f: Fixture =>
      import f._

      val planInfo = makePlan("TestPlan", None)
      entity ! EntityMessages.Add( planInfo.id, Some(planInfo) )
      bus.expectMsgPF( max = 1.millis.dilated, hint = "add plan" ) {
        case p: EntityMessages.Added => {
          logger.info( "ADD PLAN: p.sourceId[{}]=[{}]   id[{}]=[{}]", p.sourceId.getClass.getCanonicalName, p.sourceId, tid.getClass.getCanonicalName, tid)
          p.sourceId mustBe planInfo.id
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

      implicit val to = Timeout( 2.seconds )
      val info = ( entity ?+ P.UseAlgorithms( tid, Set( 'foo, 'bar ), ConfigFactory.empty() ) ).mapTo[P.PlanInfo]
      bus.expectNoMsg()
    }

    "recover and continue after passivation" taggedAs WIP in { f: Fixture =>
      import f._

      def infoFrom( ar: ActorRef ): OutlierPlan = {
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val to = Timeout( 2.seconds )
        Await.result(
          ( ar ?+ P.GetInfo(tid) ).mapTo[Envelope].map{ _.payload }.mapTo[P.PlanInfo].map{ _.info },
          2.seconds
        )
      }

      val rootType = new AggregateRootType {
        override def name: String = module.shardName
        override def aggregateRootProps( implicit model: DomainModel ): Props = module.aggregateRootPropsOp( model, this )
        override val toString: String = "AlteredAnalysisPlanAggregateRootType"

        override def passivation: PassivationSpecification = new PassivationSpecification {
          override def inactivityTimeout: Duration = 100.millis
        }
//        override def snapshot: SnapshotSpecification = super.snapshot
      }

      val aggregateRegistry = model.asInstanceOf[DomainModelImpl].aggregateRegistry

      val change = aggregateRegistry alter { r =>
        val (ref, oldRootType) = r( module.shardName )
        r + ( module.shardName -> (ref, rootType) )
      }
      Await.ready( change, 2.seconds )
      logger.info( "TEST:aggregateRegistry = [{}]", aggregateRegistry.get().mkString(","))

      val p1 = makePlan( "TestPlan", None )
      entity ! EntityMessage.Add( p1.id, Some(p1) )
      bus.expectMsgClass( classOf[EntityMessage.Added] )

      logger.info( "TEST:SLEEPING...")
      Thread.sleep( 30000 )
      logger.info( "TEST:AWAKE...")

      infoFrom( entity ) mustBe p1

      entity ! P.UseAlgorithms( tid, Set( 'stella, 'otis, 'apollo ), ConfigFactory.empty() )
      bus.expectMsgPF( 1.second, "change algos" ) {
        case Envelope( P.AlgorithmsChanged(pid, algos, c), _ ) => {
          pid mustBe tid
          algos mustBe Set( 'stella, 'otis, 'apollo )
          assert( c.isEmpty )
        }
      }
    }
  }
}

object AnalysisPlanModuleSpec {
  val config: Config = ConfigFactory.parseString(
    """
      |akka.loggers = ["akka.testkit.TestEventListener"]
      |
      |akka.persistence {
      |#  journal.plugin = "akka.persistence.journal.leveldb-shared"
      |  journal.plugin = "akka.persistence.journal.leveldb"
      |  journal.leveldb-shared.store {
      |    # DO NOT USE 'native = off' IN PRODUCTION !!!
      |    native = off
      |    dir = "target/shared-journal"
      |  }
      |  journal.leveldb {
      |    # DO NOT USE 'native = off' IN PRODUCTION !!!
      |    native = off
      |    dir = "target/journal"
      |  }
      |  snapshot-store.local.dir = "target/snapshots"
      |}
      |
      |#akka {
      |#  persistence {
      |#    journal.plugin = "inmemory-journal"
      |#    snapshot-store.plugin = "inmemory-snapshot-store"
      |#
      |#    journal.plugin = "akka.persistence.journal.leveldb"
      |#    journal.leveldb.dir = "target/journal"
      |#    journal.leveldb.native = off
      |#    snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      |#    snapshot-store.local.dir = "target/snapshots"
      |#  }
      |#}
      |
      |akka {
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  logging-filter = "akka.event.DefaultLoggingFilter"
      |  loglevel = DEBUG
      |  stdout-loglevel = "DEBUG"
      |  log-dead-letters = on
      |  log-dead-letters-during-shutdown = on
      |
      |  actor {
      |    provider = "akka.cluster.ClusterActorRefProvider"
      |  }
      |
      |  remote {
      |    log-remote-lifecycle-events = off
      |    netty.tcp {
      |      hostname = "127.0.0.1"
      |      port = 0
      |    }
      |  }
      |
      |  cluster {
      |    seed-nodes = [
      |      "akka.tcp://ClusterSystem@127.0.0.1:2551",
      |      "akka.tcp://ClusterSystem@127.0.0.1:2552"
      |    ]
      |
      |    auto-down-unreachable-after = 10s
      |  }
      |}
      |
      |akka.actor.debug {
      |  # enable function of Actor.loggable(), which is to log any received message
      |  # at DEBUG level, see the “Testing Actor Systems” section of the Akka
      |  # Documentation at http://akka.io/docs
      |  receive = on
      |
      |  # enable DEBUG logging of all AutoReceiveMessages (Kill, PoisonPill et.c.)
      |  autoreceive = on
      |
      |  # enable DEBUG logging of actor lifecycle changes
      |  lifecycle = on
      |
      |  # enable DEBUG logging of all LoggingFSMs for events, transitions and timers
      |  fsm = on
      |
      |  # enable DEBUG logging of subscription changes on the eventStream
      |  event-stream = on
      |
      |  # enable DEBUG logging of unhandled messages
      |  unhandled = on
      |
      |  # enable WARN logging of misconfigured routers
      |  router-misconfiguration = on
      |}
      |
      |demesne.register-dispatcher {
      |  type = Dispatcher
      |  executor = "fork-join-executor"
      |  fork-join-executor {
      |    # Min number of threads to cap factor-based parallelism number to
      |    parallelism-min = 2
      |    # Parallelism (threads) ... ceil(available processors * factor)
      |    parallelism-factor = 2.0
      |    # Max number of threads to cap factor-based parallelism number to
      |    parallelism-max = 10
      |  }
      |  # Throughput defines the maximum number of messages to be
      |  # processed per actor before the thread jumps to the next actor.
      |  # Set to 1 for as fair as possible.
      |  throughput = 100
      |}
    """.stripMargin
  )
}