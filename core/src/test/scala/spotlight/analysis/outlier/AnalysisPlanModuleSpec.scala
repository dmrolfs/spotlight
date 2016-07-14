package spotlight.analysis.outlier

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import akka.actor.{ActorRef, PoisonPill}
import akka.util.Timeout
import demesne.module.entity.{EntityAggregateModule, messages => EntityMessages}
import peds.commons.V
import spotlight.analysis.outlier.algorithm.skyline.SimpleMovingAverageModule
import spotlight.model.outlier._
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.outlier.{AnalysisPlanProtocol => P}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/15/16.
  */
class AnalysisPlanModuleSpec extends EntityModuleSpec[OutlierPlan] { outer =>
  override type ID = AnalysisPlanModule.AggregateRoot.module.ID
  override type Protocol = AnalysisPlanProtocol.type
  override val protocol: Protocol = AnalysisPlanProtocol

  val standardModule: EntityAggregateModule[OutlierPlan] = AnalysisPlanModule.AggregateRoot.module

  class Fixture extends EntityFixture( config = AnalysisPlanModuleSpec.config ) {
    override type Module = EntityAggregateModule[OutlierPlan]
    override val module: Module = outer.standardModule

    val identifying = AnalysisPlanModule.identifying
    override def nextId(): module.TID = identifying.safeNextId
    lazy val plan = makePlan( "TestPlan", None )
    override lazy val tid: TID = plan.id

    lazy val algo: Symbol = SimpleMovingAverageModule.algorithm.label

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

  override def createAkkaFixture( test: OneArgTest ): Fixture = new Fixture

  s"${standardModule.rootType.name}" should {
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

      entity ! P.AcceptTimeSeries( tid, TimeSeries( "test" ) )
      val p1 = proxiesFrom( entity, tid )
      p1.keys must contain ( "test".toTopic )

      val proxy = p1( "test" )
      proxy ! PoisonPill
      logger.info( "TEST: waiting a bit for Terminated to propagate..." )
      Thread.sleep( 1000 )
      logger.info( "TEST: checking proxy..." )

      val p2 = proxiesFrom( entity, tid )
      logger.info( "TEST: p2 = [{}]", p2.mkString(", ") )
      p2 mustBe empty

      entity ! P.AcceptTimeSeries( tid, TimeSeries( "test" ) )
      val p3 = proxiesFrom( entity, tid )
      p3.keys must contain ( "test".toTopic )
    }

    "handle workflow" in { f: Fixture =>
      import f._
      entity !+ EntityMessages.Add( tid, Some(plan) )
      bus.expectMsgType[EntityMessages.Added]

      proxiesFrom( entity, tid ) mustBe empty

      entity ! P.AcceptTimeSeries( tid, TimeSeries( "test" ) )
      val p1 = proxiesFrom( entity, tid )
      p1.keys must contain ( "test".toTopic )

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

      implicit val to = Timeout( 2.seconds )
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
  }
}

object AnalysisPlanModuleSpec {
  def config: Config = {
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

    planConfig withFallback spotlight.testkit.config( "core" )
  }
}