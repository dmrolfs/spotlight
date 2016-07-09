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
import demesne.module.entity.{EntityAggregateModule, messages => EntityMessages}
import demesne.DomainModel.DomainModelImpl
import demesne.module.AggregateRootProps
import demesne.module.entity.EntityAggregateModule.MakeIndexSpec
import demesne.register.AggregateIndexSpec
import peds.commons.log.Trace
import shapeless.Lens
import spotlight.analysis.outlier.algorithm.skyline.SimpleMovingAverageModule
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.outlier.{AnalysisPlanProtocol => P}

import scala.reflect.ClassTag


/**
  * Created by rolfsd on 6/15/16.
  */
class AnalysisPlanModuleSpec extends EntityModuleSpec[OutlierPlan] {
  override type Module = AnalysisPlanModule.AggregateRoot.module.type
  override val module: Module = AnalysisPlanModule.AggregateRoot.module

  override type ID = AnalysisPlanModule.AggregateRoot.module.ID
  override type Protocol = AnalysisPlanProtocol.type
  override val protocol: Protocol = AnalysisPlanProtocol

  class Fixture extends EntityFixture( config = AnalysisPlanModuleSpec.config ) {
    val identifying = AnalysisPlanModule.identifying
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
      bus.expectMsgPF( max = 5.seconds.dilated, hint = "add plan" ) {
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

  }
}

object AnalysisPlanModuleSpec {
  val config: Config = {
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

    planConfig withFallback spotlight.testkit.config
  }
}