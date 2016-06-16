package spotlight.analysis.outlier

import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.ConfigFactory
import peds.akka.envelope._
import spotlight.analysis.outlier.algorithm.skyline.SimpleMovingAverageModule
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.testkit.EntityModuleSpec


/**
  * Created by rolfsd on 6/15/16.
  */
class AnalysisPlanModuleSpec extends EntityModuleSpec[OutlierPlan] {
  override type Module = AnalysisPlanModule.AggregateRoot.module.type
  override val module: Module = AnalysisPlanModule.AggregateRoot.module
  class Fixture extends EntityFixture {
    override def nextId(): module.TID = OutlierPlan.nextId()

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
      import AnalysisPlanModule.AggregateRoot.{ Protocol => P }

      val planInfo = makePlan("TestPlan", None)
      entity ! P.Entity.Add( planInfo )
      bus.expectMsgPF( max = 400.millis.dilated, hint = "add plan" ) {
        case p: P.Entity.Added => {
          logger.info( "ADD PLAN: p.sourceId[{}]=[{}]   id[{}]=[{}]", p.sourceId.getClass.getCanonicalName, p.sourceId, id.getClass.getCanonicalName, id)
          p.sourceId mustBe planInfo.id
          p.info.name mustBe "TestPlan"
          p.info.algorithms mustBe Set( algo )
        }
      }
    }

    "must not respond before add" taggedAs WIP in { f: Fixture =>
      import f._
      import AnalysisPlanModule.AggregateRoot.{ Protocol => P }
      import peds.akka.envelope.pattern.ask

      implicit val to = akka.util.Timeout( 2.seconds )
      val info = ( entity ?+ P.UseAlgorithms( id, Set( 'foo, 'bar ), ConfigFactory.empty() ) ).mapTo[P.PlanInfo]
      bus.expectNoMsg()
    }

  }
}
