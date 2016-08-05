package spotlight.analysis.outlier

import scala.concurrent.duration._
import akka.pattern.ask
import akka.testkit._
import org.mockito.Mockito._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.ScalaFutures
import peds.archetype.domain.model.core.EntityIdentifying
import peds.akka.envelope.Envelope
import demesne.AggregateRootModule
import demesne.module.entity.EntityAggregateModule
import demesne.module.entity.{messages => EntityMessages}
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.outlier.{CatalogProtocol => P}


/**
  * Created by rolfsd on 8/1/16.
  */
class CatalogModuleSpec extends EntityModuleSpec[Catalog] with ScalaFutures { outer =>
  override type Protocol = CatalogProtocol.type
  override val protocol: Protocol = CatalogProtocol

  class Fixture extends EntityFixture( config = CatalogModuleSpec.config ) { fixture =>
    override type Module = EntityAggregateModule[Catalog]
    override val module: Module = CatalogModule
    override def moduleCompanions: List[AggregateRootModule] = List( AnalysisPlanModule.module, module )

    override val identifying: EntityIdentifying[Catalog] = Catalog.identifying

    val catalog = Catalog( id = fixture.tid, name = "TestCatalog", slug = "test-catalog" )

//    override def context: Map[Symbol, Any] = {
//      val base = super.context
//      base + de
//    }
  }

  override def createAkkaFixture( tags: OneArgTest ): Fixture = new Fixture


  "CatalogModule" should {
    "add Catalog" in { f: Fixture =>
      import f.{ timeout => _, _ }
      implicit val to = f.timeout

      logger.info( "TEST: entity-type=[{}] tid=[{}]", identifying.evEntity, tid )

      import scala.concurrent.ExecutionContext.Implicits.global
      
      entity ! EntityMessages.Add( tid, Some(catalog) )
      whenReady(
        ( entity ? P.GetPlansForTopic(tid, "foo") ).mapTo[Envelope].map(_.payload).mapTo[P.CatalogedPlans],
        timeout(15.seconds.dilated)
      ) { actual =>
        actual.plans.size mustBe 1
        val actualPlan = actual.plans.head
        actualPlan.name mustBe "foo"
      }
    }
  }
}

object CatalogModuleSpec extends StrictLogging {
  val config: Config = {
    val catalogConfig: Config = ConfigFactory.parseString(
      """
        |spotlight {
        |  detection-plans {
        |#    default = ${spotlight.dbscan-plan} { is-default: on }
        |#    bar = ${spotlight.dbscan-plan} { topics: [bar] }
        |    foo = ${spotlight.dbscan-plan} { topics: [foo] }
        |#    foo-bar = ${spotlight.dbscan-plan} { topics: [zed, bar] }
        |  }
        |
        |  dbscan-plan = {
        |    timeout: 100ms
        |    algorithms: [dbscan]
        |    algorithm-config {
        |      dbscan {
        |        tolerance: 3
        |        seedEps: 5
        |        minDensityConnectedPoints: 3
        |#        distance: Euclidean
        |      }
        |    }
        |  }
        |
        |  workflow {
        |    buffer: 1000
        |    detect {
        |      timeout: 10s
        |      max-in-flight-cpu-factor: 1
        |    }
        |  }
        |}
      """.stripMargin
    )

    val result: Config = catalogConfig.resolve() withFallback spotlight.testkit.config( "core" )
    logger.info(
      "TEST CONFIGURATION:[\n{}\n]",
      result.root().render( com.typesafe.config.ConfigRenderOptions.defaults().setFormatted(true))
    )
    result
  }
}
