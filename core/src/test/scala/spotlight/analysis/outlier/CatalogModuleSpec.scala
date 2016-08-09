package spotlight.analysis.outlier


import akka.Done

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.actor.{ActorRef, Props}
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import demesne.module.AggregateRootProps
import org.scalatest.concurrent.ScalaFutures
import peds.archetype.domain.model.core.EntityIdentifying
import peds.akka.envelope.Envelope
import demesne.{AggregateRootModule, AggregateRootType, DomainModel}
import demesne.module.entity.EntityAggregateModule
import demesne.module.entity.{messages => EntityMessages}
import demesne.register.StackableRegisterBusPublisher
import org.scalatest.Tag
import peds.akka.publish.StackableStreamPublisher
import peds.commons.Valid
import peds.commons.log.Trace
import shapeless.Lens
import spotlight.analysis.outlier.CatalogProtocol.CatalogedPlans
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.outlier.{CatalogProtocol => P}
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.Topic


/**
  * Created by rolfsd on 8/1/16.
  */
class CatalogModuleSpec extends EntityModuleSpec[Catalog] with ScalaFutures { outer =>
  override type Protocol = CatalogProtocol.type
  override val protocol: Protocol = CatalogProtocol

  class Fixture( config: Config = CatalogModuleSpec.config ) extends EntityFixture( config = config ) { fixture =>
    override type Module = EntityAggregateModule[Catalog]
    override val module: Module = CatalogModule
    override def moduleCompanions: List[AggregateRootModule] = {
      val result = List( AnalysisPlanModule.module, fixture.module )
      logger.debug( "TEST: FIXTURE module-companions:[{}]", result.mkString(", ") )
      result
    }

    override val identifying: EntityIdentifying[Catalog] = Catalog.identifying

    val catalog = Catalog( id = fixture.tid, name = "TestCatalog", slug = "test-catalog" )

    def stateFrom( ar: ActorRef, tid: module.TID, topic: Topic ): Future[CatalogedPlans] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      import akka.pattern.ask
      ( ar ? P.GetPlansForTopic(tid, topic) ).mapTo[Envelope].map{ _.payload }.mapTo[P.CatalogedPlans]
    }
  }

  class PassivationFixture( config: Config = CatalogModuleSpec.config ) extends Fixture( config = config) { fixture =>
    class PassivationModule extends CatalogModule { testModule =>
      override def initializer(
        rootType: AggregateRootType,
        model: DomainModel,
        props: Map[Symbol, Any]
      )(
        implicit ec: ExecutionContext
      ): Valid[Future[Done]] = trace.block( "PASSIVATION-MODULE INITIALIZER" ) {
        super.initializer( rootType, model, props )
      }


      override def aggregateRootPropsOp: AggregateRootProps = {
        (model: DomainModel, rootType: AggregateRootType) => Props( new FixtureCatalogActor( model, rootType ) )
      }

      override def rootType: AggregateRootType = {
        new AggregateRootType {
          override def passivateTimeout: Duration = {
            val pto = 2.seconds
            logger.info( "passivateTimeout=[{}]", pto )
            pto
          }

          override def name: String = testModule.shardName

          override def aggregateRootProps(implicit model: DomainModel): Props = testModule.aggregateRootPropsOp( model, this )
        }
      }


      class FixtureCatalogActor( model: DomainModel, rootType: AggregateRootType )
      extends CatalogModule.CatalogActor( model, rootType )
      with CatalogModule.CatalogActor.Provider
      with StackableStreamPublisher
      with StackableRegisterBusPublisher {
        override def detectionBudget: FiniteDuration = testModule.detectionBudget
        override def specifiedPlans: Set[OutlierPlan] = testModule.specifiedPlans

        override def preActivate(): Unit = {
          log.info( "TEST: PASSIVATION CATALOG MODULE -- BEFORE ACTIVATION")
        }

        override def active: Receive = {
          case m if super.active isDefinedAt m => {
            log.debug(
              "TEST: IN PASSIVATION FIXTURE ACTOR!!!  detection-budget:[{}] specified-plans:[{}]",
              testModule.detectionBudget.toCoarsest,
              testModule.specifiedPlans.mkString(", ")
            )
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
    }

    override val module: Module = new PassivationModule
    override def moduleCompanions: List[AggregateRootModule] = {
      val result = List( AnalysisPlanModule.module, fixture.module )
      logger.debug( "TEST: PASSIVATION-FIXTURE module-companions:[{}]", result.mkString(", ") )
      result
    }
  }


  override def createAkkaFixture( test: OneArgTest ): Fixture = {
    test.tags match {
      case tags if tags.contains( PASSIVATE.name ) => new PassivationFixture
      case _ => new Fixture
    }
  }

  object PASSIVATE extends Tag( "passivate" )


  "CatalogModule" should {
    "add Catalog" in { f: Fixture =>
      import f.{ timeout => to, _ }
      logger.info( "TEST: entity-type=[{}] tid=[{}]", identifying.evEntity, tid )

      import scala.concurrent.ExecutionContext.Implicits.global

      entity ! EntityMessages.Add( tid, Some(catalog) )
      whenReady( stateFrom(entity, tid, "foo"), timeout(3.seconds.dilated) ) { actual =>
        logger.info( "TEST: actual plans for topic:[{}] = [{}]", actual.request, actual )
        actual.plans.size mustBe 1
        val actualPlan = actual.plans.head
        actualPlan.name mustBe "foo"
      }

      whenReady( stateFrom(entity, tid, "dummy"), timeout(3.seconds.dilated) ) { actual =>
        logger.info( "TEST: actual plans for topic:[{}] = [{}]", actual.request, actual )
        actual.plans mustBe empty
      }
    }

    "recover from passivation" taggedAs ( PASSIVATE, WIP )in { f: Fixture =>
      import f.{ timeout => to, _ }
      logger.info( "TEST: entity-type=[{}] tid=[{}]", identifying.evEntity, tid )
      entity ! EntityMessages.Add( tid, Some(catalog) )
      whenReady( stateFrom(entity, tid, "foo"), timeout(3.seconds.dilated) ) { actual =>
        logger.info( "TEST: actual plans for topic:[{}] = [{}]", actual.request, actual )
        actual.plans.size mustBe 1
        val actualPlan = actual.plans.head
        actualPlan.name mustBe "foo"
      }

      logger.info( "TEST:SLEEPING..." )
      Thread.sleep( 10000 )
      logger.info( "TEST:AWAKE...")

      whenReady( stateFrom(entity, tid, "foo"), timeout(3.seconds.dilated) ) { actual =>
        logger.info( "TEST: actual plans for topic:[{}] = [{}]", actual.request, actual )
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
