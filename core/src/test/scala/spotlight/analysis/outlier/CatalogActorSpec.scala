package spotlight.analysis.outlier

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.testkit._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.ScalaFutures
import peds.akka.envelope.Envelope
import demesne.DomainModel
import demesne.testkit.concurrent.CountDownFunction
import peds.akka.envelope._
import spotlight.analysis.outlier.CatalogProtocol.CatalogedPlans
import spotlight.testkit.ParallelAkkaSpec
import spotlight.analysis.outlier.{CatalogProtocol => P}
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.Topic


/**
  * Created by rolfsd on 8/1/16.
  */
class CatalogActorSpec extends ParallelAkkaSpec with ScalaFutures with StrictLogging { outer =>
  class Fixture( config: Config = CatalogActorSpec.config ) extends AkkaFixture( config = config ) with StrictLogging { fixture =>
    import ExecutionContext.Implicits.global

    val model: DomainModel = Await.result( DomainModel.make( s"CatalogTest-${fixtureId}" )( system, global ).toOption.get, 1.second )
    val modelContext = Map(
      demesne.ModelKey -> model,
      demesne.SystemKey -> system,
      demesne.FactoryKey -> demesne.factory.contextFactory,
      demesne.ConfigurationKey -> config
    )

    implicit val actorTimeout = Timeout( 5.seconds.dilated )
    AnalysisPlanModule.module.initialize( modelContext ) map { Await.ready( _ , 5.seconds ) }

    val bus = TestProbe()
    system.eventStream.subscribe( bus.ref, classOf[AnalysisPlanProtocol.Event] )

    val index = model.aggregateIndexFor[String, AnalysisPlanModule.module.TID, OutlierPlan.Summary](
      AnalysisPlanModule.module.rootType, AnalysisPlanModule.namedPlanIndex
    ).toOption.get

    def stateFrom( ar: ActorRef, topic: Topic ): Future[CatalogedPlans] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      import peds.akka.envelope.pattern.ask

      ( ar ?+ P.GetPlansForTopic(topic) ).mapTo[Envelope].map{ _.payload }.mapTo[P.CatalogedPlans]
    }
  }

//  class
//  class PassivationFixture( config: Config = CatalogActorSpec.config ) extends Fixture( config = config ) { fixture =>
//    class PassivationModule extends CatalogModule { testModule =>
//      override def initializer(
//        rootType: AggregateRootType,
//        model: DomainModel,
//        props: Map[Symbol, Any]
//      )(
//        implicit ec: ExecutionContext
//      ): Valid[Future[Done]] = trace.block( "PASSIVATION-MODULE INITIALIZER" ) {
//        super.initializer( rootType, model, props )
//      }
//
//
//      override def aggregateRootPropsOp: AggregateRootProps = {
//        (model: DomainModel, rootType: AggregateRootType) => Props( new FixtureCatalogActor( model, rootType ) )
//      }
//
//      override def rootType: AggregateRootType = {
//        new AggregateRootType {
//          override def passivateTimeout: Duration = {
//            val pto = 2.seconds
//            logger.info( "passivateTimeout=[{}]", pto )
//            pto
//          }
//
//          override def name: String = testModule.shardName
//
//          override def aggregateRootProps(implicit model: DomainModel): Props = testModule.aggregateRootPropsOp( model, this )
//        }
//      }
//
//
//      class FixtureCatalogActor( model: DomainModel, rootType: AggregateRootType )
//      extends CatalogModule.CatalogActor( model, rootType )
//      with CatalogModule.CatalogActor.Provider
//      with StackableStreamPublisher
//      with StackableRegisterBusPublisher {
//        override def detectionBudget: FiniteDuration = testModule.detectionBudget
//        override def specifiedPlans: Set[OutlierPlan] = testModule.specifiedPlans
//
//        override def preActivate(): Unit = {
//          log.info( "TEST: PASSIVATION CATALOG MODULE -- BEFORE ACTIVATION")
//        }
//
//        override def active: Receive = {
//          case m if super.active isDefinedAt m => {
//            log.debug(
//              "TEST: IN PASSIVATION FIXTURE ACTOR!!!  detection-budget:[{}] specified-plans:[{}]",
//              testModule.detectionBudget.toCoarsest,
//              testModule.specifiedPlans.mkString(", ")
//            )
//            super.active( m )
//          }
//        }
//
//        override protected def onPersistRejected( cause: Throwable, event: Any, seqNr: Long ): Unit = {
//          log.error(
//            "Rejected to persist event type [{}] with sequence number [{}] for persistenceId [{}] due to [{}].",
//            event.getClass.getName, seqNr, persistenceId, cause
//          )
//          throw cause
//        }
//      }
//    }
//
//    override val module: Module = new PassivationModule
//    override def moduleCompanions: List[AggregateRootModule] = {
//      val result = List( AnalysisPlanModule.module, fixture.module )
//      logger.debug( "TEST: PASSIVATION-FIXTURE module-companions:[{}]", result.mkString(", ") )
//      result
//    }
//  }

  override def createAkkaFixture( test: OneArgTest ): Fixture = {
    test.tags match {
//      case tags if tags.contains( PASSIVATE.name ) => new PassivationFixture
      case _ => new Fixture
    }
  }

//  object PASSIVATE extends Tag( "passivate" )


  "CatalogActor" should {
    "start Catalog" in { f: Fixture =>
      import f._
      import akka.pattern.ask
      import spotlight.analysis.outlier.{ AnalysisPlanProtocol => AP }
      import demesne.module.entity.{ messages => EntityMessages }

      logger.info( "TEST: ==============  STARTING TEST  ==============")
      whenReady( index.futureEntries, timeout(3.seconds.dilated) ) { _ mustBe empty }

      logger.info( "TEST: ==============  CREATING CATALOG  ==============")
      val catalog = system.actorOf( CatalogActor.props(model, config) )
      val foo = (catalog ? P.WaitingForStart).mapTo[P.Started.type]
      Await.ready( foo, 15.seconds.dilated )

      logger.info( "TEST: ==============  COUNTDOWN  ==============")
      val countDown = new CountDownFunction[String]
      countDown await 200.millis.dilated

      logger.info( "TEST: ==============  CHECKING BUS  ==============")
      bus.expectMsgPF( hint = "added" ) {
        case payload: EntityMessages.Added => {
          logger.info( "TEST: Bus received Added message:[{}]", payload )
          payload.info mustBe defined
        }
      }

      logger.info( "TEST: ==============  CHECKING INDEX  ==============")
      whenReady( index.futureEntries, timeout(15.seconds.dilated) ) { after =>
        after must not be empty
        after.keySet must contain ("foo")
      }

      logger.info( "TEST: ==============  DONE  ==============")
      //      logger.info( "TEST: entity-type=[{}] tid=[{}]", identifying.evEntity, tid )

      import scala.concurrent.ExecutionContext.Implicits.global

//      entity ! EntityMessages.Add( tid, Some(catalog) )
    }

//    "recover from passivation" taggedAs ( PASSIVATE, WIP )in { f: Fixture =>
//      import f.{ timeout => to, _ }
//      logger.info( "TEST: entity-type=[{}] tid=[{}]", identifying.evEntity, tid )
//      entity ! EntityMessages.Add( tid, Some(catalog) )
//      whenReady( stateFrom(entity, tid, "foo"), timeout(3.seconds.dilated) ) { actual =>
//        logger.info( "TEST: actual plans for topic:[{}] = [{}]", actual.request, actual )
//        actual.plans.size mustBe 1
//        val actualPlan = actual.plans.head
//        actualPlan.name mustBe "foo"
//      }
//
//      logger.info( "TEST:SLEEPING..." )
//      Thread.sleep( 10000 )
//      logger.info( "TEST:AWAKE...")
//
//      whenReady( stateFrom(entity, tid, "foo"), timeout(3.seconds.dilated) ) { actual =>
//        logger.info( "TEST: actual plans for topic:[{}] = [{}]", actual.request, actual )
//        actual.plans.size mustBe 1
//        val actualPlan = actual.plans.head
//        actualPlan.name mustBe "foo"
//      }
//    }
  }
}

object CatalogActorSpec extends StrictLogging {
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
