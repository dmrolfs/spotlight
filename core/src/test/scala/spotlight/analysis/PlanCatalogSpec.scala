package spotlight.analysis

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.ScalaFutures
import omnibus.akka.envelope.Envelope
import demesne.{AggregateRootType, BoundedContext, DomainModel}
import demesne.testkit.concurrent.CountDownFunction
import omnibus.akka.envelope._
import spotlight.Settings
import spotlight.analysis.PlanCatalogProtocol.CatalogedPlans
import spotlight.analysis.{AnalysisPlanProtocol => AP}
import spotlight.testkit.ParallelAkkaSpec
import spotlight.analysis.{PlanCatalogProtocol => P}
import spotlight.model.outlier.AnalysisPlan
import spotlight.model.timeseries.Topic


/**
  * Created by rolfsd on 8/1/16.
  */
class PlanCatalogSpec extends ParallelAkkaSpec with ScalaFutures with StrictLogging { outer =>

  override def testConfiguration( test: OneArgTest, slug: String ): Config = PlanCatalogSpec.config( slug )

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    test.tags match {
      //      case tags if tags.contains( PASSIVATE.name ) => new PassivationFixture
      case _ => new Fixture( config, system, slug )
    }
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AkkaFixture( _config, _system, _slug ) {
    fixture =>

    import ExecutionContext.Implicits.global

    implicit val materializer: Materializer = ActorMaterializer()

    def rootTypes: Set[AggregateRootType] = Set( AnalysisPlanModule.module.rootType )
    lazy val boundedContext: BoundedContext = trace.block( "boundedContext" ) {
      val bc = {
        for {
          made <- BoundedContext.make( Symbol(slug), config, rootTypes )
          started <- made.start()( global, actorTimeout )
        } yield started
      }
      Await.result( bc, 5.seconds )
    }

    implicit lazy val model: DomainModel = trace.block( "model" ) { Await.result( boundedContext.futureModel, 5.seconds ) }

    implicit val actorTimeout = Timeout( 5.seconds.dilated )

    val bus = TestProbe()
    system.eventStream.subscribe( bus.ref, classOf[AP.Event] )

//    lazy val index = trace.block( "index" ) {
//      PlanCatalog.
//      model.aggregateIndexFor[String, AnalysisPlanModule.module.TID, AnalysisPlan.Summary](
//        AnalysisPlanModule.module.rootType, AnalysisPlanModule.namedPlanIndex
//      ).toOption.get
//    }

    def stateFrom( ar: ActorRef, topic: Topic ): Future[CatalogedPlans] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      import omnibus.akka.envelope.pattern.ask

      ( ar ?+ P.GetPlansForTopic(topic) ).mapTo[Envelope].map{ _.payload }.mapTo[P.CatalogedPlans]
    }
  }

//  class
//  class PassivationFixture( config: Config = PlanCatalogSpec.config ) extends Fixture( config = config ) { fixture =>
//    class PassivationModule extends PlanCatalog { testModule =>
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
//        (model: DomainModel, rootType: AggregateRootType) => Props( new FixturePlanCatalog( model, rootType ) )
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
//      class FixturePlanCatalog( model: DomainModel, rootType: AggregateRootType )
//      extends PlanCatalog.PlanCatalog( model, rootType )
//      with PlanCatalog.PlanCatalog.Provider
//      with StackableStreamPublisher
//      with StackableRegisterBusPublisher {
//        override def detectionBudget: FiniteDuration = testModule.detectionBudget
//        override def specifiedPlans: Set[AnalysisPlan] = testModule.specifiedPlans
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
//            "Rejected to persist event type [{}] with sequence number [{}] for aggregateId [{}] due to [{}].",
//            event.getClass.getName, seqNr, aggregateId, cause
//          )
//          throw cause
//        }
//      }
//    }
//
//    override val module: Algo = new PassivationModule
//    override def moduleCompanions: List[AggregateRootModule] = {
//      val result = List( AnalysisPlanModule.module, fixture.module )
//      logger.debug( "TEST: PASSIVATION-FIXTURE module-companions:[{}]", result.mkString(", ") )
//      result
//    }
//  }


  "PlanCatalog" should {
    "start Catalog" in { f: Fixture =>
      import f._
      import akka.pattern.ask

      val planSpecs = Settings.detectionPlansConfigFrom( config )
      assert( planSpecs.hasPath( "foo" ) )
      logger.debug( "#TEST planSpecs.foo = [{}]", planSpecs.getConfig("foo") )
      logger.info( "TEST: ==============  STARTING TEST  ==============")
//      whenReady( index.futureEntries, timeout(3.seconds.dilated) ) { _ mustBe empty }

      logger.info( "TEST: ==============  CREATING CATALOG  ==============")
      val catalog = system.actorOf(
        PlanCatalog.props(
          config,
          applicationPlans = Settings.PlanFactory.makePlans( planSpecs, 30.seconds )
        )(
          boundedContext
        )
      )

      val foo = (catalog ? P.WaitForStart).mapTo[P.Started.type]
      Await.ready( foo, 15.seconds.dilated )

//      whenReady( plans.view.future(), timeout(3.seconds.dilated) ) { _ mustBe empty }

      logger.info( "TEST: ==============  COUNTDOWN  ==============")
      val countDown = new CountDownFunction[String]
      countDown await 200.millis.dilated

      logger.info( "TEST: ==============  CHECKING BUS  ==============")
      bus.expectMsgPF( hint = "added" ) {
        case payload: AP.Added => {
          logger.info( "TEST: Bus received Added message:[{}]", payload )
          payload.info mustBe defined
        }
      }

//      logger.info( "TEST: ==============  CHECKING INDEX  ==============")
//      whenReady( plans.view.future(), timeout(15.seconds.dilated) ) { after =>
//        after must not be empty
//        assert( after.exists{ _.name == "foo" } )
//      }

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

object PlanCatalogSpec extends StrictLogging {
  def config( systemName: String ): Config = {
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
        |      parallelism-cpu-factor: 1
        |    }
        |  }
        |}
      """.stripMargin
    )

    val result: Config = catalogConfig.resolve() withFallback spotlight.testkit.config( "core", systemName )
//    logger.info(
//      "TEST CONFIGURATION:[\n{}\n]",
//      result.root().render( com.typesafe.config.ConfigRenderOptions.defaults().setFormatted(true))
//    )
    result
  }
}
