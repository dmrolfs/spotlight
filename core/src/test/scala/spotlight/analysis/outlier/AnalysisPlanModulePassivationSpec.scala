package spotlight.analysis.outlier

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import demesne._
import demesne.module.entity.{EntityAggregateModule, messages => EntityMessages}
import demesne.DomainModel.DomainModelImpl
import demesne.module.AggregateRootProps
import demesne.module.entity.EntityAggregateModule.MakeIndexSpec
import demesne.register.AggregateIndexSpec
import peds.commons.builder.HasBuilder
import peds.commons.identifier.{ShortUUID, TaggedID}
import peds.commons.log.Trace
import shapeless.{Generic, HNil, Lens}
import spotlight.analysis.outlier.algorithm.skyline.SimpleMovingAverageModule
import spotlight.model.outlier.{IsQuorum, OutlierPlan, ReduceOutliers}
import spotlight.testkit.EntityModuleSpec
import spotlight.analysis.outlier.{AnalysisPlanProtocol => P}

import scala.reflect.ClassTag


/**
  * Created by rolfsd on 6/15/16.
  */
class AnalysisPlanModulePassivationSpec extends EntityModuleSpec[OutlierPlan] {

  val trace = Trace[AnalysisPlanModulePassivationSpec]
//  class TestModuleBuilderFactory extends EntityAggregateModule.BuilderFactory[OutlierPlan] {
//    override type CC = TestModuleImpl
//
//    case class TestModuleImpl(
//      __aggregateIdTag: Symbol,
//      __aggregateRootPropsOp: AggregateRootProps,
//      __indexes: MakeIndexSpec,
//      __idLens: Lens[OutlierPlan, OutlierPlan#TID],
//      __nameLens: Lens[OutlierPlan, String],
//      __slugLens: Option[Lens[OutlierPlan, String]],
//      __isActiveLens: Option[Lens[OutlierPlan, Boolean]]
//    ) extends EntityAggregateModuleImpl(
//      __aggregateIdTag,
//      __aggregateRootPropsOp,
//      __indexes,
//      __idLens,
//      __nameLens,
//      __slugLens,
//      __isActiveLens
//    ) with Equals {
//      override val trace: Trace[_] = Trace( "TestModuleImpl" )
//      override val evState: ClassTag[OutlierPlan] = implicitly[ClassTag[OutlierPlan]]
//
//      override def rootType: AggregateRootType = new EntityAggregateRootType {
//        override def passivateTimeout: Duration = 2.seconds
//        override def aggregateRootProps(implicit model: DomainModel): Props = module.aggregateRootPropsOp( model, this )
//        override def name: String = module.shardName
//      }
//
//      override lazy val indexes: Seq[AggregateIndexSpec[_,_]] = _indexes()
//
//      override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[TestModuleImpl]
//
//      override def equals( rhs: Any ): Boolean = rhs match {
//        case that: TestModuleImpl => {
//          if ( this eq that ) true
//          else {
//            ( that.## == this.## ) &&
//            ( that canEqual this ) &&
//            ( this.aggregateIdTag == that.aggregateIdTag )
//          }
//        }
//
//        case _ => false
//      }
//
//      override def hashCode: Int = 41 * ( 41 + aggregateIdTag.## )
//
//    }
//  }
//
//  override type Module = EntityAggregateModule[OutlierPlan]
//  override val module: Module = {
//    val b = new TestModuleBuilderFactory().make
//    import b.P.{ Tag => BTag, Props => BProps, _ }
//
//    b
//    .builder
//    .set( BTag, AnalysisPlanModule.identifying.idTag )
//    .set( BProps, AnalysisPlanModule.AggregateRoot.OutlierPlanActor.props(_, _) )
//    .set( IdLens, OutlierPlan.idLens )
//    .set( NameLens, OutlierPlan.nameLens )
//    .set( IsActiveLens, Some(OutlierPlan.isActiveLens) )
//    .build()
//  }


  class Module extends EntityAggregateModule[OutlierPlan] { testModule =>
    override val trace: Trace[_] = Trace[Module]
    override val idLens: Lens[OutlierPlan, TaggedID[ShortUUID]] = OutlierPlan.idLens
    override val nameLens: Lens[OutlierPlan, String] = OutlierPlan.nameLens
    override def aggregateRootPropsOp: AggregateRootProps = AnalysisPlanModule.AggregateRoot.OutlierPlanActor.props( _, _ )

    override def rootType: AggregateRootType = {
      new AggregateRootType {
        override def passivateTimeout: Duration = {
          logger.info( "passivateTimeout=[{}]", 2.seconds )
          2.seconds
        }
        override def aggregateRootProps( implicit model: DomainModel ): Props = testModule.aggregateRootPropsOp( model, this )
        override def name: String = testModule.shardName
      }
    }
  }

  override val module: Module = new Module

  override type ID = module.ID
  override type Protocol = AnalysisPlanProtocol.type
  override val protocol: Protocol = AnalysisPlanProtocol

  class Fixture extends EntityFixture( config = AnalysisPlanModulePassivationSpec.config ) {
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

    "recover and continue after passivation" taggedAs WIP in { f: Fixture =>
      import f._
      import demesne.module.entity.{ messages => EntityMessages }

      val p1 = makePlan( "TestPlan", None )
      val planTid = p1.id

      def infoFrom( ar: ActorRef ): OutlierPlan = trace.block( s"infoFrom($ar)" ) {
        import scala.concurrent.ExecutionContext.Implicits.global
        import akka.pattern.ask
        implicit val to = Timeout( 2.seconds )
        Await.result(
          ( ar ? P.GetInfo(planTid) ).mapTo[Envelope].map{ _.payload }.mapTo[P.PlanInfo].map{ _.info },
          2.seconds.dilated
        )
      }

      logger.info( "TEST: P1.id=[{}]  planTid:[{}]", p1.id, planTid )
      entity ! EntityMessages.Add( p1.id, Some(p1) )
      bus.expectMsgClass( classOf[EntityMessages.Added] )

      infoFrom( entity ) mustBe p1

      logger.info( "TEST:SLEEPING..." )
      Thread.sleep( 10000 )
      logger.info( "TEST:AWAKE...")

      infoFrom( entity ) mustBe p1

      entity !+ P.UseAlgorithms( planTid, Set( 'stella, 'otis, 'apollo ), ConfigFactory.empty() )
      bus.expectMsgPF( 1.second.dilated, "change algos" ) {
        case P.AlgorithmsChanged(pid, algos, c) => {
          pid mustBe planTid
          algos mustBe Set( 'stella, 'otis, 'apollo )
          assert( c.isEmpty )
        }
      }
    }
  }
}

object AnalysisPlanModulePassivationSpec {
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