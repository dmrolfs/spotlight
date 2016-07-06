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

      trait RootTypeConfiguration extends AggregateRootType.ConfigurationProvider {

      }

      class TestModuleBuilderFactory extends EntityAggregateModule.BuilderFactory[OutlierPlan] {
        override type CC = TestModuleImpl

        case class TestModuleImpl(
          override val aggregateIdTag: Symbol,
          override val aggregateRootPropsOp: AggregateRootProps,
          _indexes: MakeIndexSpec,
          override val idLens: Lens[OutlierPlan, OutlierPlan#TID],
          override val nameLens: Lens[OutlierPlan, String],
          override val slugLens: Option[Lens[OutlierPlan, String]],
          override val isActiveLens: Option[Lens[OutlierPlan, Boolean]]
        ) extends EntityAggregateModule[OutlierPlan] with Equals {
          override val trace: Trace[_] = Trace( s"EntityAggregateModule[OutlierPlan]" )
          override val evState: ClassTag[OutlierPlan] = implicitly[ClassTag[OutlierPlan]]


          override def rootType: AggregateRootType = new EntityAggregateRootType with RootTypeConfiguration {
            override def aggregateRootProps(implicit model: DomainModel): Props = module.aggregateRootPropsOp( model, this )
            override def name: String = module.shardName
          }

          override lazy val indexes: Seq[AggregateIndexSpec[_,_]] = _indexes()

          override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[EntityAggregateModuleImpl]

          override def equals( rhs: Any ): Boolean = rhs match {
            case that: EntityAggregateModuleImpl => {
              if ( this eq that ) true
              else {
                ( that.## == this.## ) &&
                  ( that canEqual this ) &&
                  ( this.aggregateIdTag == that.aggregateIdTag )
              }
            }

            case _ => false
          }

          override def hashCode: Int = 41 * ( 41 + aggregateIdTag.## )

        }
      }



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