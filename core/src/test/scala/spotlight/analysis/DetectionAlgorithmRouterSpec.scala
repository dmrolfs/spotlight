package spotlight.analysis

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.{ Config, ConfigFactory }
import demesne.{ AggregateRootType, DomainModel }
import org.joda.{ time ⇒ joda }
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import peds.akka.envelope.{ Envelope, WorkId }
import spotlight.analysis.shard._
import spotlight.model.outlier.{ IsQuorum, AnalysisPlan, ReduceOutliers }
import spotlight.model.timeseries.{ DataPoint, TimeSeries }
import spotlight.testkit.ParallelAkkaSpec

/** Created by rolfsd on 10/20/15.
  */
class DetectionAlgorithmRouterSpec extends ParallelAkkaSpec with MockitoSugar {
  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AkkaFixture( _config, _system, _slug ) {
    val algo = "foo"
    val algorithm = TestProbe()
    val algorithmRootType = mock[AggregateRootType]
    when( algorithmRootType.name ).thenReturn { algo }
    implicit val shardIdentifying = CellShardModule.identifying
    val catalogId = ShardCatalog.idFor( plan, algo )
    val catalog = TestProbe()
    val catalogRootType = CellShardModule.module.rootType
    val model = mock[DomainModel]
    logger.debug( "FIXTURE: SHARD ROOT-TYPE:[{}]", catalogRootType )
    when( model( catalogRootType, catalogId ) ).thenReturn( catalog.ref )
    when( model.configuration ).thenReturn( _config )

    lazy val plan = makePlan( "TestPlan", None )
    def makePlan( name: String, g: Option[AnalysisPlan.Grouping] ): AnalysisPlan = {
      AnalysisPlan.default(
        name = name,
        algorithms = Set( algo ),
        grouping = g,
        timeout = 500.millis,
        isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = 1, triggerPoint = 1 ),
        reduce = ReduceOutliers.byCorroborationPercentage( 50 ),
        planSpecification = ConfigFactory.parseString(
          s"""
             |algorithm-config.${algo}.seedEps: 5.0
             |algorithm-config.${algo}.minDensityConnectedPoints: 3
          """.stripMargin
        )
      )
    }

    //    val resolver = DetectionAlgorithmRouter.DirectProxy( algorithm.ref )
    //    val resolver = DetectionAlgorithmRouter.ShardedRootTypeProxy( plan, algorithmRootType, model )
    //    val router = TestActorRef[DetectionAlgorithmRouter]( DetectionAlgorithmRouter.props( plan, Map(algo -> resolver) ) )
    val subscriber = TestProbe()
  }

  "DetectionAlgorithmRouter" should {
    import DetectionAlgorithmRouter._
    "register algorithms" in { f: Fixture ⇒
      import f._
      val probe = TestProbe()
      val router = TestActorRef[DetectionAlgorithmRouter]( DetectionAlgorithmRouter.props( plan, Map() ) )
      router.receive( RegisterAlgorithmReference( "foo", probe.ref ), probe.ref )
      probe.expectMsgPF( hint = "register", max = 200.millis.dilated ) {
        case Envelope( AlgorithmRegistered( actual ), _ ) ⇒ actual mustBe "foo"
      }
    }

    "route detection messages" taggedAs WIP in { f: Fixture ⇒
      import f._
      model must not be ( null )
      val testConfig = ConfigFactory.parseString(
        s"""
           | spotlight.detection-plans.${plan.name}.shard: ${AlgorithmRoute.ShardedRoute.CellStrategy.key}
         """.stripMargin
      )
      when( model.configuration ).thenReturn( testConfig.withFallback( config ) )
      model.configuration.getString( s"spotlight.detection-plans.${plan.name}.shard" ) mustBe AlgorithmRoute.ShardedRoute.CellStrategy.key

      algorithmRootType must not be ( null )
      catalogId must not be ( null )
      CellShardModule.module.rootType must not be ( null )
      logger.debug( "TEST: SHARD ROOT-TYPE:[{}]", CellShardModule.module.rootType )
      catalogRootType mustBe catalogRootType
      CellShardModule.module.rootType mustBe CellShardModule.module.rootType
      CellShardModule.module.rootType mustBe catalogRootType
      model( CellShardModule.module.rootType, catalogId ) must be( catalog.ref )

      val router = TestActorRef[DetectionAlgorithmRouter]( DetectionAlgorithmRouter.props( plan, Map() ) )
      router.receive( RegisterAlgorithmRootType( algo, algorithmRootType, model, true ) )
      logger.debug( "#TEST looking for shard catalog add..." )
      catalog.expectMsgPF( hint = "catalog-add" ) {
        case Envelope( m: CellShardProtocol.Add, _ ) ⇒ {
          m.plan mustBe plan.toSummary
          m.algorithmRootType mustBe algorithmRootType
        }
        case Envelope( m, _ ) ⇒ {
          m mustBe a[CellShardProtocol.Add]
        }
      }
      logger.debug( "#TEST ...catalog add passed" )

      //      logger.debug( "#TEST looking for catalog add AGAIN..." )
      //      catalog.expectMsgPF( hint = "catalog-add" ) {
      //        case Envelope( m: LookupShardProtocol.Add, _ ) => {
      //          m.plan mustBe plan.toSummary
      //          m.algorithmRootType mustBe algorithmRootType
      //        }
      //      }
      //      logger.debug( "#TEST ...catalog add passed AGAIN" )

      val myPoints = Seq(
        DataPoint( new joda.DateTime( 448 ), 8.46 ),
        DataPoint( new joda.DateTime( 449 ), 8.9 ),
        DataPoint( new joda.DateTime( 450 ), 8.58 ),
        DataPoint( new joda.DateTime( 451 ), 8.36 ),
        DataPoint( new joda.DateTime( 452 ), 8.58 ),
        DataPoint( new joda.DateTime( 453 ), 7.5 ),
        DataPoint( new joda.DateTime( 454 ), 7.1 ),
        DataPoint( new joda.DateTime( 455 ), 7.3 ),
        DataPoint( new joda.DateTime( 456 ), 7.71 ),
        DataPoint( new joda.DateTime( 457 ), 8.14 ),
        DataPoint( new joda.DateTime( 458 ), 8.14 ),
        DataPoint( new joda.DateTime( 459 ), 7.1 ),
        DataPoint( new joda.DateTime( 460 ), 7.5 ),
        DataPoint( new joda.DateTime( 461 ), 7.1 ),
        DataPoint( new joda.DateTime( 462 ), 7.1 ),
        DataPoint( new joda.DateTime( 463 ), 7.3 ),
        DataPoint( new joda.DateTime( 464 ), 7.71 ),
        DataPoint( new joda.DateTime( 465 ), 8.8 ),
        DataPoint( new joda.DateTime( 466 ), 8.9 )
      )

      val series = TimeSeries( "series", myPoints )
      val aggregator = TestProbe()
      val msg = DetectUsing(
        plan.id,
        "foo",
        DetectOutliersInSeries( series, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
        HistoricalStatistics( 2, false )
      )

      implicit val sender = aggregator.ref
      router.receive( msg )

      logger.debug( "#TEST looking for routing to catalog..." )
      catalog.expectMsgPF( hint = "catalog" ) {
        //        case Envelope( payload, _ ) => payload mustBe msg
        case Envelope( ShardProtocol.RouteMessage( _, payload ), _ ) ⇒ { payload mustBe msg }
        case m ⇒ {
          logger.debug( "#TEST NOT FOUND" )
          fail( s"not found: ${m}" )
        }
      }
      logger.debug( "#TEST ...catalog routing passed" )
      //      catalog.forward( algo)
      //
      //      algorithm.expectMsgPF( 2.seconds.dilated, "route" ) {
      //        case Envelope( actual, _ ) => actual mustBe msg
      //      }
    }
  }
}
