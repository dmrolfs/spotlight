package spotlight.analysis.outlier.algorithm

import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout
import shapeless.syntax.typeable._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.Tag
import com.typesafe.scalalogging.LazyLogging
import org.joda.{time => joda}
import peds.akka.envelope._
import demesne.AggregateRootModule
import demesne.testkit.AggregateRootSpec
import spotlight.analysis.outlier.DetectionAlgorithmRouter
import spotlight.analysis.outlier.algorithm.AlgorithmModule.{Protocol => P}
import spotlight.model.timeseries.{DataPoint, ThresholdBoundary}

import scala.concurrent.Await


/**
  * Created by rolfsd on 6/9/16.
  */
abstract class AlgorithmModuleSpec[S: ClassTag] extends AggregateRootSpec[S] with ScalaFutures with LazyLogging {
  val module: AlgorithmModule

  override type Fixture <: AlgorithmFixture
  abstract class AlgorithmFixture extends AggregateFixture { fixture =>
    logger.info( "Fixture: DomainModel=[{}]",model, model )

    override def moduleCompanions: List[AggregateRootModule[_]] = List( module )
    logger.debug( "Fixture.context = [{}]", context )
    logger.debug(
      "checkSystem elems: system:[{}] raw:[{}]",
      context.get(demesne.SystemKey).flatMap{_.cast[ActorSystem]},
      context.get(demesne.SystemKey)
    )

    val router = TestProbe()

    val bus = TestProbe()
    system.eventStream.subscribe( bus.ref, classOf[AlgorithmModule.Event] )

    def nextId(): module.TID
    val id: module.TID = nextId()

    lazy val aggregate = module aggregateOf id

    def addAndRegisterAggregate(
      aggregate: ActorRef = fixture.aggregate,
      id: module.TID = fixture.id
    )(
      implicit timeout: Timeout = fixture.timeout,
      router: TestProbe = fixture.router
    ): Unit = {
      aggregate ! P.Add( id )
      val registered = ( aggregate ? P.Register(id, router.ref) ).mapTo[Envelope]
      router.expectMsgPF( 400.millis.dilated, "router register" ) {
        case Envelope(DetectionAlgorithmRouter.RegisterDetectionAlgorithm(a, h), _) => {
          a.name mustBe module.algorithm.label.name
          h !+ DetectionAlgorithmRouter.AlgorithmRegistered( a )
        }
      }

      val envelope = Await.result( registered, 1.second.dilated )
      envelope.payload mustBe a [P.Registered]
      val actual = envelope.payload.asInstanceOf[P.Registered]
      actual.sourceId mustBe id
    }
  }


  type TestState = module.State
  type TestAdvanced = module.AnalysisState.Advanced
  type TestHistory = module.analysisStateCompanion.History

  def expectedUpdatedState( state: TestState, event: TestAdvanced ): TestState = {
    state.addThreshold( event.threshold )
  }

  def actualVsExpectedState( actual: TestState, expected: TestState ): Unit = {
    actual.id mustBe expected.id
    actual.name mustBe expected.name
    actual.algorithm.name mustBe expected.algorithm.name
    actual.tolerance mustBe expected.tolerance
    actual.thresholds mustBe expected.thresholds
    actual.## mustBe expected.##
    actual mustEqual expected
    expected mustEqual actual
  }

  def actualVsExpectedHistory( actual: TestHistory, expected: TestHistory ): Unit = {
    actual mustEqual expected
    expected mustEqual actual
  }


  object WIP extends Tag( "wip" )


  def bootstrapSuite(): Unit = {
    s"${module.algorithm.label.name} entity" should {
      "add algorithm" in { f: Fixture =>
        import f._
        aggregate ! P.Add( id )
        bus.expectMsgPF( max = 400.millis.dilated, hint = "algo added" ) {
          case p: P.Added => p.sourceId mustBe id
        }
      }

      "must not respond before add" in { f: Fixture =>
        import f._
        import module.AnalysisState.Protocol._

        aggregate !+ P.Register( id, router.ref )
        aggregate !+ GetStateSnapshot( id )
        bus.expectNoMsg()
      }

      "have zero state after add" in { f: Fixture =>
        import f._
        import module.AnalysisState.Protocol._

        implicit val to = f.timeout

        aggregate ! P.Add( id )
        val actual = ( aggregate ? GetStateSnapshot( id ) ).mapTo[StateSnapshot]
        whenReady( actual ) { a =>
          a.sourceId mustBe id
          a.snapshot mustBe module.analysisStateCompanion.zero( id )
        }
      }

      "save and load snapshot" taggedAs (WIP) in { f: Fixture =>
        import f._
        import module.AnalysisState.Protocol._
        implicit val to = f.timeout
        addAndRegisterAggregate()
        val now = joda.DateTime.now
        val pt = DataPoint( now, 3.14159 )
        val t = ThresholdBoundary( now, Some(1.1), Some(2.2), Some(3.3) )
        val adv = module.AnalysisState.Advanced( id, pt, true, t )
        aggregate ! adv
        val s1 = Await.result( ( aggregate ? GetStateSnapshot(id) ).mapTo[StateSnapshot], 1.second.dilated )
        val zero = module.analysisStateCompanion.zero( id )
        actualVsExpectedState( s1.snapshot, expectedUpdatedState(zero, adv) )
      }

      "register with router" in { f: Fixture =>
        import f._
        implicit val to = f.timeout
        addAndRegisterAggregate()
      }
    }
  }


  def analysisStateSuite(): Unit = {
    s"${module.algorithm.label.name} state" should {
      "advance history" in { f: Fixture =>
        import f._

        val zero = module.analysisStateCompanion.zero( id )
        val now = joda.DateTime.now
        val pt = DataPoint( now, 3.14159 )
        val t = ThresholdBoundary( now, Some(1.1), Some(2.2), Some(3.3) )
        val adv = module.AnalysisState.Advanced( id, pt, false, t )
        val hlens = module.analysisStateCompanion.historyLens
        val actualState = hlens.modify( zero.addThreshold(t) ){ h => module.analysisStateCompanion.updateHistory(h, adv) }
        val expectedState = expectedUpdatedState( zero, adv )
        val zeroHistory = hlens.get( zero )
        val actualHistory = module.analysisStateCompanion.updateHistory( zeroHistory, adv )
        actualVsExpectedState( actualState, expectedState )
        actualVsExpectedHistory( actualHistory, hlens.get(expectedState) )
      }
    }
  }
}
