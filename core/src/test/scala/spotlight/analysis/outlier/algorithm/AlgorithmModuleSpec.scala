package spotlight.analysis.outlier.algorithm

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout
import shapeless.syntax.typeable._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.Tag
import org.mockito.Mockito._
import com.typesafe.scalalogging.LazyLogging
import org.joda.{time => joda}
import peds.akka.envelope._
import demesne.AggregateRootModule
import demesne.testkit.AggregateRootSpec
import org.apache.commons.math3.random.RandomDataGenerator
import peds.commons.V
import peds.commons.log.Trace
import spotlight.analysis.outlier.{DetectionAlgorithmRouter, HistoricalStatistics}
import spotlight.analysis.outlier.algorithm.AlgorithmModule.{Protocol => P}
import spotlight.model.outlier._
import spotlight.model.timeseries._



/**
  * Created by rolfsd on 6/9/16.
  */
abstract class AlgorithmModuleSpec[S: ClassTag] extends AggregateRootSpec[S] with ScalaFutures with LazyLogging {
  type Module <: AlgorithmModule
  val module: Module

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

    val appliesToAll: OutlierPlan.AppliesTo = {
      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification(0, 0)
      val reduce: ReduceOutliers = new ReduceOutliers {
        import scalaz._
        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: OutlierPlan
        ): V[Outliers] = Validation.failureNel[Throwable, Outliers]( new IllegalStateException("should not use" ) ).disjunction
      }

      import scala.concurrent.duration._
      val grouping: Option[OutlierPlan.Grouping] = {
        val window = None
        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
      }

      OutlierPlan.default( "", 1.second, isQuorun, reduce, Set.empty[Symbol], grouping ).appliesTo
    }

    implicit val nowTimestamp: joda.DateTime = joda.DateTime.now

    val router = TestProbe()

    val bus = TestProbe()
    system.eventStream.subscribe( bus.ref, classOf[AlgorithmModule.Event] )

    def nextId(): module.TID
    lazy val id: module.TID = nextId()

    lazy val aggregate = module aggregateOf id

    private val trace = Trace[AlgorithmFixture]

    def addAndRegisterAggregate(
      aggregate: ActorRef = fixture.aggregate,
      id: module.TID = fixture.id
    )(
      implicit timeout: Timeout = fixture.timeout,
      router: TestProbe = fixture.router
    ): ActorRef = trace.block( "addAndRegisterAggregate" ){
      logger.info( "aggregate:[{}]", aggregate )
      logger.info( "id:[{}]", id )
      logger.info( "timeout:[{}]", timeout )
      logger.info( "router:[{}]", router )

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

      aggregate
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


  def makeDataPoints(
    values: Seq[Double],
    start: joda.DateTime = joda.DateTime.now,
    period: FiniteDuration = 1.second,
    timeWiggle: (Double, Double) = (1D, 1D),
    valueWiggle: (Double, Double) = (1D, 1D)
  ): Seq[DataPoint] = {
    val random = new RandomDataGenerator

    def nextFactor( wiggle: (Double, Double) ): Double = {
      val (lower, upper) = wiggle
      if ( upper <= lower ) upper else random.nextUniform( lower, upper )
    }

    val secs = start.getMillis / 1000L
    val epochStart = new joda.DateTime( secs * 1000L )

    values.zipWithIndex map { vi =>
      import com.github.nscala_time.time.Imports._
      val (v, i) = vi
      val tadj = ( i * nextFactor(timeWiggle) ) * period
      val ts = epochStart + tadj.toJodaDuration
      val vadj = nextFactor( valueWiggle )
      DataPoint( timestamp = ts, value = (v * vadj) )
    }
  }

  def spike( topic: Topic, data: Seq[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
    val (front, last) = data.sortBy{ _.timestamp.getMillis }.splitAt( position )
    val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
    TimeSeries( topic, spiked )
  }

  def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = {
    prior map { h =>
      series.points.foldLeft( h ){ _ :+ _ }
    } getOrElse {
      HistoricalStatistics.fromActivePoints( series.points, false )
    }
  }

  def tailAverageData(
    data: Seq[DataPoint],
    last: Seq[DataPoint] = Seq.empty[DataPoint],
    tailLength: Int = module.AlgorithmContext.DefaultTailAverageLength
  ): Seq[DataPoint] = {
    val lastPoints = last.drop( last.size - tailLength + 1 ) map { _.value }
    data.map { _.timestamp }
    .zipWithIndex
    .map { case (ts, i ) =>
      val pointsToAverage: Seq[Double] = {
        if ( i < tailLength ) {
          val all = lastPoints ++ data.take( i + 1 ).map{ _.value }
          all.drop( all.size - tailLength )
        } else {
          data
          .map { _.value }
          .slice( i - tailLength + 1, i + 1 )
        }
      }

      ( ts, pointsToAverage )
    }
    .map { case (ts, pts) => DataPoint( timestamp = ts, value = pts.sum / pts.size ) }
  }

  def calculateControlBoundaries(
    points: Seq[DataPoint],
    tolerance: Double,
    lastPoints: Seq[DataPoint] = Seq.empty[DataPoint]
  ): Seq[ThresholdBoundary]


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

        val algoRef = addAndRegisterAggregate()
        val actual = ( algoRef ? GetStateSnapshot( id ) ).mapTo[StateSnapshot]
        whenReady( actual ) { a =>
          a.sourceId mustBe id
          a.snapshot mustBe module.analysisStateCompanion.zero( id )
        }
      }

      "register with router" in { f: Fixture =>
        import f._
        implicit val to = f.timeout
        addAndRegisterAggregate()
      }

      "save and load snapshot" in { f: Fixture =>
        import f._
        import module.AnalysisState.Protocol._
        implicit val to = f.timeout
        val algoRef = addAndRegisterAggregate()
        val pt = DataPoint( nowTimestamp, 3.14159 )
        val t = ThresholdBoundary( nowTimestamp, Some(1.1), Some(2.2), Some(3.3) )
        val adv = module.AnalysisState.Advanced( id, pt, true, t )
        algoRef ! adv
        val s1 = Await.result( ( aggregate ? GetStateSnapshot(id) ).mapTo[StateSnapshot], 1.second.dilated )
        val zero = module.analysisStateCompanion.zero( id )
        actualVsExpectedState( s1.snapshot, expectedUpdatedState(zero, adv) )
      }
    }
  }


  def analysisStateSuite(): Unit = {
    s"${module.algorithm.label.name} state" should {
      "advance history" in { f: Fixture =>
        import f._

        val zero = module.analysisStateCompanion.zero( id )
        val pt = DataPoint( nowTimestamp, 3.14159 )
        val t = ThresholdBoundary( nowTimestamp, Some(1.1), Some(2.2), Some(3.3) )
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
