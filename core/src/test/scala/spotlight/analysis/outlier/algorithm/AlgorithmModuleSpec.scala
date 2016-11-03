package spotlight.analysis.outlier.algorithm

import akka.actor.ActorSystem

import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.pattern.ask
import akka.testkit._
import com.typesafe.config.Config

import scalaz.{-\/, \/-}
import org.scalatest.concurrent.ScalaFutures
import org.joda.{time => joda}
import demesne.AggregateRootType
import demesne.testkit.AggregateRootSpec
import org.apache.commons.math3.random.RandomDataGenerator
import org.scalatest.OptionValues
import peds.archetype.domain.model.core.EntityIdentifying
import peds.commons.V
import peds.commons.log.Trace
import spotlight.analysis.outlier.HistoricalStatistics
import spotlight.analysis.outlier.algorithm.{AlgorithmProtocol => P}
import spotlight.model.outlier._
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/9/16.
  */
abstract class AlgorithmModuleSpec[S: ClassTag] extends AggregateRootSpec[S] with ScalaFutures with OptionValues { outer =>
  private val trace = Trace[AlgorithmModuleSpec[S]]


  override type ID = AlgorithmModule#ID
  override type Protocol = AlgorithmProtocol.type
  override val protocol: Protocol = AlgorithmProtocol


  type Module <: AlgorithmModule
  val defaultModule: Module
  lazy val identifying: EntityIdentifying[AlgorithmModule.AnalysisState] = AlgorithmModule.identifying


  override def testSlug( test: OneArgTest ): String = {
    s"Test-${defaultModule.algorithm.label.name}-${testPosition.incrementAndGet()}"
  }

  override def testConfiguration( test: OneArgTest, slug: String ): Config = {
    val c = spotlight.testkit.config( systemName = slug )
    import scala.collection.JavaConversions._
    logger.debug( "Test Config: akka.cluster.seed-nodes=[{}]", c.getStringList("akka.cluster.seed-nodes").mkString(", "))
    c
  }

  override type Fixture <: AlgorithmFixture

  abstract class AlgorithmFixture( _config: Config, _system: ActorSystem, _slug: String )
  extends AggregateFixture( _config, _system, _slug ) {
    fixture =>

    val subscriber = TestProbe()

    override def before( test: OneArgTest ): Unit = {
      super.before( test )
      logger.info( "Fixture: DomainModel=[{}]", model )
    }

    type Module = outer.Module
    override lazy val module: Module = outer.defaultModule
    override def rootTypes: Set[AggregateRootType] = Set( module.rootType )


    type TestState = module.State
    type TestAdvanced = P.Advanced
    type TestShape = module.Shape
    val shapeLens = module.analysisStateCompanion.shapeLens
//    val thresholdLens = module.analysisStateCompanion.thresholdLens
//    val advancedLens = shapeLens ~ thresholdLens
    val advancedLens = shapeLens

    def expectedUpdatedState( state: TestState, event: TestAdvanced ): TestState = trace.block( s"expectedUpdatedState" ) {
      logger.debug( "TEST: argument state=[{}]", state )
      val result = advancedLens.modify( state ){ case shape =>
        logger.debug( "TEST: in advancedLens: BEFORE shape=[{}]", shape )
        val newShape = module.analysisStateCompanion.advanceShape( shape, event )
        logger.debug( "TEST: in advancedLens: AFTER shape=[{}]", newShape )

        newShape
//        (
//          newShape,
//          thresholds :+ event.threshold
//        )
      }
      logger.debug( "TEST: MODIFIED State Shape:[{}]", shapeLens get result )
      result
    }

    import AlgorithmModule.AnalysisState

    def actualVsExpectedState( actual: Option[AnalysisState], expected: Option[AnalysisState] ): Unit = {
      actual.isDefined mustBe expected.isDefined
      for {
        a <- actual
        e <- expected
      } {
        logger.debug( "TEST: actualVsExpected STATE:\n  Actual:[{}]\nExpected:[{}]", a, e )
        a.id.id mustBe e.id.id
        a.name mustBe e.name
        a.algorithm.name mustBe e.algorithm.name
//        a.thresholds mustBe e.thresholds
        a.## mustBe e.##
      }

      actual mustEqual expected
      expected mustEqual actual
      actual.## mustEqual expected.##
    }

    implicit val shapeOrdering: Ordering[TestShape]

    def actualVsExpectedShape( actual: TestShape, expected: TestShape )( implicit ordering: Ordering[TestShape] ): Unit = {
      logger.debug( "TEST: actualVsExpected SHAPE:\n  Actual:[{}]\nExpected:[{}]", actual.toString, expected.toString )
      assert( ordering.equiv(actual, expected) )
      assert( ordering.equiv(expected, actual) )
    }

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

    lazy val id: module.TID = nextId()

    override def nextId(): TID = identifying.nextIdAs[TID] match {
      case \/-( tid ) => tid
      case -\/( ex ) => {
        logger.error( "failed to create next ID", ex )
        throw ex
      }
    }

    lazy val aggregate = module aggregateOf id

    private val trace = Trace[AlgorithmFixture]
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

  def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = trace.block("historyWith") {
    logger.info( "series:{}", series)
    logger.info( "prior={}", prior )
    prior map { h =>
      logger.info( "Adding series to prior shape")
      series.points.foldLeft( h ){ _ :+ _ }
    } getOrElse {
      logger.info( "Creating new shape from series")
      HistoricalStatistics.fromActivePoints( series.points, false )
    }
  }

  def tailAverageData(
    data: Seq[DataPoint],
    last: Seq[DataPoint] = Seq.empty[DataPoint],
    tailLength: Int = defaultModule.AlgorithmContext.DefaultTailAverageLength
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


  def bootstrapSuite(): Unit = {
    s"${defaultModule.algorithm.label.name} entity" should {
      "have zero state before advance" in { f: Fixture =>
        import f._

        logger.debug( "aggregate = [{}]", aggregate )
        val actual = ( aggregate ? P.GetStateSnapshot( id ) ).mapTo[P.StateSnapshot]
        whenReady( actual, timeout(5.seconds) ) { a =>
          a.sourceId.id mustBe id.id
          a.snapshot mustBe None
        }
      }

      "advance for datapoint processing" taggedAs WIP in { f: Fixture =>
        import f._

        val pt = DataPoint( nowTimestamp, 3.14159 )
        val t = ThresholdBoundary( nowTimestamp, Some(1.1), Some(2.2), Some(3.3) )
        val adv = P.Advanced( id, pt, true, t )
        logger.debug( "TEST: Advancing: [{}] for id:[{}]", adv, id )
        aggregate ! adv

        Thread.sleep(1000)
        logger.debug( "TEST: getting current state of id:[{}]...", id )
        whenReady( ( aggregate ? P.GetStateSnapshot(id) ).mapTo[P.StateSnapshot], timeout(15.seconds.dilated) ){ s1 =>
          val zero = module.analysisStateCompanion.zero( id )
          actualVsExpectedState( s1.snapshot, Option(expectedUpdatedState(zero, adv)) )
        }
      }
    }
  }


  def analysisStateSuite(): Unit = {
    s"${defaultModule.algorithm.label.name} state" should {
      "advance state" in { f: Fixture =>
        import f._
        val zero = module.analysisStateCompanion.zero( id )
        val pt = DataPoint( nowTimestamp, 3.14159 )
        val t = ThresholdBoundary( nowTimestamp, Some(1.1), Some(2.2), Some(3.3) )
        val adv = P.Advanced( id, pt, false, t )
//        val zeroWithThreshold = thresholdLens.modify( zero ){ _ :+ t }
        val zeroWithThreshold = zero
        val actual = shapeLens.modify( zeroWithThreshold ){ s => module.analysisStateCompanion.advanceShape(s, adv) }
        val expected = expectedUpdatedState( zero, adv )
        logger.debug( "TEST: expectedState=[{}]", expected )
        actualVsExpectedState( Option(actual), Option(expected) )
      }

      "advance shape" in { f: Fixture =>
        import f._
        val zero = module.analysisStateCompanion.zero( id )
        logger.debug( "TEST: zero=[{}]", zero)
        val pt = DataPoint( nowTimestamp, 3.14159 )
        val t = ThresholdBoundary( nowTimestamp, Some(1.1), Some(2.2), Some(3.3) )
        val adv = P.Advanced( id, pt, false, t )
        val expected = advancedLens.modify( zero ){ case shape =>
//          (
//            module.analysisStateCompanion.updateShape( shape, adv ),
//            thresholds :+ adv.threshold
//          )
          module.analysisStateCompanion.advanceShape( shape, adv )
        }
        logger.debug( "TEST: expectedState=[{}]", expected )

        val zeroShape = shapeLens.get( zero )
        val actualShape = module.analysisStateCompanion.advanceShape( zeroShape, adv )
        actualVsExpectedShape( actualShape, shapeLens get expected )
      }
    }
  }
}
