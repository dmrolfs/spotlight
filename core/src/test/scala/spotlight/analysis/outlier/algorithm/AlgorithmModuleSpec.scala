package spotlight.analysis.outlier.algorithm

import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit._
import com.typesafe.config.Config

import org.scalatest.concurrent.ScalaFutures
import org.joda.{time => joda}
import demesne.AggregateRootType
import demesne.testkit.AggregateRootSpec
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.mockito.Mockito._
import org.scalatest.{Assertion, OptionValues}
import peds.akka.envelope._
import peds.archetype.domain.model.core.EntityIdentifying
import peds.commons.V
import peds.commons.log.Trace
import spotlight.analysis.outlier.{DetectOutliersInSeries, DetectUsing, HistoricalStatistics}
import spotlight.analysis.outlier.algorithm.{AlgorithmProtocol => P}
import spotlight.model.outlier._
import spotlight.model.timeseries._
import spotlight.testkit.TestCorrelatedSeries


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
    private val trace = Trace[AlgorithmFixture]

    val sender = TestProbe()
    val subscriber = TestProbe()

    val appliesToAll: OutlierPlan.AppliesTo = {
      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification( 0, 0 )
      val reduce: ReduceOutliers = new ReduceOutliers {

        import scalaz._

        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: OutlierPlan
        ): V[Outliers] = {
          Validation.failureNel[Throwable, Outliers]( new IllegalStateException( "should not use" ) ).disjunction
        }
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

    val scope: module.TID = identifying tag OutlierPlan.Scope( plan = "TestPlan", topic = "test.topic" )
    val plan = mock[OutlierPlan]
    when( plan.name ).thenReturn( scope.id.plan )
    when( plan.appliesTo ).thenReturn( fixture.appliesToAll )
    when( plan.algorithms ).thenReturn( Set( module.algorithm.label ) )

    lazy val aggregate = module aggregateOf id

    override def nextId(): module.TID = fixture.scope

    //    override def nextId(): TID = identifying.nextIdAs[TID] match {
    //      case \/-( tid ) => tid
    //      case -\/( ex ) => {
    //        logger.error( "failed to create next ID", ex )
    //        throw ex
    //      }
    //    }


    override def before(test: OneArgTest): Unit = {
      super.before( test )
      logger.info( "Fixture: DomainModel=[{}]", model )
    }

    type Module = outer.Module
    override lazy val module: Module = outer.defaultModule
    implicit val evState: ClassTag[module.State] = module.evState.asInstanceOf[ClassTag[module.State]]

    override def rootTypes: Set[AggregateRootType] = Set( module.rootType )


    type TestState = module.State
    type TestAdvanced = P.Advanced
    type TestShape = module.Shape
    val shapeLens = module.analysisStateCompanion.shapeLens
    //    val thresholdLens = module.analysisStateCompanion.thresholdLens
    //    val advancedLens = shapeLens ~ thresholdLens
    val advancedLens = shapeLens

    def expectedUpdatedState(state: TestState, event: TestAdvanced): TestState = trace.block( s"expectedUpdatedState" ) {
      logger.debug( "TEST: argument state=[{}]", state )
      val result = advancedLens.modify( state ) { case shape =>
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

    def actualVsExpectedState(actual: Option[AnalysisState], expected: Option[AnalysisState]): Unit = {
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

    def actualVsExpectedShape(actual: TestShape, expected: TestShape)(implicit ordering: Ordering[TestShape]): Unit = {
      logger.debug( "TEST: actualVsExpected SHAPE:\n  Actual:[{}]\nExpected:[{}]", actual.toString, expected.toString )
      assert( ordering.equiv( actual, expected ) )
      assert( ordering.equiv( expected, actual ) )
    }


    def evaluate(
      hint: String,
      series: TimeSeries,
      history: HistoricalStatistics,
      expectedResults: Seq[Expected],
      assertStateFn: ( module.State ) => Assertion = (_: module.State) => succeed
    ): Unit = {
      import scala.concurrent.duration._
      aggregate.sendEnvelope(
        DetectUsing(
          algorithm = module.algorithm.label,
          payload = DetectOutliersInSeries(
            TestCorrelatedSeries( series ),
            plan,
            subscriber.ref
          ),
          history = history
        )
      )(
        sender.ref
      )

      val expectedAnomalies = expectedResults.exists { e => e.isOutlier }

      sender.expectMsgPF( 500.millis.dilated, hint ) {
        case m@Envelope( SeriesOutliers( a, s, p, o, t ), _ ) if expectedAnomalies => {
          a mustBe Set( module.algorithm.label )
          s mustBe series
          o.size mustBe 1
          o mustBe Seq( series.points.last )

          t( module.algorithm.label ).zip( expectedResults ).zipWithIndex foreach { case ( ((actual, expected), i) ) =>
            (i, actual.floor) mustBe(i, expected.floor)
            (i, actual.expected) mustBe(i, expected.expected)
            (i, actual.ceiling) mustBe(i, expected.ceiling)
          }
        }

        case m@Envelope( NoOutliers( a, s, p, t ), _ ) if !expectedAnomalies => {
          a mustBe Set( module.algorithm.label )
          s mustBe series

          t( module.algorithm.label ).zip( expectedResults ).zipWithIndex foreach { case ( ((actual, expected), i) ) =>
            logger.debug( "evaluating expectation: {}", i.toString )
            actual.floor.isDefined mustBe expected.floor.isDefined
            for {
              af <- actual.floor
              ef <- expected.floor
            } {af mustBe ef +- 0.000001}

            actual.expected.isDefined mustBe expected.expected.isDefined
            for {
              ae <- actual.expected
              ee <- expected.expected
            } {ae mustBe ee +- 0.000001}

            actual.ceiling.isDefined mustBe expected.ceiling.isDefined
            for {
              ac <- actual.ceiling
              ec <- expected.ceiling
            } {ac mustBe ec +- 0.000001}
          }
        }
      }

      import akka.pattern.ask

      val actual = ( aggregate ? P.GetStateSnapshot( id ) ).mapTo[P.StateSnapshot]
      whenReady( actual, timeout( 15.seconds.dilated ) ) { a =>
        val as = a.snapshot
        logger.info( "{}: ACTUAL = [{}]", hint, as )
        a.snapshot mustBe defined
        as mustBe defined
        as.value mustBe an[module.State]
        val sas = as.value.asInstanceOf[module.State]
        sas.id.id mustBe id.id
        sas.algorithm.name mustBe module.algorithm.label.name
        logger.info( "asserting state: {}", sas )
        assertStateFn( sas )
      }
    }
  }


  case class Expected( isOutlier: Boolean, floor: Option[Double], expected: Option[Double], ceiling: Option[Double] ) {
    def stepResult( i: Int, intervalSeconds: Int = 10 )( implicit start: joda.DateTime ): Option[(Boolean, ThresholdBoundary)] = {
      Some(
        isOutlier,
        ThresholdBoundary(
          timestamp = start.plusSeconds( i * intervalSeconds ),
          floor = floor,
          expected = expected,
          ceiling = ceiling
        )
      )
    }
  }

  object Expected {
    def fromStatistics( isOutlier: Boolean, tolerance: Double, result: Option[CalculationMagnetResult] ): Expected = {
      val (f, e, c) = {
        val expected = result map { _.expected }

        val (floor, ceiling) = {
          import scalaz.Unzip
          import scalaz.std.option._

          val fc = for {
            e <- expected
            d <- result map { r => math.abs( r.height * tolerance ) }
          } yield ( e - d, e + d )

          Unzip[Option] unzip fc
        }

        ( floor, expected, ceiling )
      }

      Expected( isOutlier, floor = f, expected = e, ceiling = c )
    }
  }

  def makeExpected(
    magnet: CalculationMagnet
  )(
    points: Seq[DataPoint],
    outliers: Seq[Boolean],
    history: Seq[DataPoint] = Seq.empty[DataPoint], // datapoints?
    tolerance: Double = 3.0
  ): (Seq[Expected], Option[magnet.Result]) = {
    val all = history ++ points
    val calculated: List[Option[magnet.Result]] = {
      for {
        pos <- ( 1 to all.size ).toList
        pts = all take pos
      } yield Option( magnet(pts) )
    }

    val results = None :: calculated //todo: right thinking?  prove out with subsequent batch
    logger.info( "TEST: results-size:[{}]  points-size:[{}] history-size:[{}]", results.size.toString, points.size.toString, history.size.toString )
    val expected = outliers.zip( results.drop(history.size) ) map { case (o, r) =>
      Expected.fromStatistics( isOutlier = o, tolerance = tolerance, result = r )
    }
    ( expected, results.last )
  }


  sealed trait NoHint
  object NoHint extends NoHint

  trait CalculationMagnetResult {
    type Value
    def N: Long
    def expected: Double
    def height: Double
  }

  trait CalculationMagnet {
    type Result <: CalculationMagnetResult
    def apply( points: Seq[DataPoint] ): Result
  }

  abstract class CommonCalculationMagnet extends CalculationMagnet {
    case class Result( value: DescriptiveStatistics ) extends CalculationMagnetResult {
      override type Value = DescriptiveStatistics
      override def N: Long = value.getN
      override def expected: Double = value.getMean
      override def height: Double = value.getStandardDeviation
    }

    override def apply( points: Seq[DataPoint] ): Result = Result( new DescriptiveStatistics(points.map{ _.value }.toArray) )
  }

  object CalculationMagnet {
    implicit def fromNoHint( noHint: NoHint ): CalculationMagnet = new CommonCalculationMagnet { }
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

      "advance for datapoint processing" in { f: Fixture =>
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
