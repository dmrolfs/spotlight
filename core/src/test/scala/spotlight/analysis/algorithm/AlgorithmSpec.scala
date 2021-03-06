package spotlight.analysis.algorithm

import java.io.Serializable
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.math
import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.ask
import akka.testkit._
import cats.syntax.validated._
import com.persist.logging._
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.concurrent.ScalaFutures
import org.joda.{ time ⇒ joda }
import demesne.AggregateRootType
import demesne.testkit.AggregateRootSpec
import demesne.testkit.concurrent.CountDownFunction
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.commons.math3.stat.descriptive.{ StatisticalSummary, SummaryStatistics }
import org.mockito.Mockito._
import org.scalatest.{ Assertion, OptionValues, Tag }
import omnibus.akka.envelope._
import omnibus.commons._
import omnibus.commons.identifier.Identifying
import omnibus.commons.log.Trace
import spotlight.Settings
import spotlight.analysis.{ AnomalyScore, DetectOutliersInSeries, DetectUsing, HistoricalStatistics }
import spotlight.analysis.algorithm.Advancing.syntax._
import spotlight.analysis.algorithm.{ AlgorithmProtocol ⇒ P }
import spotlight.infrastructure.ClusterRole
import spotlight.model.outlier._
import spotlight.model.statistics.MovingStatistics
import spotlight.model.timeseries._
import squants.information.Bytes

/** Created by rolfsd on 6/9/16.
  */
abstract class AlgorithmSpec[S <: Serializable: Advancing: ClassTag]
    extends AggregateRootSpec[S] with ScalaFutures with OptionValues with ClassLogging {
  outer ⇒

  private val trace = Trace[AlgorithmSpec[S]]

  override type ID = Algorithm.ID
  override type Protocol = AlgorithmProtocol.type
  override val protocol: Protocol = AlgorithmProtocol

  type Algo <: Algorithm[S]
  val defaultAlgorithm: Algo
  type State = defaultAlgorithm.State
  type Shape = defaultAlgorithm.Shape

  lazy val identifying: Identifying.Aux[defaultAlgorithm.State, Algorithm.ID] = defaultAlgorithm.identifying

  val memoryPlateauNr: Int
  lazy val memoryStepsToValidate: Int = memoryPlateauNr * 100
  val memoryAllowedMargin: Double = 0.05

  override def testSlug( test: OneArgTest ): String = {
    s"Test-${defaultAlgorithm.label}-${testPosition.incrementAndGet()}"
  }

  override def testConfiguration( test: OneArgTest, slug: String ): Config = {
    val tc = ConfigFactory.parseString(
      """
        |
      """.stripMargin
    )

    val c = Settings.adaptConfiguration(
      config = tc.resolve().withFallback(
        spotlight.testkit.config( systemName = slug, portOffset = scala.util.Random.nextInt( 20000 ) )
      ),
      role = ClusterRole.All,
      externalHostname = "127.0.0.1",
      requestedPort = 0,
      systemName = slug
    )

    import scala.collection.JavaConverters._
    log.debug(
      Map(
        "@msg" → "Test Config",
        "akka.cluster.seed-nodes" → c.getStringList( "akka.cluster.seed-nodes" ).asScala.mkString( ", " )
      )
    )

    c
  }

  override type Fixture <: AlgorithmFixture

  abstract class AlgorithmFixture( _config: Config, _system: ActorSystem, _slug: String )
      extends AggregateFixture( _config, _system, _slug ) {
    fixture ⇒
    private val trace = Trace[AlgorithmFixture]

    val tol = 1E-11
    val emptyConfig: Config = ConfigFactory.empty
    val sender = TestProbe()
    val subscriber = TestProbe()

    def shapeMemoryConfig: Option[Config] = None

    val countDown = new CountDownFunction[String]

    var loggingSystem: LoggingSystem = LoggingSystem( _system, s"Test:${defaultAlgorithm.label}", "1", "localhost" )
    log.info( Map( "@msg" → "#TEST #############  logging system", "logging" → loggingSystem.toString ) )

    override def after( test: OneArgTest ): Unit = {
      Option( loggingSystem ) foreach { ls ⇒
        log.warn( "#TEST stopping persist logger..." )
        //        Await.ready( ls.stop, 15.seconds )
      }

      log.warn( "#TEST STOPPED PERSIST LOGGER" )
      super.after( test )
    }

    val appliesToAll: AnalysisPlan.AppliesTo = {
      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification( 0, 0 )
      val reduce: ReduceOutliers = new ReduceOutliers {

        import scalaz._

        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: AnalysisPlan
        ): AllErrorsOr[Outliers] = {
          new IllegalStateException( "should not use" ).invalidNel.toEither
        }
      }

      import scala.concurrent.duration._
      val grouping: Option[AnalysisPlan.Grouping] = {
        val window = None
        window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
      }

      AnalysisPlan.default( "", 1.second, isQuorun, reduce, Map.empty[String, Config], grouping ).appliesTo
    }

    implicit val nowTimestamp: joda.DateTime = joda.DateTime.now

    val router = TestProbe()

    lazy val id: module.TID = nextId()

    val scope = AnalysisPlan.Scope( plan = "TestPlan", topic = "test.topic" )
    val plan = mock[AnalysisPlan]
    when( plan.id ).thenReturn( AnalysisPlan.analysisPlanIdentifying.nextTID.unsafeGet )
    when( plan.name ).thenReturn( scope.plan )
    when( plan.appliesTo ).thenReturn( fixture.appliesToAll )
    when( plan.algorithms ).thenReturn( Map( defaultAlgorithm.label → emptyConfig ) )

    lazy val aggregate: ActorRef = {
      val r = module aggregateOf id
      log.info( Map( "@msg" → "#TEST: AGGREGATE", "id" → id.toString, "module ref" → r.toString() ) )
      r
    }

    //    override def nextId(): module.TID = identifying.tag( ShortUUID() )
    override def nextId(): module.TID = identifying.tag(
      AlgorithmIdentifier.nextId(
        planName = plan.name,
        planId = plan.id.id.toString,
        spanType = AlgorithmIdentifier.TopicSpan,
        topic = scope.topic
      )
    )

    type Module = defaultAlgorithm.module.type
    private val moduleCounter: AtomicInteger = new AtomicInteger( 0 )
    override val module: Module = {
      if ( 1 < moduleCounter.incrementAndGet() ) throw new IllegalStateException( "#TEST infinite loop" )

      log.debug( Map( "@msg" → "#TEST getting module", "defaultAlgorithm" → defaultAlgorithm ) )
      val m: Module = defaultAlgorithm.module
      log.debug( Map( "@msg" → "#TEST:module", "result" → m.toString ) )
      m
    }

    override def rootTypes: Set[AggregateRootType] = { Set( module.rootType ) }

    type TestState = defaultAlgorithm.State
    type TestShape = defaultAlgorithm.Shape

    //    val shapeLens = module.analysisStateCompanion.shapeLens
    //    val thresholdLens = module.analysisStateCompanion.thresholdLens
    //    val advancedLens = shapeLens ~ thresholdLens
    //    val advancedLens = shapeLens

    def expectedUpdatedShape( shape: TestShape, event: P.Advanced ): TestShape

    def actualVsExpectedState( actual: Option[State], expected: Option[State] ): Unit = {
      actual.isDefined mustBe expected.isDefined
      for {
        a ← actual
        e ← expected
      } {
        log.debug( Map( "@msg" → "#TEST: actualVsExpected STATE", "actual" → a.toString, "expected" → e ) )
        a.id.id mustBe e.id.id
        a.name mustBe e.name
        a.name mustBe e.name
        //        a.thresholds mustBe e.thresholds
        a.## mustBe e.##
      }

      actual mustEqual expected
      expected mustEqual actual
      actual.## mustEqual expected.##
    }

    implicit val shapeOrdering: Ordering[TestShape]

    def actualVsExpectedShape(
      actual: TestShape,
      expected: TestShape
    )(
      implicit
      ordering: Ordering[TestShape]
    ): Unit = {
      log.debug( Map( "@msg" → "#TEST: actualVsExpected SHAPE", "actual" → actual.toString, "expected" → expected.toString ) )
      assert( ordering.equiv( actual, expected ) )
      assert( ordering.equiv( expected, actual ) )
    }

    def assertOption( actual: Option[Double], expected: Option[Double], tolerance: Double, hint: String ): Assertion = {
      log.info( Map( "@msg" → "assertOption", "hint" → hint, "actual" → actual.toString, "expected" → expected.toString, "within" → tolerance.toString ) )
      if ( expected.isDefined ) {
        assert( actual.isDefined )
        actual.get mustBe ( expected.get +- tolerance )
      } else {
        assert( actual.isEmpty )
      }
    }

    def evaluate(
      hint: String,
      algorithmAggregateId: Algorithm.TID,
      series: TimeSeries,
      history: HistoricalStatistics,
      expectedResults: Seq[Expected],
      assertShapeFn: ( TestShape ) ⇒ Assertion = ( _: TestShape ) ⇒ succeed
    ): Assertion = {
      import scala.concurrent.duration._
      log.info(
        Map(
          "@msg" → "#TEST: evaluate",
          "id" → id.toString,
          "aggregate.path" → aggregate.path.toString,
          "series-size" → series.points.size,
          "expectedResults.size" → expectedResults.size,
          "plan.algorithms" → plan.algorithms
        )
      )

      val expectedAnomalies = series.points.zipWithIndex.collect { case ( dp, i ) if expectedResults( i ).isOutlier ⇒ dp }
      aggregate.sendEnvelope(
        DetectUsing(
          targetId = algorithmAggregateId,
          algorithm = defaultAlgorithm.label,
          payload = DetectOutliersInSeries( series, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
          history = history,
          properties = plan.algorithms.getOrElse( defaultAlgorithm.label, ConfigFactory.empty )
        )
      )(
          sender.ref
        )

      sender.expectMsgPF( 3.seconds.dilated, hint ) {
        case m @ Envelope( SeriesOutliers( a, s, p, o, t ), _ ) if expectedAnomalies.nonEmpty ⇒ {
          log.info( "evaluate EXPECTED ANOMALIES..." )
          a mustBe Set( defaultAlgorithm.label )
          s mustBe series
          o mustBe expectedAnomalies
          o.size mustBe expectedAnomalies.size

          t( defaultAlgorithm.label ).zip( expectedResults ).zipWithIndex foreach {
            case ( ( ( actual, expected ), i ) ) ⇒ {
              log.info( Map( "@msg" → "evaluate", "i" → i, "actual" → actual.toString, "expected" → expected.toString ) )
              assertOption( actual.floor, expected.floor, tol, s"$i floor" )
              assertOption( actual.expected, expected.expected, tol, s"$i expected" )
              assertOption( actual.ceiling, expected.ceiling, tol, s"$i ceiling" )
            }
          }
        }

        case m @ Envelope( NoOutliers( a, s, p, t ), _ ) if expectedAnomalies.isEmpty ⇒ {
          log.info( "evaluate EXPECTED normal..." )
          a mustBe Set( defaultAlgorithm.label )
          s mustBe series

          t( defaultAlgorithm.label ).zip( expectedResults ).zipWithIndex foreach {
            case ( ( ( actual, expected ), i ) ) ⇒ {
              log.debug(
                Map(
                  "@msg" → "evaluating expectation",
                  "i" → i,
                  "floor" → Map( "actual" → actual.floor.toString, "expected" → expected.floor.toString ),
                  "expected" → Map( "actual" → actual.expected.toString, "expected" → expected.expected.toString ),
                  "ceiling" → Map( "actual" → actual.ceiling.toString, "expected" → expected.ceiling.toString )
                )
              )

              actual.floor.isDefined mustBe expected.floor.isDefined
              for {
                af ← actual.floor
                ef ← expected.floor
              } { af mustBe ef +- 0.000001 }

              actual.expected.isDefined mustBe expected.expected.isDefined
              for {
                ae ← actual.expected
                ee ← expected.expected
              } { ae mustBe ee +- 0.000001 }

              actual.ceiling.isDefined mustBe expected.ceiling.isDefined
              for {
                ac ← actual.ceiling
                ec ← expected.ceiling
              } { ac mustBe ec +- 0.000001 }
            }
          }
        }
      }

      log.info( "TEST: --- AFTER DETECTUSING ---" )

      import akka.pattern.ask

      log.info( "TEST: --  GETTING SNAPSHOT ---" )

      val actual = ( aggregate ? P.GetTopicShapeSnapshot( id, series.topic ) ).mapTo[P.TopicShapeSnapshot]
      whenReady( actual, timeout( 15.seconds.dilated ) ) { a ⇒
        val as = a.snapshot
        log.info( Map( "@msg" → hint, "ACTUAL" → as.toString ) )
        as mustBe defined
        as.value mustBe an[TestShape]
        val sas = as.value.asInstanceOf[TestShape]
        a.sourceId.id mustBe id.id
        a.algorithm mustBe defaultAlgorithm.label
        log.info( Map( "@msg" → "asserting shape", "shape" → sas.toString ) )
        assertShapeFn( sas )
      }
    }

    def explodeOutlierIdentifiers( size: Int, positions: Set[Int] ): Seq[Boolean] = { ( 0 until size ).map { positions.contains } }
  }

  case class Expected( isOutlier: Boolean, floor: Option[Double], expected: Option[Double], ceiling: Option[Double] ) {
    def stepResult( i: Int, intervalSeconds: Int = 10 )( implicit start: joda.DateTime ): Option[AnomalyScore] = {
      Some(
        AnomalyScore(
          isOutlier,
          ThresholdBoundary(
            timestamp = start.plusSeconds( i * intervalSeconds ),
            floor = floor,
            expected = expected,
            ceiling = ceiling
          )
        )
      )
    }
  }

  object Expected {
    def fromStatistics( isOutlier: Boolean, tolerance: Double, result: Option[CalculationMagnetResult] ): Expected = {
      val tb = result map { _.thresholdBoundary }
      Expected(
        isOutlier,
        floor = tb.flatMap { _.floor },
        expected = tb.flatMap { _.expected },
        ceiling = tb.flatMap { _.ceiling }
      )
    }
  }

  def makeExpected(
    magnet: CalculationMagnet
  )(
    points: Seq[DataPoint],
    outliers: Seq[Boolean],
    history: Seq[DataPoint] = Seq.empty[DataPoint], // datapoints?
    tolerance: Double = 3.0,
    minimumPopulation: Int = 1
  ): ( Seq[Expected], Option[magnet.Result] ) = {
    val all = history ++ points
    val calculated: List[Option[magnet.Result]] = {
      for {
        pos ← ( minimumPopulation to all.size ).toList
        pts = all take pos
      } yield Option( magnet( pts ) )
    }

    val results = List.fill( minimumPopulation ) { None } ::: calculated
    log.info(
      Map(
        "@msg" → "#TEST: makeExpected",
        "results-size" → results.size,
        "points-size" → points.size,
        "history-size" → history.size
      )
    )
    val expected = outliers.zip( results.drop( history.size ) ) map {
      case ( o, r ) ⇒ Expected.fromStatistics( isOutlier = o, tolerance = tolerance, result = r )
    }
    log.info(
      Map(
        "@msg" → "makeExpected",
        "calculated-result" → results.last.toString,
        "expected" → expected.mkString( "\n", "\n", "\n" )
      )
    )

    ( expected, results.last )
  }

  sealed trait NoHint
  object NoHint extends NoHint

  trait CalculationMagnetResult {
    type Value
    def underlying: Value
    def tolerance: Double
    def timestamp: joda.DateTime
    def statistics: Option[StatisticalSummary]
    def thresholdBoundary: ThresholdBoundary
  }

  trait CalculationMagnet {
    type Result <: CalculationMagnetResult
    def apply( points: Seq[DataPoint], tolerance: Double = 3.0 ): Result
  }

  abstract class CommonCalculationMagnet extends CalculationMagnet {
    case class Result(
        override val underlying: StatisticalSummary,
        override val timestamp: joda.DateTime,
        override val tolerance: Double
    ) extends CalculationMagnetResult {
      override type Value = StatisticalSummary
      override def statistics: Option[StatisticalSummary] = Option( underlying )
      override def thresholdBoundary: ThresholdBoundary = {
        ThresholdBoundary.fromExpectedAndDistance(
          timestamp,
          expected = underlying.getMean,
          distance = tolerance * underlying.getStandardDeviation
        )
      }
    }

    override def apply( points: Seq[DataPoint], tolerance: Double = 3.0 ): Result = {
      Result(
        underlying = points.foldLeft( new SummaryStatistics() ) { ( s, p ) ⇒ s.addValue( p.value ); s },
        timestamp = points.last.timestamp,
        tolerance = tolerance
      )
    }
  }

  object CalculationMagnet {
    implicit def fromNoHint( noHint: NoHint ): CalculationMagnet = new CommonCalculationMagnet {}

    def sliding( width: Int = 3, tolerance: Double = 3.0 ) = new CommonCalculationMagnet {
      override def apply( points: Seq[DataPoint], tolerance: Double = 3.0 ): Result = {
        Result(
          underlying = points.foldLeft( MovingStatistics( width ) ) { ( s, p ) ⇒ s :+ p.value },
          timestamp = points.last.timestamp,
          tolerance = tolerance
        )
      }
    }
  }

  def makeDataPoints(
    values: Seq[Double],
    start: joda.DateTime = joda.DateTime.now,
    period: FiniteDuration = 1.second,
    timeWiggle: ( Double, Double ) = ( 1D, 1D ),
    valueWiggle: ( Double, Double ) = ( 1D, 1D )
  ): Seq[DataPoint] = {
    val random = new RandomDataGenerator

    def nextFactor( wiggle: ( Double, Double ) ): Double = {
      val ( lower, upper ) = wiggle
      if ( upper <= lower ) upper else random.nextUniform( lower, upper )
    }

    val secs = start.getMillis / 1000L
    val epochStart = new joda.DateTime( secs * 1000L )

    values.zipWithIndex map { vi ⇒
      import com.github.nscala_time.time.Imports._
      val ( v, i ) = vi
      val tadj = ( i * nextFactor( timeWiggle ) ) * period
      val ts = epochStart + tadj.toJodaDuration
      val vadj = nextFactor( valueWiggle )
      DataPoint( timestamp = ts, value = ( v * vadj ) )
    }
  }

  def assemble[T]( collection: Seq[T], positions: Seq[Int] ): Seq[T] = positions map collection

  def spike( topic: Topic, data: Seq[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
    val ( front, last ) = data.sortBy { _.timestamp.getMillis }.splitAt( position )
    val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
    TimeSeries( topic, spiked )
  }

  def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = trace.block( "historyWith" ) {
    log.info( Map( "@msg" → "historyWith", "series" → series.toString, "prior" → prior ) )
    prior map { h ⇒
      log.info( "Adding series to prior shape" )
      series.points.foldLeft( h ) { _ :+ _ }
    } getOrElse {
      log.info( "Creating new history from series" )
      HistoricalStatistics.fromActivePoints( series.points, false )
    }
  }

  def tailAverageData(
    data: Seq[DataPoint],
    last: Seq[DataPoint] = Seq.empty[DataPoint],
    tailLength: Int = AlgorithmContext.DefaultTailAverageLength
  ): Seq[DataPoint] = {
    val lastPoints = last.drop( last.size - tailLength + 1 ) map { _.value }
    data.map { _.timestamp }
      .zipWithIndex
      .map {
        case ( ts, i ) ⇒
          val pointsToAverage: Seq[Double] = {
            if ( i < tailLength ) {
              val all = lastPoints ++ data.take( i + 1 ).map { _.value }
              all.drop( all.size - tailLength )
            } else {
              data
                .map { _.value }
                .slice( i - tailLength + 1, i + 1 )
            }
          }

          ( ts, pointsToAverage )
      }
      .map { case ( ts, pts ) ⇒ DataPoint( timestamp = ts, value = pts.sum / pts.size ) }
  }

  def calculateControlBoundaries(
    points: Seq[DataPoint],
    tolerance: Double,
    lastPoints: Seq[DataPoint] = Seq.empty[DataPoint]
  ): Seq[ThresholdBoundary]

  def bootstrapSuite(): Unit = {
    s"${defaultAlgorithm.label} entity" should {
      "have zero shape before advance" in { f: Fixture ⇒
        import f._

        logger.debug( "aggregate = [{}]", aggregate )
        val actual = ( aggregate ? P.GetTopicShapeSnapshot( id, scope.topic ) ).mapTo[P.TopicShapeSnapshot]
        whenReady( actual, timeout( 10.seconds ) ) { a ⇒
          a.sourceId.id mustBe id.id
          a.snapshot mustBe None
        }
      }

      "advance for datapoint processing" in { f: Fixture ⇒
        import f._

        val pt = DataPoint( nowTimestamp, 3.14159 )
        val t = ThresholdBoundary( nowTimestamp, Some( 1.1 ), Some( 2.2 ), Some( 3.3 ) )
        val adv = P.Advanced( id, scope.topic, pt, true, t )
        log.debug( Map( "@msg" → "#TEST: Advancing", "advancing" → adv.toString, "id" → id.toString ) )
        aggregate ! adv

        countDown await 1.second.dilated

        log.debug( Map( "@msg" → "#TEST: getting current shape", "id" → id ) )
        whenReady(
          ( aggregate ? P.GetTopicShapeSnapshot( id, adv.topic ) ).mapTo[P.TopicShapeSnapshot],
          timeout( 15.seconds.dilated )
        ) { s1 ⇒
            val zero = shapeless.the[Advancing[Shape]].zero( None )
            countDown await 25.millis.dilated
            actualVsExpectedShape( s1.snapshot.get.asInstanceOf[TestShape], expectedUpdatedShape( zero, adv ) )
          }
      }
    }
  }

  object MEMORY extends Tag( "memory" )

  def analysisStateSuite(): Unit = {
    s"${defaultAlgorithm.label}" should {
      //      "advance state" in { f: Fixture =>
      //        import f._
      //        val zero = module.shapeCompanion.zero( None )
      //        val pt = DataPoint( nowTimestamp, 3.14159 )
      //        val t = ThresholdBoundary( nowTimestamp, Some(1.1), Some(2.2), Some(3.3) )
      //        val adv = P.Advanced( id, scope.topic, pt, false, t )
      ////        val zeroWithThreshold = thresholdLens.modify( zero ){ _ :+ t }
      //        val zeroWithThreshold = zero
      //        val actual = module.shapeCompanion.advance( zeroWithThreshold, adv )
      ////        val actual = shapeLens.modify( zeroWithThreshold ){ s => module.analysisStateCompanion.advanceShape(s, adv) }
      //        val expected = expectedUpdatedShape( zero, adv )
      //        logger.debug( "TEST: expectedState=[{}]", expected )
      //        actualVsExpectedShape( actual, expected )
      //      }

      "advance shape" in { f: Fixture ⇒
        import f._
        val zero = shapeless.the[Advancing[S]].zero( None )
        val pt = DataPoint( nowTimestamp, 3.14159 )
        val t = ThresholdBoundary( nowTimestamp, Some( 1.1 ), Some( 2.2 ), Some( 3.3 ) )
        val adv = P.Advanced( id, scope.topic, pt, false, t )
        val expected = expectedUpdatedShape( zero, adv )
        log.debug( Map( "@msg" → "#TEST advance shape", "zero" → zero.toString, "expected" → expected.toString ) )

        val actual = zero.advance( adv )
        countDown await 25.millis.dilated
        actualVsExpectedShape( actual, expected )
      }

      "verify estimated memory usage" taggedAs ( MEMORY ) in { f: Fixture ⇒
        import f._
        def makeData( i: Int ): P.Advanced = {
          val pt = DataPoint( nowTimestamp.plusMillis( 10 * i ), 0.14159265353 + 0.0001 * i )
          val t = ThresholdBoundary( nowTimestamp, Some( 1.1 ), Some( 2.2 ), Some( 3.3 ) )
          P.Advanced( id, scope.topic, pt, false, t )
        }

        defaultAlgorithm.estimatedAverageShapeSize( shapeMemoryConfig ) match {
          case None ⇒ pending
          case Some( expected ) ⇒ {
            val plateauNr = math.max( 1, memoryPlateauNr )
            val advancing = shapeless.the[Advancing[S]]
            val zero = advancing.zero( shapeMemoryConfig )
            val atPlateau = ( 0 to plateauNr ).foldLeft( zero ) { ( acc, i ) ⇒ acc.advance( makeData( i ) ) }

            import akka.serialization.{ SerializationExtension, Serializer }
            val serializer: Serializer = SerializationExtension( system ).serializerFor( zero.getClass )
            val actual = Bytes( serializer.toBinary( atPlateau.asInstanceOf[AnyRef] ).size )
            log.info( Map( "@msg" → "memory for shape", "shape" → atPlateau.toString, "actual" → actual.toString, "expected" → expected.toString ) )
            actual mustBe <=( expected )
          }
        }
      }

      "maintain constant memory per shape" taggedAs MEMORY in { f: Fixture ⇒
        import f._
        val random = scala.util.Random
        def makeData( i: Int ): P.Advanced = {
          val pt = DataPoint( nowTimestamp.plusMillis( 10 * i ), random.nextGaussian() )
          val t = ThresholdBoundary( nowTimestamp, Some( 1.1 ), Some( 2.2 ), Some( 3.3 ) )
          P.Advanced( id, scope.topic, pt, false, t )
        }

        val advancing: Advancing[S] = shapeless.the[Advancing[S]]
        val zero = advancing.zero( shapeMemoryConfig )

        import akka.serialization.{ SerializationExtension, Serializer }
        val serializer: Serializer = SerializationExtension( system ).serializerFor( zero.getClass )

        val zeroSize = Bytes( serializer.toBinary( zero.asInstanceOf[AnyRef] ).size )
        val first: S = zero.advance( makeData( 0 ) )
        val firstBytes = serializer.toBinary( first.asInstanceOf[AnyRef] )
        val firstSize = Bytes( firstBytes.size )

        val plateauNr = math.max( 1, memoryPlateauNr )
        val ( plateauState, plateauSize ) = ( 2 to plateauNr ).foldLeft( first, firstSize ) {
          case ( ( acc, accSize ), i ) ⇒ {
            val next = acc.advance( makeData( i ) )
            val nextSize = Bytes( serializer.toBinary( next.asInstanceOf[AnyRef] ).size )
            log.debug(
              Map(
                "@msg" → "advancing to expected plateau size",
                "step" → i,
                "plateau-nr" → plateauNr,
                "zero-size" → zeroSize.toString(),
                "accumulated" → Map(
                  "size" → accSize.toString,
                  "nr-shapes" → i,
                  "average-increment" → ( ( accSize - zeroSize ) / i ).toString
                )
              )
            )

            ( next, nextSize )
          }
        }

        log.debug(
          Map(
            "@msg" → "memory at expected plateau",
            "zero-size" → zeroSize.toString(),
            "plateau" → Map(
              "plateau-size" → plateauSize.toString,
              "plateau-nr-shapes" → plateauNr,
              "plateau-average-increment" → ( ( plateauSize - zeroSize ) / plateauNr ).toString
            )
          )
        )

        ( ( plateauNr + 1 ) to memoryStepsToValidate ).foldLeft( ( first, firstSize ) ) {
          case ( ( acc, accSize ), i ) ⇒ {
            val next = acc.advance( makeData( i ) )
            val nextSize = Bytes( serializer.toBinary( next.asInstanceOf[AnyRef] ).size )

            log.debug(
              Map(
                "@msg" → "checking memory impact on advance",
                "algorithm" → defaultAlgorithm.label,
                "step" → i,
                "zero-size" → zeroSize.toString,
                "plateau-size" → plateauSize.toString,
                "accumulated" → Map(
                  "size" → accSize.toString,
                  "nr-shapes" → i,
                  "average-increment" → ( ( accSize - zeroSize ) / i ).toString
                ),
                "next-size" → nextSize.toString
              )
            )

            countDown await 5.millis.dilated

            nextSize mustBe <( plateauSize * ( 1 + memoryAllowedMargin ) )

            ( next, nextSize )
          }
        }
      }
    }
  }
}
