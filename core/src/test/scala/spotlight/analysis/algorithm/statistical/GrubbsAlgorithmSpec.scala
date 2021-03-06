package spotlight.analysis.algorithm.statistical

import scala.annotation.tailrec
import scala.math
import akka.actor.ActorSystem
import cats.syntax.validated._
import cats.scalatest.{ EitherMatchers, EitherValues }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.commons.math3.stat.descriptive.{ DescriptiveStatistics, StatisticalSummary }
import org.joda.{ time ⇒ joda }
import org.mockito.Mockito._
import org.scalatest.Assertion
import com.persist.logging._
import omnibus.commons._
import omnibus.commons.log.Trace
import spotlight.analysis.algorithm.{ Algorithm, AlgorithmSpec, InsufficientDataSize, AlgorithmProtocol ⇒ P }
import spotlight.model.timeseries._

/** Created by rolfsd on 10/7/16.
  */
class GrubbsAlgorithmSpec
    extends AlgorithmSpec[GrubbsShape]
    with EitherMatchers
    with EitherValues {
  private val trace = Trace[GrubbsAlgorithmSpec]

  override type Algo = GrubbsAlgorithm.type
  override val defaultAlgorithm: Algo = GrubbsAlgorithm

  override val memoryPlateauNr: Int = GrubbsShape.DefaultSlidingWindow

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    logger.debug( "TEST ActorSystem: {}", system.name )
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AlgorithmFixture( _config, _system, _slug ) {
    override implicit val shapeOrdering: Ordering[TestShape] = new Ordering[TestShape] {
      override def compare( lhs: TestShape, rhs: TestShape ): Int = {
        if ( lhs.N == rhs.N && lhs.mean == rhs.mean && lhs.standardDeviation == rhs.standardDeviation ) 0
        else ( rhs.N - lhs.N ).toInt
      }
    }

    override def expectedUpdatedShape( shape: TestShape, event: P.Advanced ): TestShape = {
      val expected = shape.copy()
      expected :+ event.point.value
    }

    def assertShape( result: Option[CalculationMagnetResult], topic: Topic )( s: TestShape ): Assertion = {
      logger.info( "assertState: \nresult:[{}]\ns:[{}]", result, s.toString )

      val expectedStats = for { r ← result; rs ← r.statistics } yield { ( rs.getN, rs.getMean, rs.getStandardDeviation ) }

      expectedStats match {
        case None ⇒ {
          assert( s.mean.isNaN )
          assert( s.standardDeviation.isNaN )
        }

        case Some( ( size, mean, standardDeviation ) ) ⇒ {
          log.info(
            Map(
              "@msg" → "assertState",
              "actual" → Map( "size" → s.N, "mean" → s.mean, "stddev" → s.standardDeviation ),
              "expected" → Map( "size" → size, "mean" → mean, "stddev" → standardDeviation )
            )
          )
          s.N mustBe size
          s.mean mustBe ( mean +- tol )
          s.standardDeviation mustBe ( standardDeviation +- tol )
        }
      }
    }
  }

  //  def shapeFor( stats: DescriptiveStatistics ): GrubbsShape = GrubbsShape( stats )
  def shapeFor( values: Seq[Double] ): GrubbsShape = values.foldLeft( GrubbsShape() ) { _ :+ _ }

  override def calculateControlBoundaries(
    points: Seq[DataPoint],
    tolerance: Double,
    lastPoints: Seq[DataPoint]
  ): Seq[ThresholdBoundary] = {
    implicit val context = mock[GrubbsAlgorithm.Context]
    when( context.alpha ) thenReturn 0.05

    val allPoints = lastPoints ++ points
    val shape = shapeFor( allPoints map { _.value } )
    val score = GrubbsAlgorithm.grubbsScore( shape ).unsafeGet

    @tailrec def loop( pts: List[DataPoint], history: Array[Double], acc: Seq[ThresholdBoundary] ): Seq[ThresholdBoundary] = {
      pts match {
        case Nil ⇒ acc

        case p :: tail ⇒ {
          val stats = new DescriptiveStatistics( history )
          val mean = stats.getMean
          val stddev = stats.getStandardDeviation
          val control = ThresholdBoundary.fromExpectedAndDistance(
            p.timestamp,
            expected = mean,
            distance = math.abs( tolerance * score * stddev )
          )

          logger.debug( "EXPECTED for point:[{}] Control [{}] = [{}]", ( p.timestamp.getMillis, p.value ), acc.size.toString, control )
          loop( tail, history :+ p.value, acc :+ control )
        }
      }
    }

    loop( points.toList, lastPoints.map { _.value }.toArray, Seq.empty[ThresholdBoundary] )
  }

  implicit def fromAlpha( alpha: Double ): CalculationMagnet = new CalculationMagnet {
    case class Result(
        override val underlying: StatisticalSummary,
        override val timestamp: joda.DateTime,
        override val tolerance: Double,
        score: ErrorOr[Double]
    ) extends CalculationMagnetResult {
      override type Value = StatisticalSummary
      override def statistics: Option[StatisticalSummary] = Option( underlying )
      override def thresholdBoundary: ThresholdBoundary = {
        score match {
          case Right( s ) ⇒ {
            ThresholdBoundary.fromExpectedAndDistance(
              timestamp,
              expected = underlying.getMean,
              distance = math.abs( tolerance * s * underlying.getStandardDeviation )
            )
          }

          case Left( ex: InsufficientDataSize ) ⇒ ThresholdBoundary empty timestamp

          case Left( ex ) ⇒ throw ex
        }
      }
    }

    override def apply( points: Seq[DataPoint], tolerance: Double = 3.0 ): Result = {
      implicit val context = mock[GrubbsAlgorithm.Context]
      when( context.alpha ) thenReturn alpha
      when( context.properties ) thenReturn ConfigFactory.empty

      val stats = points.foldLeft( new DescriptiveStatistics( GrubbsShape.DefaultSlidingWindow ) ) { ( s, p ) ⇒
        s.addValue( p.value )
        s
      }
      val shape = shapeFor( points.map { _.value } )
      val grubbs = GrubbsAlgorithm.grubbsScore( shape )
      logger.info( "TEST: SCORE = [{}]", grubbs )
      Result( underlying = stats, timestamp = points.last.timestamp, tolerance = tolerance, score = grubbs )
    }
  }

  bootstrapSuite()
  analysisStateSuite()

  s"${defaultAlgorithm.label} algorithm" should {
    //    "change configuration" taggedAs WIP in { f: Fixture =>
    //      import f._
    //      import akka.pattern.ask
    //      import scala.concurrent.duration._
    //      import akka.testkit._
    //
    //      whenReady(
    //        ( aggregate ? P.GetTopicShapeSnapshot( id, scope.topic ) ).mapTo[P.TopicShapeSnapshot], timeout( 5.seconds.dilated )
    //      ){ actual =>
    //        actualVsExpectedShape( actual.snapshot.get, defaultAlgorithm.shapeCompanion.zero( Some(config) ) )
    //      }
    //
    //      val c1 = ConfigFactory.parseString( s"${module.algorithm.label.name} { sample-size = 7 }" )
    //      aggregate !+ P.UseConfiguration( id, c1 )
    //      whenReady( ( aggregate ? P.GetTopicShapeSnapshot( id ) ).mapTo[P.TopicShapeSnapshot], timeout( 5.seconds.dilated ) ){ actual =>
    //        val expected = module.State( id, "", new DescriptiveStatistics(), 7 )
    //        actualVsExpectedState( actual.snapshot, Some(expected) )
    //        actual.snapshot.get.asInstanceOf[GrubbsAlgorithm.State].sampleSize mustBe 7
    //      }
    //
    //      val c2 = ConfigFactory.parseString( s"${module.algorithm.label.name} { sample-size = 13 }" )
    //      aggregate ! P.UseConfiguration( id, c2 )
    //      whenReady( ( aggregate ? P.GetTopicShapeSnapshot( id ) ).mapTo[P.TopicShapeSnapshot], timeout( 5.seconds.dilated ) ){ actual =>
    //        val expected = module.State( id, "", new DescriptiveStatistics(), 13 )
    //        actualVsExpectedState( actual.snapshot, Some(expected) )
    //        actual.snapshot.get.asInstanceOf[GrubbsAlgorithm.State].sampleSize mustBe 13
    //      }
    //    }

    //todo define and use smaller fixture
    "calculate grubbs score" in { f: Fixture ⇒
      val minPopulation = 8
      val grubbsConfig = ConfigFactory.parseString(
        s"""
          |tail-average: 3
          |tolerance: 3
          |minimum-population: ${minPopulation}
        """.stripMargin
      )

      implicit val ctx = mock[GrubbsAlgorithm.Context]
      when( ctx.alpha ) thenReturn 0.05
      when( ctx.properties ) thenReturn grubbsConfig

      def caller( size: Int ): GrubbsShape = {
        val d = Array( 199.31, 199.53, 200.19, 200.82, 201.92, 201.95, 202.18, 245.57 )
        d.take( size ).foldLeft( GrubbsShape() ) { _ :+ _ }
      }

      def score( s: GrubbsShape ): ErrorOr[Double] = GrubbsAlgorithm.grubbsScore( s )

      for ( i ← 0 until minPopulation ) {
        val s = score( caller( i ) )
        log.info( Map( "@msg" → "grubbs score", "i" → i, "score" → s.toString ) )
        s.isLeft mustBe true
      }
      //      for ( i ← 0 until 7 ) score( caller( i ) ).value mustBe true
      //      score( caller( 7 ) ).value mustBe ( 2.0199684174 +- 0.00001 )
      score( caller( 8 ) ).value mustBe ( 2.1266465543 +- 0.00001 )
    }

    //    "step to find anomalies from flat signal" in { f: Fixture =>
    //      import f._
    //
    //      val algorithm = module.algorithm
    //      implicit val testContext = mock[module.Context]
    //      when( testContext.alpha ) thenReturn 0.05
    //      when( testContext.tolerance ) thenReturn 3.0
    //
    //      var shape = shapeFor( Seq() )
    //      val stats = shape.movingStatistics
    //
    //      def advanceWith( v: Double ): Unit = {
    //        logger.debug( "Advancing with [{}]", v.toString )
    //        val newStats = shape.movingStatistics.copy()
    //        newStats addValue v
    //        shape = shape.copy( movingStatistics = newStats )
    //      }
    //
    //      val data = Seq[Double]( 1, 1, 1, 1, 1, 1, 1, 1, 1, 1000 )
    //      val expected = Seq(
    //        Expected( false, None, None, None ),
    //        Expected( false, None, None, None ),
    //        Expected( false, None, None, None ),
    //        Expected( false, None, None, None ),
    //        Expected( false, None, None, None ),
    //        Expected( false, None, None, None ),
    //        Expected( false, None, None, None ),
    //        Expected( false, Some(1.0), Some(1.0), Some(1.0) ),
    //        Expected( false, Some(1.0), Some(1.0), Some(1.0) ),
    //        Expected( true, Some(1.0), Some(1.0), Some(1.0) )
    //      )
    //      val dataAndExpected: Seq[(Double, Expected)] = data.zip( expected )
    //
    //      for {
    //        ( (value, expected), i ) <- dataAndExpected.zipWithIndex
    //      } {
    //        shape.movingStatistics.getN mustBe i
    //        val ts = nowTimestamp.plusSeconds( 10 * i )
    //        val actualPair = algorithm.step( (ts.getMillis.toDouble, value), shape )
    //        val expectedPair = expected.stepResult( i )
    ////        val expectedPair = expected.stepResult( ts.getMillis )
    //        (i, actualPair) mustBe (i, expectedPair)
    //        advanceWith( value )
    //      }
    //    }

    "find outliers across two batches" in { f: Fixture ⇒
      import f._
      val dp1 = makeDataPoints( values = Seq.fill( 10 ) { 1.0 }, timeWiggle = ( 0.98, 1.02 ), valueWiggle = ( 0.99, 1.01 ) )
      val s1 = spike( scope.topic, dp1 )()
      val h1 = historyWith( None, s1 )
      val ( e1, r1 ) = makeExpected( 0.05 )( points = s1.points, outliers = Seq.fill( s1.size - 1 ) { false } :+ true )
      s1.size mustBe 10
      evaluate(
        hint = "first",
        algorithmAggregateId = id,
        series = s1,
        history = h1,
        expectedResults = e1,
        assertShapeFn = assertShape( r1, scope.topic )( _: TestShape )
      )

      val dp2 = makeDataPoints(
        values = Seq.fill( 10 ) { 1.0 },
        start = dp1.last.timestamp.plusSeconds( 10 ),
        timeWiggle = ( 0.97, 1.03 )
      )
      val s2 = spike( scope.topic, dp2 )( 0 )
      val h2 = historyWith( Option( h1.recordLastPoints( s1.points ) ), s2 )
      val ( e2, r2 ) = makeExpected( 0.05 )(
        points = s2.points,
        outliers = Seq.fill( s2.size ) { false },
        history = h2.lastPoints map { _.toDataPoint }
      )
      s2.size mustBe 10
      evaluate(
        hint = "second",
        algorithmAggregateId = id,
        series = s2,
        history = h2,
        expectedResults = e2,
        assertShapeFn = assertShape( r2, scope.topic )( _: TestShape )
      )
    }
    //
    //    "OLD find outliers across two batches" in { f: Fixture =>
    //      import f._
    //
    //      val algorithm = module.algorithm
    //      val algoProps = ConfigFactory.parseString(
    //         s"""
    //            |${algorithm.label.name} {
    //            |  tolerance = 3
    //            |  alpha = 0.05
    //            |}
    //         """.stripMargin
    //      )
    //
    //      implicit val testContext = mock[module.Context]
    //      when( testContext.alpha ) thenReturn 0.05
    //      when( testContext.tolerance ) thenReturn 3.0
    //
    //      implicit val testState = stateFor( Seq() )
    //
    //      def advanceWith( v: Double ): Unit = testState.movingStatistics addValue v
    //
    //      def evaluateSeries(
    //        state: GrubbsAlgorithm.State,
    //        context: GrubbsAlgorithm.Context
    //      )(
    //        series: TimeSeries,
    //        outliers: Seq[Boolean]
    //      ): Unit = {
    //        val start = state.movingStatistics.getN
    //        val expectedResults = makeExpected(
    //          series.points.map{ _.value },
    //          outliers
    //        )(
    //          stateFor( state.movingStatistics.copy() ),
    //          testContext
    //        )
    //
    //        for {
    //          ( (point, expected), i ) <- series.points.zip( expectedResults ).zipWithIndex
    //        } {
    //          val pos = start + i
    //          logger.debug( "TEST: Grubbs[{}]: point:[{}] expected:[{}]", pos.toString, point, expected )
    //          testState.movingStatistics.getN mustBe pos
    //          val actualPair = algorithm step point
    //          val expectedPair = expected stepResult point.timestamp
    //          logger.debug( "Grubbs[{}]: actual  :[{}]  point:[{}]", pos.toString, actualPair, point.value.toString )
    //          logger.debug( "Grubbs[{}]: expected:[{}]", pos.toString, expectedPair )
    //          logger.debug( "-----------------" )
    //          ( pos, actualPair ) mustBe ( pos, expectedPair )
    //          advanceWith( point.value )
    //        }
    //      }
    //
    //      val eval = evaluateSeries( testState, testContext )_
    //
    //      val flatline1 = makeDataPoints(
    //        values = IndexedSeq.fill( 10 )( 1.0 ).to[scala.collection.immutable.IndexedSeq],
    //        timeWiggle = (0.98, 1.02),
    //        valueWiggle = (0.99, 1.01)
    //      )
    //
    //      val series1 = spike( "TestTopic", flatline1 )()
    //      eval( series1, Seq.fill( series1.size - 1){ false } :+ true )
    //
    //      val flatline2 = makeDataPoints(
    //        values = Seq.fill( 10 ){ 1.0 },
    //        start = flatline1.last.timestamp.plusSeconds( 10 ),
    //        timeWiggle = (0.98, 1.02)
    //      )
    //      val series2 = spike( f.scope.id.topic, flatline2, 1000 )( 0 )
    //      eval( series2, Seq.fill( series2.size ){ false } )
    //    }

  }
}

//    "detect outlier through series of micro events" in { f: Fixture =>
//      import f._
//
//      def detectUsing( series: TimeSeries, history: HistoricalStatistics ): DetectUsing = {
//        DetectUsing(
//          algorithm = algoS,
//          payload = OutlierDetectionMessage( series, plan, subscriber.ref ).toOption.get,
//          history = history,
//          properties = algProps
//        )
//      }
//
//      implicit val sender = aggregator.ref
//
//      val analyzer = TestActorRef[GrubbsAnalyzer]( GrubbsAnalyzer.props( router.ref ) )
//      analyzer ! DetectionAlgorithmRouter.AlgorithmRegistered( algoS )
//
//      val topic = "test.topic"
//      val start = joda.DateTime.now
//      val rnd = new RandomDataGenerator
//
//      @tailrec def loop( i: Int, left: Int, previous: Option[(TimeSeries, HistoricalStatistics)] = None ): Unit = {
//        log.debug( ">>>>>>>>>  TEST-LOOP( i:[{}] left:[{}]", i, left )
//        val dt = start plusSeconds (10 * i)
//        val v = if ( left == 0 ) 1000.0 else rnd.nextUniform( 0.99, 1.01, true )
//        val s = TimeSeries( topic, Seq( DataPoint(dt, v) ) )
//        val h = {
//          previous
//          .map { case (ps, ph) => s.points.foldLeft( ph recordLastPoints ps.points ) { (acc, p) => acc :+ p } }
//          .getOrElse { HistoricalStatistics.fromActivePoints( s.points, false ) }
//        }
//        analyzer ! detectUsing( s, h )
//
//        val expected: PartialFunction[Any, Unit] = {
//          if ( left == 0 ) {
//            case m: SeriesOutliers => {
//              m.algorithms mustBe Set( algoS )
//              m.source mustBe s
//              m.hasAnomalies mustBe true
//              m.outliers mustBe s.points
//            }
//          } else {
//            case m: NoOutliers => {
//              m.algorithms mustBe Set( algoS )
//              m.source mustBe s
//              m.hasAnomalies mustBe false
//            }
//          }
//        }
//
//        aggregator.expectMsgPF( 5.seconds.dilated, s"point-$i" )( expected )
//
//        if ( left == 0 ) () else loop( i + 1, left - 1, Some( (s, h) ) )
//      }
//
//      loop( 0, 125 )
//    }
