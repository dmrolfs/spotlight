package spotlight.analysis.outlier.algorithm.statistical

import akka.actor.ActorSystem

import scala.annotation.tailrec
import scalaz.{-\/, Unzip, \/-}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.math3.stat.descriptive.{DescriptiveStatistics, StatisticalSummary}
import org.joda.{time => joda}
import org.mockito.Mockito._
import org.scalatest.Assertion
import org.typelevel.scalatest.{DisjunctionMatchers, DisjunctionValues}
import peds.commons.TryV
import peds.commons.log.Trace
import spotlight.analysis.outlier.RecentHistory
import spotlight.analysis.outlier.algorithm.{AlgorithmModule, AlgorithmModuleSpec, AlgorithmProtocol => P}
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 10/7/16.
  */
class GrubbsAlgorithmSpec
extends AlgorithmModuleSpec[GrubbsAlgorithmSpec]
with DisjunctionMatchers
with DisjunctionValues {
  private val trace = Trace[GrubbsAlgorithmSpec]

  override type Module = GrubbsAlgorithm.type
  override val defaultModule: Module = GrubbsAlgorithm


  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    logger.debug( "TEST ActorSystem: {}", system.name )
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AlgorithmFixture( _config, _system, _slug ) {
    override implicit val shapeOrdering: Ordering[TestShape] = new Ordering[TestShape] {
      override def compare( lhs: TestShape, rhs: TestShape ): Int = {
        val l = lhs.asInstanceOf[DescriptiveStatistics]
        val r = rhs.asInstanceOf[DescriptiveStatistics]
        if ( l.getN == r.getN && l.getMean == r.getMean && l.getStandardDeviation == r.getStandardDeviation ) 0
        else ( r.getN - l.getN ).toInt
      }
    }

    def assertState( result: Option[CalculationMagnetResult] )( s: module.State ): Assertion = {
      logger.info( "assertState result:[{}]", result )

      s.movingStatistics.getN mustBe ( if ( result.isDefined ) result.get.statistics.get.getN else 0 )

      val eh = for {
        r <- result
        stats <- r.statistics
      } yield ( stats.getMean, stats.getStandardDeviation )

      eh match {
        case None => {
          assert( s.movingStatistics.getMean.isNaN )
          assert( s.movingStatistics.getStandardDeviation.isNaN )
        }

        case Some( (expected, standardDeviation) ) => {
          s.movingStatistics.getMean mustBe expected
          s.movingStatistics.getStandardDeviation mustBe standardDeviation
        }
      }
    }
  }


  def stateFor( stats: DescriptiveStatistics ): GrubbsAlgorithm.State = GrubbsAlgorithm.State( null, null, stats )
  def stateFor( values: Seq[Double] ): GrubbsAlgorithm.State = {
    val stats = values.foldLeft( new DescriptiveStatistics(RecentHistory.LastN) ){ (acc, v) => acc.addValue(v); acc }
    stateFor( stats )
  }

  override def calculateControlBoundaries(
    points: Seq[DataPoint],
    tolerance: Double,
    lastPoints: Seq[DataPoint]
  ): Seq[ThresholdBoundary] = {
    implicit val context = mock[GrubbsAlgorithm.Context]
    when( context.alpha ) thenReturn 0.05

    val allPoints = lastPoints ++ points
    val state = stateFor( allPoints map { _.value } )
    val score = state.grubbsScore.toOption.get

    @tailrec def loop( pts: List[DataPoint], history: Array[Double], acc: Seq[ThresholdBoundary] ): Seq[ThresholdBoundary] = {
      pts match {
        case Nil => acc

        case p :: tail => {
          val stats = new DescriptiveStatistics( history )
          val mean = stats.getMean
          val stddev = stats.getStandardDeviation
          val control = ThresholdBoundary.fromExpectedAndDistance(
            p.timestamp,
            expected = mean,
            distance = math.abs( tolerance * score * stddev )
          )

          logger.debug( "EXPECTED for point:[{}] Control [{}] = [{}]", (p.timestamp.getMillis, p.value), acc.size.toString, control )
          loop( tail, history :+ p.value, acc :+ control )
        }
      }
    }

    loop( points.toList, lastPoints.map {_.value}.toArray, Seq.empty[ThresholdBoundary] )
  }

  implicit def fromAlpha( alpha: Double ): CalculationMagnet = new CalculationMagnet {
    case class Result(
      override val underlying: StatisticalSummary,
      override val timestamp: joda.DateTime,
      override val tolerance: Double,
      score: TryV[Double]
    ) extends CalculationMagnetResult {
      override type Value = StatisticalSummary
      override def statistics: Option[StatisticalSummary] = Option( underlying )
      override def thresholdBoundary: ThresholdBoundary = {
        score match {
          case \/-( s ) => {
            ThresholdBoundary.fromExpectedAndDistance(
              timestamp,
              expected = underlying.getMean,
              distance = math.abs( tolerance * s * underlying.getStandardDeviation )
            )
          }

          case -\/( ex: AlgorithmModule.InsufficientDataSize ) => ThresholdBoundary empty timestamp

          case -\/( ex ) => throw ex
        }
      }
    }

    override def apply( points: Seq[DataPoint] ): Result = {
      implicit val context = mock[GrubbsAlgorithm.Context]
      when( context.alpha ) thenReturn alpha

      val stats = points.foldLeft( new DescriptiveStatistics(RecentHistory.LastN) ){ (s, p) => s.addValue( p.value ); s }
      val state = stateFor( stats )
      logger.info( "TEST: SCORE = [{}]", state.grubbsScore )
      Result( underlying = stats, timestamp = points.last.timestamp, tolerance = 3.0, score = state.grubbsScore )
    }
  }

//  implicit def fromAlpha( alpha: Double ): CalculationMagnet = new CalculationMagnet {
//    case class Result( override val value: DescriptiveStatistics ) extends CalculationMagnetResult {
//      override type Value = DescriptiveStatistics
//      override def N: Long = value.getN
//      override def expected: Double = value.getMean
//      override def height: Double = value.getStandardDeviation
//    }
//
////    override def apply( values: Seq[Double] ): Result = Result( new DescriptiveStatistics(values.toArray) )
////    override def apply( points: Seq[DataPoint] ): Result = {
////      val tolerance = 3.0
////      val (last, current) = points splitAt ( points.size - 1 )
////      Result( calculateControlBoundaries(current, tolerance, last).head, points.size, tolerance )
////    }
//    override def apply( points: Seq[DataPoint] ): Result = {
//      Result( points.foldLeft( new DescriptiveStatistics(RecentHistory.LastN) ){ (acc, p) => acc.addValue(p.value); acc } )
//    }
//  }
//todo: original impl for reference
//  def calculateControlBoundaries(
//                                  points: Seq[DataPoint],
//                                  tolerance: Double,
//                                  lastPoints: Seq[DataPoint] = Seq.empty[DataPoint]
//                                ): Seq[ThresholdBoundary] = {
//    val combined = lastPoints.map{ _.value } ++ points.map{ _.value }
//    val N = combined.size
//    val stats = new DescriptiveStatistics( combined.toArray )
//    val mean = stats.getMean
//    val stddev = stats.getStandardDeviation
//    log.debug( "expected: combined:[{}]", combined.mkString(",") )
//    log.debug( "expected statistics-N:[{}] size:[{}] mean:[{}] sttdev:[{}]", stats.getN.toString, N.toString, mean.toString, stddev.toString)
//
//    val tdist = new TDistribution( math.max( N - 2, 1 ) )
//    val threshold = tdist.inverseCumulativeProbability( 0.05 / (2.0 * N) )
//    val thresholdSquared = math.pow( threshold, 2 )
//    val grubbs = ((N - 1) / math.sqrt(N)) * math.sqrt( thresholdSquared / (N - 2 + thresholdSquared) )
//    log.debug( "expected threshold^2:[{}] grubbs:[{}]", thresholdSquared.toString, grubbs.toString )
//
//    val prevTimestamps = lastPoints.map{ _.timestamp }.toSet
//
//    points
//    .filter { p => !prevTimestamps.contains(p.timestamp) }
//    .map { case DataPoint(ts, _) => ThresholdBoundary.fromExpectedAndDistance( ts, mean, tolerance * grubbs * stddev ) }
//  }
//}


  bootstrapSuite()
  analysisStateSuite()


//  //todo consider moving to AlgorithmModuleSpec as default impl
//  case class Expected( isOutlier: Boolean, floor: Option[Double], expected: Option[Double], ceiling: Option[Double] ) {
//    def stepResult( timestamp: joda.DateTime ): Option[(Boolean, ThresholdBoundary)] = {
//      Some( (isOutlier, ThresholdBoundary(timestamp, floor, expected, ceiling)) )
//    }
//
//    def stepResult( timestamp: Long ): Option[(Boolean, ThresholdBoundary)] = stepResult( new joda.DateTime(timestamp) )
//  }
//
//  def makeExpected(
//    data: Seq[Double],
//    outliers: Seq[Boolean]
//  )(
//    implicit state: GrubbsAlgorithm.State,
//    context: GrubbsAlgorithm.Context
//  ): Seq[Expected] = trace.block( "makeExpected" ){
//    logger.debug( "data:[{}]", data.mkString(", ") )
//
//    @tailrec def loop( values: List[(Double, Boolean)], acc: Seq[Expected] = Seq.empty[Expected] ): Seq[Expected] = {
//      val stats = state.movingStatistics
//      values match {
//        case Nil => acc
//
//        case (v, o) :: t => {
//
//          val (floor, expected, ceiling) = {
//            val threshold = for {
//              mean <- if ( stats.getMean.isNaN ) None else Some( stats.getMean )
//              sttdev <- if ( stats.getStandardDeviation.isNaN ) None else Some( stats.getStandardDeviation )
//              score <- state.grubbsScore.toOption
//            } yield {
//              val height = math.abs( context.tolerance * score * sttdev )
//              ( mean - height, ( mean, mean + height ) )
//            }
//
//            import scalaz.std.option._
//            Unzip[Option].unzip3( threshold )
//          }
//
//          stats addValue v
//          val e = Expected( isOutlier = o, floor = floor, expected = expected, ceiling = ceiling )
//          logger.debug( "Grubbs[{}]: expected:[{}]", stats.getN.toString, e )
//
//          loop( t, acc :+ e )
//        }
//      }
//    }
//
//    val combined = data zip outliers
//    loop( combined.toList )
//  }


  s"${defaultModule.algorithm.label.name} algorithm" should {
    "change configuration" in { f: Fixture =>
      import f._
      import akka.pattern.ask
      import scala.concurrent.duration._
      import akka.testkit._

      whenReady( ( aggregate ? P.GetStateSnapshot(id) ).mapTo[P.StateSnapshot], timeout(5.seconds.dilated) ){ actual =>
        actualVsExpectedState( actual.snapshot, None )
      }

      val c1 = ConfigFactory.parseString( "Grubbs { sample-size = 7 }" )
      aggregate ! P.UseConfiguration( id, c1 )
      whenReady( ( aggregate ? P.GetStateSnapshot(id) ).mapTo[P.StateSnapshot], timeout(5.seconds.dilated) ){ actual =>
        val expected = module.State( id, "", new DescriptiveStatistics(), 7 )
        actualVsExpectedState( actual.snapshot, Some(expected) )
        actual.snapshot.get.asInstanceOf[GrubbsAlgorithm.State].sampleSize mustBe 7
      }

      val c2 = ConfigFactory.parseString( "Grubbs { sample-size = 13 }" )
      aggregate ! P.UseConfiguration( id, c2 )
      whenReady( ( aggregate ? P.GetStateSnapshot(id) ).mapTo[P.StateSnapshot], timeout(5.seconds.dilated) ){ actual =>
        val expected = module.State( id, "", new DescriptiveStatistics(), 13 )
        actualVsExpectedState( actual.snapshot, Some(expected) )
        actual.snapshot.get.asInstanceOf[GrubbsAlgorithm.State].sampleSize mustBe 13
      }
    }

    //todo define and use smaller fixture
    "calculate grubbs score" in { f: Fixture =>
      implicit val ctx = mock[GrubbsAlgorithm.Context]
      when( ctx.alpha ) thenReturn 0.05

      def caller( size: Int ): GrubbsAlgorithm.State = {
        val d = Array( 199.31, 199.53, 200.19, 200.82, 201.92, 201.95, 202.18, 245.57 )
        GrubbsAlgorithm.State( null, null, new DescriptiveStatistics(d take size) )
      }

      for ( i <- 0 until 6 ) caller( i ).grubbsScore.isLeft mustBe true
      caller( 7 ).grubbsScore.value mustBe ( 2.0199684174 +- 0.00001 )
      caller( 8 ).grubbsScore.value mustBe ( 2.1266465543 +- 0.00001 )
    }

    "step to find anomalies from flat signal" in { f: Fixture =>
      import f._

      val algorithm = module.algorithm
      implicit val testContext = mock[module.Context]
      when( testContext.alpha ) thenReturn 0.05
      when( testContext.tolerance ) thenReturn 3.0

      implicit val testState = stateFor( Seq() )
      val testShape = testState.movingStatistics

      def advanceWith( v: Double ): Unit = {
        logger.debug( "Advancing with [{}]", v.toString )
        testShape addValue v
      }

      val data = Seq[Double]( 1, 1, 1, 1, 1, 1, 1, 1, 1, 1000 )
      val expected = Seq(
        Expected( false, None, None, None ),
        Expected( false, None, None, None ),
        Expected( false, None, None, None ),
        Expected( false, None, None, None ),
        Expected( false, None, None, None ),
        Expected( false, None, None, None ),
        Expected( false, None, None, None ),
        Expected( false, Some(1.0), Some(1.0), Some(1.0) ),
        Expected( false, Some(1.0), Some(1.0), Some(1.0) ),
        Expected( true, Some(1.0), Some(1.0), Some(1.0) )
      )
      val dataAndExpected: Seq[(Double, Expected)] = data.zip( expected )

      for {
        ( (value, expected), i ) <- dataAndExpected.zipWithIndex
      } {
        testShape.getN mustBe i
        val ts = nowTimestamp.plusSeconds( 10 * i )
        val actualPair = algorithm.step( ts.getMillis.toDouble, value )
        val expectedPair = expected.stepResult( i )
//        val expectedPair = expected.stepResult( ts.getMillis )
        (i, actualPair) mustBe (i, expectedPair)
        advanceWith( value )
      }
    }

    "find outliers across two batches" taggedAs WIP in { f: Fixture =>
      import f._
      val dp1 = makeDataPoints( values = Seq.fill( 10 ){ 1.0 }, timeWiggle = (0.98, 1.02), valueWiggle = (0.99, 1.01) )
      val s1 = spike( scope.topic, dp1 )()
      val h1 = historyWith( None, s1 )
      val (e1, r1) = makeExpected( 0.05 )( points = s1.points, outliers = Seq.fill( s1.size - 1 ){ false } :+ true )
      evaluate(
        hint = "first",
        series = s1,
        history = h1,
        expectedResults = e1,
        assertStateFn = assertState( r1 )( _: module.State )
      )

      val dp2 = makeDataPoints(
        values = Seq.fill( 10 ){ 1.0 },
        start = dp1.last.timestamp.plusSeconds( 10 ),
        timeWiggle =  (0.97, 1.03)
      )
      val s2 = spike( scope.topic, dp2 )( 0 )
      val h2 = historyWith( Option(h1.recordLastPoints(s1.points)), s2 )
      val (e2, r2) = makeExpected( 0.05 )(
        points = s2.points,
        outliers = Seq.fill( s2.size ){ false },
        history = h2.lastPoints map { _.toDataPoint }
      )
      evaluate(
        hint = "second",
        series = s2,
        history = h2,
        expectedResults = e2,
        assertStateFn = assertState( r2 )( _: module.State )
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