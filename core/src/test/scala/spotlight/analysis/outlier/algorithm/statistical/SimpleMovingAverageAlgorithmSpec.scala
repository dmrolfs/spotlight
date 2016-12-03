package spotlight.analysis.outlier.algorithm.statistical

import scala.annotation.tailrec
import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.mockito.Mockito._
import org.scalatest.Assertion
import spotlight.analysis.outlier.algorithm.{AlgorithmModuleSpec, AlgorithmProtocol => P}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/9/16.
  */
class SimpleMovingAverageAlgorithmSpec extends AlgorithmModuleSpec[SimpleMovingAverageAlgorithmSpec] {

  override type Module = SimpleMovingAverageAlgorithm.type
  override val defaultModule: Module = SimpleMovingAverageAlgorithm


  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AlgorithmFixture( _config, _system, _slug ) {
    override implicit val shapeOrdering: Ordering[module.Shape] = new Ordering[module.Shape] {
      override def compare( lhs: TestShape, rhs: TestShape ): Int = {
        if ( lhs == rhs ) 0
        else {
          val l = lhs.asInstanceOf[SimpleMovingAverageAlgorithm.Shape]
          val r = rhs.asInstanceOf[SimpleMovingAverageAlgorithm.Shape]
          ( l.getN - r.getN ).toInt
        }
      }
    }

    override def expectedUpdatedState( state: module.State, event: P.Advanced ): module.State = {
      val historicalStatsLens = module.State.shapeLens
      val s = super.expectedUpdatedState( state, event ).asInstanceOf[SimpleMovingAverageAlgorithm.State]
      val stats = s.statistics.copy()
      stats addValue event.point.value
      historicalStatsLens.set( s )( stats )
    }

    def assertState( result: Option[CalculationMagnetResult] )( s: module.State ): Assertion = {
      logger.info( "assertState result:[{}]", result )

      val expectedN = for { r <- result; s <- r.statistics } yield s.getN

      s.statistics.getN mustBe ( if ( expectedN.isDefined  ) expectedN.get else 0 )

      val msd = for {
        r <- result
        s <- r.statistics
      } yield ( s.getMean, s.getStandardDeviation )

      msd match {
        case None => {
          assert( s.statistics.getMean.isNaN )
          assert( s.statistics.getStandardDeviation.isNaN )
        }

        case Some( (expected, standardDeviation) ) => {
          s.statistics.getMean mustBe expected
          s.statistics.getStandardDeviation mustBe standardDeviation
        }
      }
    }
  }


  override def calculateControlBoundaries(
    points: Seq[DataPoint],
    tolerance: Double,
    lastPoints: Seq[DataPoint]
  ): Seq[ThresholdBoundary] = {
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
            distance = math.abs( tolerance * stddev )
          )
          logger.debug( "EXPECTED for point:[{}] Control [{}] = [{}]", (p.timestamp.getMillis, p.value), acc.size.toString, control )
          loop( tail, history :+ p.value, acc :+ control )
        }
      }
    }

    loop( points.toList, lastPoints.map{ _.value }.toArray, Seq.empty[ThresholdBoundary] )
  }


  bootstrapSuite()
  analysisStateSuite()


  s"${defaultModule.algorithm.label.name} algorithm" should {
    "step to find anomalies from flat signal" in { f: Fixture =>
      import f._

      logger.info( "************** TEST NOW ************" )
      val algorithm = module.algorithm
      implicit val testContext = mock[module.Context]
      val testHistory: module.Shape = module.makeShape()

      implicit val testState = mock[module.State]
      when( testState.statistics ).thenReturn( testHistory )

      def advanceWith( v: Double ): Unit = testHistory addValue v

      val data = Seq[Double](1, 1, 1, 1, 1000)
      val expected = Seq(
        Expected( false, None, None, None ),
        Expected( false, Some(1.0), Some(1.0), Some(1.0) ),
        Expected( false, Some(1.0), Some(1.0), Some(1.0) ),
        Expected( false, Some(1.0), Some(1.0), Some(1.0) ),
        Expected( true, Some(1.0), Some(1.0), Some(1.0) )
      )
      val dataAndExpected: Seq[(Double, Expected)] = data.zip( expected )

      for {
        ( (value, expected), i ) <- dataAndExpected.zipWithIndex
      } {
        testHistory.getN mustBe i
        val ts = nowTimestamp.plusSeconds( 10 * i )
        algorithm.step( ts.getMillis.toDouble, value ) mustBe expected.stepResult( i )
        advanceWith( value )
      }
    }

    "happy path process two batches" in { f: Fixture =>
      import f._

      logger.info( "****************** TEST NOW ****************" )

      val dp1 = makeDataPoints( values = Seq.fill( 5 ){ 1.0 }, timeWiggle = (0.97, 1.03) )
      val s1 = spike( scope.topic, dp1 )()
      val h1 = historyWith( None, s1 )
      val (e1, r1) = makeExpected( NoHint )( points = s1.points, outliers = Seq(false, false, false, false, true) )
      evaluate(
        hint = "first",
        series = s1,
        history = h1,
        expectedResults = e1,
        assertStateFn = assertState( r1 )( _: module.State )
      )

      val dp2 = makeDataPoints(
        values = Seq.fill( 5 ){ 1.0 },
        start = dp1.last.timestamp.plusSeconds( 10 ),
        timeWiggle = (0.97, 1.03)
      )
      val s2 = spike( scope.topic, dp2 )( 0 )
      val h2 = historyWith( Option(h1.recordLastPoints(s1.points)), s2 )
      val (e2, r2) = makeExpected( NoHint )(
        points = s2.points,
        outliers = Seq(false, false, false, false, false),
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
  }
}
