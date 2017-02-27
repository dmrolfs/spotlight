package spotlight.analysis.algorithm.statistical

import scala.annotation.tailrec
import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.apache.commons.math3.stat.descriptive.{ DescriptiveStatistics, SummaryStatistics }
import org.mockito.Mockito._
import org.scalatest.Assertion
import spotlight.analysis.algorithm.{ Advancing, AlgorithmSpec, AlgorithmProtocol ⇒ P }
import spotlight.model.timeseries._
import spotlight.analysis.algorithm.summaryStatisticsAdvancing

/** Created by rolfsd on 6/9/16.
  */
class SimpleMovingAverageAlgorithmSpec extends AlgorithmSpec[SummaryStatistics] {

  override type Algo = SimpleMovingAverageAlgorithm.type
  override val defaultAlgorithm: Algo = SimpleMovingAverageAlgorithm

  override val memoryPlateauNr: Int = 70

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AlgorithmFixture( _config, _system, _slug ) {

    override implicit val shapeOrdering: Ordering[Shape] = new Ordering[Shape] {
      override def compare( lhs: TestShape, rhs: TestShape ): Int = {
        if ( lhs == rhs ) 0
        else {
          val l = lhs.asInstanceOf[SimpleMovingAverageAlgorithm.Shape]
          val r = rhs.asInstanceOf[SimpleMovingAverageAlgorithm.Shape]
          ( l.getN - r.getN ).toInt
        }
      }
    }

    override def expectedUpdatedShape( shape: TestShape, event: P.Advanced ): TestShape = {
      val newShape = shape.copy()
      newShape addValue event.point.value
      newShape
    }

    def assertShape( result: Option[CalculationMagnetResult], topic: Topic )( s: TestShape ): Assertion = {
      logger.info( "assertState result:[{}]", result )

      val expectedStats = for { r ← result; s ← r.statistics } yield ( s.getN, s.getMean, s.getStandardDeviation )

      expectedStats match {
        case None ⇒ {
          assert( s.getMean.isNaN )
          assert( s.getStandardDeviation.isNaN )
        }

        case Some( ( size, mean, standardDeviation ) ) ⇒ {
          s.getN mustBe size
          s.getMean mustBe mean
          s.getStandardDeviation mustBe standardDeviation
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
        case Nil ⇒ acc

        case p :: tail ⇒ {
          val stats = new DescriptiveStatistics( history )
          val mean = stats.getMean
          val stddev = stats.getStandardDeviation
          val control = ThresholdBoundary.fromExpectedAndDistance(
            p.timestamp,
            expected = mean,
            distance = math.abs( tolerance * stddev )
          )
          logger.debug( "EXPECTED for point:[{}] Control [{}] = [{}]", ( p.timestamp.getMillis, p.value ), acc.size.toString, control )
          loop( tail, history :+ p.value, acc :+ control )
        }
      }
    }

    loop( points.toList, lastPoints.map { _.value }.toArray, Seq.empty[ThresholdBoundary] )
  }

  bootstrapSuite()
  analysisStateSuite()

  s"${defaultAlgorithm.label} algorithm" should {
    "step to find anomalies from flat signal" in { f: Fixture ⇒
      import f._

      logger.info( "************** TEST NOW ************" )
      val algorithm = defaultAlgorithm
      implicit val testContext = mock[defaultAlgorithm.Context]
      val testShape: Shape = shapeless.the[Advancing[Shape]].zero( None )

      implicit val testState = mock[State]
      when( testState.shapes ).thenReturn( Map( scope.topic → testShape ) )

      def advanceWith( v: Double ): Unit = testShape addValue v

      val data = Seq[Double]( 1, 1, 1, 1, 1000 )
      val expected = Seq(
        Expected( false, None, None, None ),
        Expected( false, Some( 1.0 ), Some( 1.0 ), Some( 1.0 ) ),
        Expected( false, Some( 1.0 ), Some( 1.0 ), Some( 1.0 ) ),
        Expected( false, Some( 1.0 ), Some( 1.0 ), Some( 1.0 ) ),
        Expected( true, Some( 1.0 ), Some( 1.0 ), Some( 1.0 ) )
      )
      val dataAndExpected: Seq[( Double, Expected )] = data.zip( expected )

      for {
        ( ( value, expected ), i ) ← dataAndExpected.zipWithIndex
      } {
        testShape.getN mustBe i
        val ts = nowTimestamp.plusSeconds( 10 * i )
        algorithm.step( ( ts.getMillis.toDouble, value ), testShape ) mustBe expected.stepResult( i )
        advanceWith( value )
      }
    }

    "happy path process two batches" in { f: Fixture ⇒
      import f._

      logger.info( "****************** TEST NOW ****************" )

      val dp1 = makeDataPoints( values = Seq.fill( 5 ) { 1.0 }, timeWiggle = ( 0.97, 1.03 ) )
      val s1 = spike( scope.topic, dp1 )()
      val h1 = historyWith( None, s1 )
      val ( e1, r1 ) = makeExpected( NoHint )( points = s1.points, outliers = Seq( false, false, false, false, true ) )
      evaluate(
        hint = "first",
        algorithmAggregateId = id,
        series = s1,
        history = h1,
        expectedResults = e1,
        assertShapeFn = assertShape( r1, scope.topic )( _: TestShape )
      )

      val dp2 = makeDataPoints(
        values = Seq.fill( 5 ) { 1.0 },
        start = dp1.last.timestamp.plusSeconds( 10 ),
        timeWiggle = ( 0.97, 1.03 )
      )
      val s2 = spike( scope.topic, dp2 )( 0 )
      val h2 = historyWith( Option( h1.recordLastPoints( s1.points ) ), s2 )
      val ( e2, r2 ) = makeExpected( NoHint )(
        points = s2.points,
        outliers = Seq( false, false, false, false, false ),
        history = h2.lastPoints map { _.toDataPoint }
      )
      evaluate(
        hint = "second",
        algorithmAggregateId = id,
        series = s2,
        history = h2,
        expectedResults = e2,
        assertShapeFn = assertShape( r2, scope.topic )( _: TestShape )
      )
    }
  }
}
