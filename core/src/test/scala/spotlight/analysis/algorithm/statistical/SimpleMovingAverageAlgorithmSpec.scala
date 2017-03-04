package spotlight.analysis.algorithm.statistical

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.annotation.tailrec
import akka.actor.ActorSystem
import akka.pattern.ask
import com.typesafe.config.{ Config, ConfigFactory }
import com.persist.logging._
import omnibus.akka.envelope._
import org.apache.commons.math3.stat.descriptive.{ DescriptiveStatistics, SummaryStatistics }
import org.mockito.Mockito._
import org.scalatest.Assertion
import spotlight.analysis.{ DetectOutliersInSeries, DetectUsing }
import spotlight.analysis.algorithm.{ Advancing, AlgorithmSpec, AlgorithmProtocol ⇒ AP }
import spotlight.model.timeseries._

/** Created by rolfsd on 6/9/16.
  */
class SimpleMovingAverageAlgorithmSpec extends AlgorithmSpec[SimpleMovingAverageShape] {

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
          ( l.N - r.N ).toInt
        }
      }
    }

    override def expectedUpdatedShape( shape: TestShape, event: AP.Advanced ): TestShape = {
      val newShape = shape.copy()
      newShape :+ event.point.value
      newShape
    }

    def assertShape( result: Option[CalculationMagnetResult], topic: Topic )( s: TestShape ): Assertion = {
      val expectedStats = for { r ← result; s ← r.statistics } yield ( s.getN, s.getMean, s.getStandardDeviation )

      log.info(
        Map(
          "@msg" → "assertState",
          "expected" → result.toString,
          "shape" → s.toString,
          "N" → Map( "actual" → s.N.toString, "expected" → expectedStats.map { _._1 }.toString ),
          "mean" → Map( "actual" → s.mean.toString, "expected" → expectedStats.map { _._2 }.toString ),
          "stdev" → Map( "actual" → s.standardDeviation.toString, "expected" → expectedStats.map { _._3 }.toString )
        )
      )

      expectedStats match {
        case None ⇒ {
          assert( s.mean.isNaN )
          assert( s.standardDeviation.isNaN )
        }

        case Some( ( size, mean, standardDeviation ) ) ⇒ {
          s.N mustBe size
          s.mean mustBe mean
          s.standardDeviation mustBe standardDeviation
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
      when( testContext.properties ) thenReturn { ConfigFactory.empty }
      val testShape: Shape = shapeless.the[Advancing[Shape]].zero( None )

      implicit val testState = mock[State]
      when( testState.shapes ).thenReturn( Map( scope.topic → testShape ) )

      def advanceWith( v: Double ): Unit = testShape :+ v

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
        testShape.N mustBe i
        val ts = nowTimestamp.plusSeconds( 10 * i )
        algorithm.step( ( ts.getMillis.toDouble, value ), testShape ) mustBe expected.stepResult( i )
        advanceWith( value )
      }
    }

    "verify points case from OutlierScoringModel spec" in { f: Fixture ⇒
      import f._

      val minPopulation = 2
      val smaConfig = ConfigFactory.parseString(
        s"""
          |tail-average: 3
          |tolerance: 3
          |minimum-population: ${minPopulation}
        """.stripMargin
      )

      when( plan.algorithms ) thenReturn { Map( defaultAlgorithm.label → smaConfig ) }

      val points: Seq[Double] = Seq(
        9.46, 9.9, 11.6, 14.5, 17.3, 19.2, 18.4, 14.5, 12.2, 10.8, 8.58,
        8.36, 8.58, 7.5, 7.1, 7.3, 7.71, 8.14, 8.14, 7.1, 7.5,
        7.1, 7.1, 7.3, 7.71, 8.8, 9.9, 14.2, 18.8, 25.2, 31.5,
        22, 24.1, 39.2
      )

      val ts = TimeSeries( "foo.bar", makeDataPoints( points ) )
      log.debug( Map( "@msg" → "timeseries", "values" → ts.points.map { _.value }.mkString( "[", ", ", "]" ) ) )
      val h = historyWith( None, ts )
      val ( e, r ) = makeExpected( NoHint )(
        points = ts.points,
        outliers = explodeOutlierIdentifiers( points.size, Set( 30 ) ),
        minimumPopulation = minPopulation
      )

      evaluate(
        hint = "scoring model test",
        algorithmAggregateId = id,
        series = ts,
        history = h,
        expectedResults = e,
        assertShapeFn = assertShape( r, ts.topic )( _: TestShape )
      )
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

      log.info(
        Map(
          "@msg" → "#TEST: after run 1",
          "shape" → Await.result( ( aggregate ? AP.GetTopicShapeSnapshot( id, scope.topic ) ).mapTo[AP.TopicShapeSnapshot], 5.seconds ).toString
        )
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

      log.info(
        Map(
          "@msg" → "#TEST: before run 2",
          "shape" → Await.result( ( aggregate ? AP.GetTopicShapeSnapshot( id, scope.topic ) ).mapTo[AP.TopicShapeSnapshot], 5.seconds ).toString,
          "history-size" → h2.N,
          "series-size" → s2.size,
          "expected-size" → e2.size,
          "magnet-results" → r2.toString
        )
      )

      evaluate(
        hint = "second",
        algorithmAggregateId = id,
        series = s2,
        history = h2,
        expectedResults = e2,
        assertShapeFn = assertShape( r2, scope.topic )( _: TestShape )
      )

      log.info(
        Map(
          "@msg" → "#TEST: after run 2",
          "shape" → Await.result( ( aggregate ? AP.GetTopicShapeSnapshot( id, scope.topic ) ).mapTo[AP.TopicShapeSnapshot], 5.seconds ).toString
        )
      )
    }

    "validate size after two calls" taggedAs WIP in { f: Fixture ⇒
      import f._
      val size = 1
      log.info( "---------  BUILDING REQUEST 1..." )
      val dp1 = makeDataPoints( values = Seq.fill( size ) { 1.0 }, timeWiggle = ( 0.97, 1.03 ) )
      val s1 = spike( scope.topic, dp1 )()
      val h1 = historyWith( None, s1 )

      log.info( "---------  SENDING REQUEST 1..." )
      aggregate.sendEnvelope(
        DetectUsing(
          targetId = id,
          algorithm = defaultAlgorithm.label,
          payload = DetectOutliersInSeries( s1, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
          history = h1,
          properties = plan.algorithms.getOrElse( defaultAlgorithm.label, emptyConfig )
        )
      )(
          sender.ref
        )

      countDown.await( 3.seconds )

      log.info( "----------  CHECKING SNAPSHOT 1..." )
      val ss1 = Await.result( ( aggregate ? AP.GetTopicShapeSnapshot( id, scope.topic ) ).mapTo[AP.TopicShapeSnapshot], 5.seconds )
      ss1.snapshot.value.asInstanceOf[SimpleMovingAverageShape].N mustBe size

      log.info( "----------  BUILDING REQUEST 2..." )
      val dp2 = makeDataPoints(
        values = Seq.fill( size ) { 1.0 },
        start = dp1.last.timestamp.plusSeconds( 10 ),
        timeWiggle = ( 0.97, 1.03 )
      )
      val s2 = spike( scope.topic, dp2 )( 0 )
      val h2 = historyWith( Option( h1.recordLastPoints( s1.points ) ), s2 )

      log.info( "---------  SENDING REQUEST 2..." )
      aggregate.sendEnvelope(
        DetectUsing(
          targetId = id,
          algorithm = defaultAlgorithm.label,
          payload = DetectOutliersInSeries( s2, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
          history = h2,
          properties = plan.algorithms.getOrElse( defaultAlgorithm.label, emptyConfig )
        )
      )(
          sender.ref
        )

      countDown.await( 3.seconds )

      log.info( "---------  CHECKING SNASHOT 2..." )
      val ss2 = Await.result( ( aggregate ? AP.GetTopicShapeSnapshot( id, scope.topic ) ).mapTo[AP.TopicShapeSnapshot], 5.seconds )
      ss2.snapshot.value.asInstanceOf[SimpleMovingAverageShape].N mustBe ( 2 * size )

      log.info( "---------- TEST DONE -----------" )
    }
  }
}
