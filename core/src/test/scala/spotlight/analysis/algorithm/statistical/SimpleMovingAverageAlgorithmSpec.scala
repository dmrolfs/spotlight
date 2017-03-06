package spotlight.analysis.algorithm.statistical

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.annotation.tailrec
import akka.actor.ActorSystem
import akka.pattern.ask
import com.typesafe.config.{ Config, ConfigFactory }
import com.persist.logging._
import omnibus.akka.envelope._
import org.apache.commons.math3.stat.descriptive.{ DescriptiveStatistics, StatisticalSummary, SummaryStatistics }
import org.mockito.Mockito._
import org.scalatest.Assertion
import spotlight.analysis.{ DetectOutliersInSeries, DetectUsing }
import spotlight.analysis.algorithm.{ Advancing, AlgorithmSpec, AlgorithmProtocol ⇒ AP }
import spotlight.model.statistics.MovingStatistics
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
      shape :+ event.point.value
    }

    def assertShape( result: Option[CalculationMagnetResult], topic: Topic )( s: TestShape ): Assertion = {
      val expectedStats = for { r ← result; s ← r.statistics } yield ( s.getN, s.getMean, s.getStandardDeviation )

      log.info(
        Map(
          "@msg" → "assertShape",
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
      var testShape: Shape = shapeless.the[Advancing[Shape]].zero( None )

      implicit val testState = mock[State]
      when( testState.shapes ).thenReturn( Map( scope.topic → testShape ) )

      def advanceWith( v: Double ): Unit = {
        testShape = testShape :+ v
      }

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

    "3-window shape should slide stats" in { f: Fixture ⇒
      import f._

      val points: Seq[Double] = Seq(
        9.46, 9.9, 11.6, 14.5, 17.3, 19.2, 18.4, 14.5, 12.2, 10.8, 8.58,
        8.36, 8.58, 7.5, 7.1, 7.3, 7.71, 8.14, 8.14, 7.1, 7.5,
        7.1, 7.1, 7.3, 7.71, 8.8, 9.9, 14.2, 18.8, 25.2, 31.5,
        22, 24.1, 39.2
      )

      val expectedMeans = Seq(
        9.46, 9.68,
        10.32, 12.0, 14.46666666666670, 17.0, 18.3, 17.36666666666670, 15.03333333333330, 12.5,
        10.52666666666670, 9.24666666666667, 8.50666666666667, 8.14666666666667, 7.72666666666667, 7.3,
        7.37, 7.71666666666667, 7.99666666666667, 7.79333333333333, 7.58, 7.23333333333333, 7.23333333333333, 7.16666666666667,
        7.37, 7.93666666666667, 8.80333333333333, 10.96666666666670, 14.3, 19.4, 25.16666666666670, 26.23333333333330,
        25.86666666666670
      )

      val expectedStdevs = Seq(
        0.0, 0.311126984, 1.130132736, 2.32594067, 2.850146195, 2.364318084, 0.953939201, 2.514623895, 3.134219733,
        1.868154169, 1.825413195, 1.34971602, 0.127017059, 0.570730526, 0.765593452, 0.2, 0.310966236, 0.420039681, 0.248260616,
        0.60044428, 0.524595082, 0.230940108, 0.230940108, 0.115470054, 0.310966236, 0.775263396,
        1.095003805, 2.85365263, 4.450842617, 5.524490927, 6.350065616, 4.833563213, 4.990323971
      )

      val expectedStats = expectedMeans zip expectedStdevs
      val pe = points zip expectedStats
      pe.foldLeft( SimpleMovingAverageShape( 3 ) ) {
        case ( shape, ( v, ( expectedMean, expectedStdev ) ) ) ⇒
          val newShape = shape :+ v
          log.debug(
            Map(
              "@msg" → "checking sliding stat",
              "shape" → newShape.toString,
              "value" → f"${v}%2.5f",
              "mean" → Map( "actual" → f"${newShape.mean}%2.5f", "expected" → f"${expectedMean}%2.5f" ),
              "stdev" → Map( "actual" → f"${newShape.standardDeviation}%2.5f", "expected" → f"${expectedStdev}%2.5f" )
            )
          )

          if ( expectedMean.isNaN ) {
            newShape.mean.isNaN mustBe true
          } else {
            newShape.mean mustBe expectedMean +- 0.000001
          }

          if ( expectedStdev.isNaN ) {
            newShape.standardDeviation.isNaN mustBe true
          } else {
            newShape.standardDeviation mustBe expectedStdev +- 0.000001
          }

          newShape
      }
    }

    "verify points case from OutlierScoringModel spec and small window" in { f: Fixture ⇒
      import f._

      val minPopulation = 2
      val smaConfig = ConfigFactory.parseString(
        s"""
        |tail-average: 3
        |tolerance: 3
        |minimum-population: ${minPopulation}
        |sliding-window: 3
        """.stripMargin
      )
      when( plan.algorithms ) thenReturn { Map( defaultAlgorithm.label → smaConfig ) }

      val slidingMagent = new CommonCalculationMagnet {
        override def apply( points: Seq[DataPoint] ): Result = {
          val r = Result(
            underlying = points.foldLeft( MovingStatistics( 3 ) ) { ( s, p ) ⇒ s :+ p.value },
            timestamp = points.last.timestamp,
            tolerance = 3.0
          )

          //          log.debug(
          //            Map(
          //              "@msg" → "SLIDING MAGNET",
          //              "points" → points.map( p ⇒ f"${p.value}%2.5f" ),
          //              "result" → Map(
          //                "N" → r.statistics.map( _.getN ),
          //                "mean" → r.statistics.map( s ⇒ f"${s.getMean}%2.5f" ),
          //                "stdev" → r.statistics.map( s ⇒ f"${s.getStandardDeviation}%2.5f" )
          //              )
          //            )
          //          )

          r
        }
      }

      val points: Seq[Double] = Seq(
        9.46, 9.9, 11.6, 14.5, 17.3, 19.2, 18.4, 14.5, 12.2, 10.8, 8.58,
        8.36, 8.58, 7.5, 7.1, 7.3, 7.71, 8.14, 8.14, 7.1, 7.5,
        7.1, 7.1, 7.3, 7.71, 8.8, 9.9, 14.2, 18.8, 25.2, 31.5,
        22, 24.1, 39.2
      )

      val expected: Seq[Expected] = Seq(
        Expected( false, None, None, None ),
        Expected( false, None, None, None ),
        Expected( false, Some( 8.746619 ), Some( 9.6800 ), Some( 10.613381 ) ),
        Expected( false, Some( 6.929602 ), Some( 10.3200 ), Some( 13.710398 ) ),
        Expected( false, Some( 5.022178 ), Some( 12.0000 ), Some( 18.977822 ) ),
        Expected( false, Some( 5.916228 ), Some( 14.4667 ), Some( 23.017105 ) ),
        Expected( false, Some( 9.907046 ), Some( 17.0000 ), Some( 24.092954 ) ),
        Expected( false, Some( 15.438182 ), Some( 18.3000 ), Some( 21.161818 ) ),
        Expected( false, Some( 9.822795 ), Some( 17.3667 ), Some( 24.910538 ) ),
        Expected( false, Some( 5.630674 ), Some( 15.0333 ), Some( 24.435993 ) ),
        Expected( false, Some( 6.895537 ), Some( 12.5000 ), Some( 18.104463 ) ),
        Expected( false, Some( 5.050427 ), Some( 10.5267 ), Some( 16.002906 ) ),
        Expected( false, Some( 5.197519 ), Some( 9.2467 ), Some( 13.295815 ) ),
        Expected( false, Some( 8.125615 ), Some( 8.5067 ), Some( 8.887718 ) ),
        Expected( false, Some( 6.434475 ), Some( 8.1467 ), Some( 9.858858 ) ),
        Expected( false, Some( 5.429886 ), Some( 7.7267 ), Some( 10.023447 ) ),
        Expected( false, Some( 6.700000 ), Some( 7.3000 ), Some( 7.900000 ) ),
        Expected( false, Some( 6.437101 ), Some( 7.3700 ), Some( 8.302899 ) ),
        Expected( false, Some( 6.456548 ), Some( 7.7167 ), Some( 8.976786 ) ),
        Expected( false, Some( 7.251885 ), Some( 7.9967 ), Some( 8.741449 ) ),
        Expected( false, Some( 5.992000 ), Some( 7.7933 ), Some( 9.594666 ) ),
        Expected( false, Some( 6.006215 ), Some( 7.5800 ), Some( 9.153785 ) ),
        Expected( false, Some( 6.540513 ), Some( 7.2333 ), Some( 7.926154 ) ),
        Expected( false, Some( 6.540513 ), Some( 7.2333 ), Some( 7.926154 ) ),
        Expected( false, Some( 6.820257 ), Some( 7.1667 ), Some( 7.513077 ) ),
        Expected( false, Some( 6.437101 ), Some( 7.3700 ), Some( 8.302899 ) ),
        Expected( false, Some( 5.610876 ), Some( 7.9367 ), Some( 10.262457 ) ),
        Expected( false, Some( 5.518322 ), Some( 8.8033 ), Some( 12.088345 ) ),
        Expected( false, Some( 2.405709 ), Some( 10.9667 ), Some( 19.527625 ) ),
        Expected( false, Some( 0.947472 ), Some( 14.3000 ), Some( 27.652528 ) ),
        Expected( false, Some( 2.826527 ), Some( 19.4000 ), Some( 35.973473 ) ),
        Expected( false, Some( 6.116470 ), Some( 25.1667 ), Some( 44.216864 ) ),
        Expected( false, Some( 11.732644 ), Some( 26.2333 ), Some( 40.734023 ) ),
        Expected( false, Some( 10.895695 ), Some( 25.8667 ), Some( 40.837639 ) )
      )

      val ts = TimeSeries( "foo.bar", makeDataPoints( points ) )
      log.debug( Map( "@msg" → "timeseries", "values" → ts.points.map { _.value }.mkString( "[", ", ", "]" ) ) )
      val h = historyWith( None, ts )
      val ( e, r ) = makeExpected( slidingMagent )(
        points = ts.points,
        outliers = explodeOutlierIdentifiers( points.size, Set() ),
        minimumPopulation = minPopulation
      )

      log.debug( Map( "@msg" → "#TEST PLAN ALGORITHMS", "algos" → plan.algorithms.toString ) )
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

    "validate size after two calls" in { f: Fixture ⇒
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
