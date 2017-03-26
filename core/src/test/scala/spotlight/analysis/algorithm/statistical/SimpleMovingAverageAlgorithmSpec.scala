package spotlight.analysis.algorithm.statistical

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.annotation.tailrec
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit._
import org.joda.{ time ⇒ joda }
import com.typesafe.config.{ Config, ConfigFactory }
import com.persist.logging._
import omnibus.akka.envelope._
import org.apache.commons.math3.stat.descriptive.{ DescriptiveStatistics, StatisticalSummary, SummaryStatistics }
import org.mockito.Mockito._
import org.scalatest.Assertion
import spotlight.analysis.{ DetectOutliersInSeries, DetectUsing }
import spotlight.analysis.algorithm.{ Advancing, AlgorithmSpec, AlgorithmProtocol ⇒ AP }
import spotlight.model.outlier.SeriesOutliers
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
        algorithm.score( ( ts.getMillis.toDouble, value ), testShape ) mustBe expected.stepResult( i )
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

      //      val slidingMagent = new CommonCalculationMagnet {
      //        override def apply( points: Seq[DataPoint] ): Result = {
      //          Result(
      //            underlying = points.foldLeft( MovingStatistics( 3 ) ) { ( s, p ) ⇒ s :+ p.value },
      //            timestamp = points.last.timestamp,
      //            tolerance = 3.0
      //          )
      //        }
      //      }

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
      val ( e, r ) = makeExpected( CalculationMagnet.sliding( 3 ) )(
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

      val dp1 = makeDataPoints( values = Seq.fill( 5 ) { 1.0 }, valueWiggle = ( 0.9999, 1.0001 ), timeWiggle = ( 0.97, 1.03 ) )
      val s1 = spike( scope.topic, dp1 )()
      val h1 = historyWith( None, s1 )
      val ( e1, r1 ) = makeExpected( NoHint )(
        points = s1.points,
        outliers = Seq( false, false, false, false, true ),
        minimumPopulation = 2
      )

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

    "handle example data" in { f: Fixture ⇒
      import f._

      val ppConfig = ConfigFactory.parseString(
        s"""
         |sliding-window: 30
         |tail-average: 3
         |tolerance: 2
         |minimum-population: 2
        """.stripMargin
      )

      when( plan.algorithms ) thenReturn { Map( defaultAlgorithm.label → ppConfig ) }

      val series = SimpleMovingAverageAlgorithmSpec.testSeries

      aggregate.sendEnvelope(
        DetectUsing(
          targetId = id,
          algorithm = defaultAlgorithm.label,
          payload = DetectOutliersInSeries( series, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
          history = historyWith( None, series ),
          properties = plan.algorithms.getOrElse( defaultAlgorithm.label, emptyConfig )
        )
      )(
          sender.ref
        )

      //      countDown await 10.seconds
      val anomalyPositions = Set( 92, 93, 94, 95, 96, 97, 98, 99, 100 )
      val expectedAnomolies = series.points.zipWithIndex.collect { case ( dp, i ) if anomalyPositions.contains( i ) ⇒ dp }

      sender.expectMsgPF( 5000.milliseconds.dilated, "result" ) {
        case m @ Envelope( SeriesOutliers( a, ts, p, o, tb ), _ ) ⇒ {
          log.info(
            Map(
              "@msg" → "evaluate series anomaly results",
              "algorithms" → a.toString,
              "source" → ts.toString,
              "plan" → p.toString,
              "outliers" → o.toString,
              "threshold-boundaries" → tb.toString
            )
          )

          a mustBe Set( defaultAlgorithm.label )
          ts mustBe series
          o.size mustBe 9
          o mustBe expectedAnomolies
        }
      }
    }
  }
}

object SimpleMovingAverageAlgorithmSpec {
  val testSeries = TimeSeries(
    topic = "sales-prediction-foo_bar+314159zz",
    points = Seq(
      DataPoint( timestamp = new joda.DateTime( 1470009600000L ), value = 60.21464729374097 ),
      DataPoint( timestamp = new joda.DateTime( 1470096000000L ), value = 125.30900325958481 ),
      DataPoint( timestamp = new joda.DateTime( 1470182400000L ), value = 114.68839507923245 ),
      DataPoint( timestamp = new joda.DateTime( 1470268800000L ), value = 107.04391508872341 ),
      DataPoint( timestamp = new joda.DateTime( 1470355200000L ), value = 101.88890908508881 ),
      DataPoint( timestamp = new joda.DateTime( 1470441600000L ), value = 109.19887681582179 ),
      DataPoint( timestamp = new joda.DateTime( 1470528000000L ), value = 100.0569068251547 ),
      DataPoint( timestamp = new joda.DateTime( 1470614400000L ), value = 103.65244716267357 ),
      DataPoint( timestamp = new joda.DateTime( 1470700800000L ), value = 101.27757540053496 ),
      DataPoint( timestamp = new joda.DateTime( 1470787200000L ), value = 96.70745615910846 ),
      DataPoint( timestamp = new joda.DateTime( 1470873600000L ), value = 96.38490772915844 ),
      DataPoint( timestamp = new joda.DateTime( 1470960000000L ), value = 84.71900163706843 ),
      DataPoint( timestamp = new joda.DateTime( 1471046400000L ), value = 87.59069715149465 ),
      DataPoint( timestamp = new joda.DateTime( 1471132800000L ), value = 85.84703176164993 ),
      DataPoint( timestamp = new joda.DateTime( 1471219200000L ), value = 85.9028544815674 ),
      DataPoint( timestamp = new joda.DateTime( 1471305600000L ), value = 88.32320449756935 ),
      DataPoint( timestamp = new joda.DateTime( 1471392000000L ), value = 79.38790653840138 ),
      DataPoint( timestamp = new joda.DateTime( 1471478400000L ), value = 79.4642098636066 ),
      DataPoint( timestamp = new joda.DateTime( 1471564800000L ), value = 74.47582354974078 ),
      DataPoint( timestamp = new joda.DateTime( 1471651200000L ), value = 74.93300680412653 ),
      DataPoint( timestamp = new joda.DateTime( 1471737600000L ), value = 71.88515099210917 ),
      DataPoint( timestamp = new joda.DateTime( 1471824000000L ), value = 74.58426252915928 ),
      DataPoint( timestamp = new joda.DateTime( 1471910400000L ), value = 70.15769835241446 ),
      DataPoint( timestamp = new joda.DateTime( 1471996800000L ), value = 65.22929694449218 ),
      DataPoint( timestamp = new joda.DateTime( 1472083200000L ), value = 62.810594090715604 ),
      DataPoint( timestamp = new joda.DateTime( 1472169600000L ), value = 68.68836194600689 ),
      DataPoint( timestamp = new joda.DateTime( 1472256000000L ), value = 69.51184153171673 ),
      DataPoint( timestamp = new joda.DateTime( 1472342400000L ), value = 65.43925997873313 ),
      DataPoint( timestamp = new joda.DateTime( 1472428800000L ), value = 63.471353373683705 ),
      DataPoint( timestamp = new joda.DateTime( 1472515200000L ), value = 65.37633751909686 ), // 2016-08-30
      DataPoint( timestamp = new joda.DateTime( 1472601600000L ), value = 63.23891091237377 ),
      DataPoint( timestamp = new joda.DateTime( 1472688000000L ), value = 120.08744537193263 ),
      DataPoint( timestamp = new joda.DateTime( 1472774400000L ), value = 95.97125374052719 ),
      DataPoint( timestamp = new joda.DateTime( 1472860800000L ), value = 116.6084189318276 ),
      DataPoint( timestamp = new joda.DateTime( 1472947200000L ), value = 115.86148720652726 ),
      DataPoint( timestamp = new joda.DateTime( 1473033600000L ), value = 113.42499271626842 ),
      DataPoint( timestamp = new joda.DateTime( 1473120000000L ), value = 112.29611967238426 ),
      DataPoint( timestamp = new joda.DateTime( 1473206400000L ), value = 107.22171528854012 ),
      DataPoint( timestamp = new joda.DateTime( 1473292800000L ), value = 105.62171009424415 ),
      DataPoint( timestamp = new joda.DateTime( 1473379200000L ), value = 96.26613351673689 ),
      DataPoint( timestamp = new joda.DateTime( 1473465600000L ), value = 101.74489858059643 ),
      DataPoint( timestamp = new joda.DateTime( 1473552000000L ), value = 97.99980412054467 ),
      DataPoint( timestamp = new joda.DateTime( 1473638400000L ), value = 90.67768152803791 ),
      DataPoint( timestamp = new joda.DateTime( 1473724800000L ), value = 92.63959986013629 ),
      DataPoint( timestamp = new joda.DateTime( 1473811200000L ), value = 85.38087692125197 ),
      DataPoint( timestamp = new joda.DateTime( 1473897600000L ), value = 86.87011762004093 ),
      DataPoint( timestamp = new joda.DateTime( 1473984000000L ), value = 82.82790955762104 ),
      DataPoint( timestamp = new joda.DateTime( 1474070400000L ), value = 83.80304766130362 ),
      DataPoint( timestamp = new joda.DateTime( 1474156800000L ), value = 76.56840300688609 ),
      DataPoint( timestamp = new joda.DateTime( 1474243200000L ), value = 70.38980526515977 ),
      DataPoint( timestamp = new joda.DateTime( 1474416000000L ), value = 66.89845934278745 ),
      DataPoint( timestamp = new joda.DateTime( 1474502400000L ), value = 68.9576094466755 ),
      DataPoint( timestamp = new joda.DateTime( 1474588800000L ), value = 65.19524190091019 ),
      DataPoint( timestamp = new joda.DateTime( 1474675200000L ), value = 62.7648750077026 ),
      DataPoint( timestamp = new joda.DateTime( 1474761600000L ), value = 68.9847141924992 ),
      DataPoint( timestamp = new joda.DateTime( 1474848000000L ), value = 70.53635412794458 ),
      DataPoint( timestamp = new joda.DateTime( 1474934400000L ), value = 66.07122431555239 ),
      DataPoint( timestamp = new joda.DateTime( 1475020800000L ), value = 65.17733635092878 ),
      DataPoint( timestamp = new joda.DateTime( 1475107200000L ), value = 64.22551601029748 ),
      DataPoint( timestamp = new joda.DateTime( 1475193600000L ), value = 60.24695085134628 ),
      DataPoint( timestamp = new joda.DateTime( 1475280000000L ), value = 104.30783640394733 ),
      DataPoint( timestamp = new joda.DateTime( 1475366400000L ), value = 106.57935303914964 ),
      DataPoint( timestamp = new joda.DateTime( 1475452800000L ), value = 101.56532377335309 ),
      DataPoint( timestamp = new joda.DateTime( 1475539200000L ), value = 101.00936675867439 ),
      DataPoint( timestamp = new joda.DateTime( 1475625600000L ), value = 92.92009787636293 ),
      DataPoint( timestamp = new joda.DateTime( 1475712000000L ), value = 93.88093887752612 ),
      DataPoint( timestamp = new joda.DateTime( 1475798400000L ), value = 76.97250591953596 ),
      DataPoint( timestamp = new joda.DateTime( 1475884800000L ), value = 85.14030078422678 ),
      DataPoint( timestamp = new joda.DateTime( 1475971200000L ), value = 97.16621240114544 ),
      DataPoint( timestamp = new joda.DateTime( 1476057600000L ), value = 95.36620091711363 ),
      DataPoint( timestamp = new joda.DateTime( 1476144000000L ), value = 91.8966529356332 ),
      DataPoint( timestamp = new joda.DateTime( 1476230400000L ), value = 87.00482059208096 ),
      DataPoint( timestamp = new joda.DateTime( 1476316800000L ), value = 90.58842807542025 ),
      DataPoint( timestamp = new joda.DateTime( 1476403200000L ), value = 81.3822738363942 ),
      DataPoint( timestamp = new joda.DateTime( 1476489600000L ), value = 84.22331733793436 ),
      DataPoint( timestamp = new joda.DateTime( 1476576000000L ), value = 91.51344679603936 ),
      DataPoint( timestamp = new joda.DateTime( 1476662400000L ), value = 87.15958574768992 ),
      DataPoint( timestamp = new joda.DateTime( 1476748800000L ), value = 90.6386991487052 ),
      DataPoint( timestamp = new joda.DateTime( 1476835200000L ), value = 83.5767488137414 ),
      DataPoint( timestamp = new joda.DateTime( 1476921600000L ), value = 96.03562068150192 ),
      DataPoint( timestamp = new joda.DateTime( 1477008000000L ), value = 90.72733070077308 ),
      DataPoint( timestamp = new joda.DateTime( 1477094400000L ), value = 87.29624967704171 ),
      DataPoint( timestamp = new joda.DateTime( 1477180800000L ), value = 86.5997168819918 ),
      DataPoint( timestamp = new joda.DateTime( 1477267200000L ), value = 82.509383675235 ),
      DataPoint( timestamp = new joda.DateTime( 1477353600000L ), value = 90.56093932701981 ),
      DataPoint( timestamp = new joda.DateTime( 1477440000000L ), value = 88.13004748936811 ),
      DataPoint( timestamp = new joda.DateTime( 1477526400000L ), value = 85.28517349847661 ),
      DataPoint( timestamp = new joda.DateTime( 1477612800000L ), value = 82.49488120758735 ),
      DataPoint( timestamp = new joda.DateTime( 1477699200000L ), value = 78.9371848838786 ),
      DataPoint( timestamp = new joda.DateTime( 1477785600000L ), value = 80.36835982546903 ),
      DataPoint( timestamp = new joda.DateTime( 1477872000000L ), value = 78.22561099300688 ),
      DataPoint( timestamp = new joda.DateTime( 1477958400000L ), value = 123.88920765790856 ),
      DataPoint( timestamp = new joda.DateTime( 1478044800000L ), value = 121.92237069687002 ),
      DataPoint( timestamp = new joda.DateTime( 1478131200000L ), value = 118.36869307522501 ),
      DataPoint( timestamp = new joda.DateTime( 1478217600000L ), value = 135.83246313367343 ),
      DataPoint( timestamp = new joda.DateTime( 1478304000000L ), value = 135.2110972209682 ),
      DataPoint( timestamp = new joda.DateTime( 1478390400000L ), value = 122.17132089028296 ),
      DataPoint( timestamp = new joda.DateTime( 1478476800000L ), value = 134.3594107812906 ),
      DataPoint( timestamp = new joda.DateTime( 1478563200000L ), value = 173.4505596587952 ),
      DataPoint( timestamp = new joda.DateTime( 1478649600000L ), value = 155.04196784763982 ),
      DataPoint( timestamp = new joda.DateTime( 1478736000000L ), value = 156.0078756948538 ),
      DataPoint( timestamp = new joda.DateTime( 1478822400000L ), value = 98.42232905637454 ),
      DataPoint( timestamp = new joda.DateTime( 1478908800000L ), value = 96.6066150715672 ),
      DataPoint( timestamp = new joda.DateTime( 1478995200000L ), value = 104.2334120354118 ),
      DataPoint( timestamp = new joda.DateTime( 1479081600000L ), value = 121.0851244410609 ),
      DataPoint( timestamp = new joda.DateTime( 1479168000000L ), value = 112.82810640258232 ),
      DataPoint( timestamp = new joda.DateTime( 1479254400000L ), value = 112.0457256058918 ),
      DataPoint( timestamp = new joda.DateTime( 1479340800000L ), value = 121.6701522768784 ),
      DataPoint( timestamp = new joda.DateTime( 1479427200000L ), value = 103.38889958510629 ),
      DataPoint( timestamp = new joda.DateTime( 1479513600000L ), value = 106.90518326893286 ),
      DataPoint( timestamp = new joda.DateTime( 1479600000000L ), value = 103.57829839295385 ),
      DataPoint( timestamp = new joda.DateTime( 1479686400000L ), value = 101.1215852508392 ),
      DataPoint( timestamp = new joda.DateTime( 1479772800000L ), value = 101.10334284169389 ),
      DataPoint( timestamp = new joda.DateTime( 1479859200000L ), value = 97.84429435219198 ),
      DataPoint( timestamp = new joda.DateTime( 1479945600000L ), value = 104.2036520752346 ),
      DataPoint( timestamp = new joda.DateTime( 1480032000000L ), value = 82.86846971150186 ),
      DataPoint( timestamp = new joda.DateTime( 1480118400000L ), value = 83.82716091606883 ),
      DataPoint( timestamp = new joda.DateTime( 1480204800000L ), value = 83.68307694008816 ),
      DataPoint( timestamp = new joda.DateTime( 1480291200000L ), value = 81.29604530832249 ),
      DataPoint( timestamp = new joda.DateTime( 1480377600000L ), value = 79.32293659830289 ),
      DataPoint( timestamp = new joda.DateTime( 1480464000000L ), value = 81.44617128370734 ),
      DataPoint( timestamp = new joda.DateTime( 1480636800000L ), value = 130.13521396974147 ),
      DataPoint( timestamp = new joda.DateTime( 1480723200000L ), value = 129.32305166766292 ),
      DataPoint( timestamp = new joda.DateTime( 1480809600000L ), value = 129.2680296634418 ),
      DataPoint( timestamp = new joda.DateTime( 1480896000000L ), value = 118.30078526544068 ),
      DataPoint( timestamp = new joda.DateTime( 1480982400000L ), value = 127.77871957843719 ),
      DataPoint( timestamp = new joda.DateTime( 1481068800000L ), value = 107.2882798475932 ),
      DataPoint( timestamp = new joda.DateTime( 1481155200000L ), value = 117.93835179315626 ),
      DataPoint( timestamp = new joda.DateTime( 1481241600000L ), value = 115.86356481831587 ),
      DataPoint( timestamp = new joda.DateTime( 1481328000000L ), value = 118.19873188171144 ),
      DataPoint( timestamp = new joda.DateTime( 1481414400000L ), value = 115.67401065515796 ),
      DataPoint( timestamp = new joda.DateTime( 1481500800000L ), value = 103.82913513199632 ),
      DataPoint( timestamp = new joda.DateTime( 1481587200000L ), value = 102.85794920515418 ),
      DataPoint( timestamp = new joda.DateTime( 1481673600000L ), value = 93.37866918879513 ),
      DataPoint( timestamp = new joda.DateTime( 1481760000000L ), value = 112.06555857111931 ),
      DataPoint( timestamp = new joda.DateTime( 1481846400000L ), value = 108.14217005921255 ),
      DataPoint( timestamp = new joda.DateTime( 1481932800000L ), value = 112.10120346176858 ),
      DataPoint( timestamp = new joda.DateTime( 1482105600000L ), value = 101.84621703120743 ),
      DataPoint( timestamp = new joda.DateTime( 1482192000000L ), value = 109.26824649903388 ),
      DataPoint( timestamp = new joda.DateTime( 1482278400000L ), value = 95.52173040516556 ),
      DataPoint( timestamp = new joda.DateTime( 1482364800000L ), value = 112.86894077618538 ),
      DataPoint( timestamp = new joda.DateTime( 1482451200000L ), value = 103.77595644684607 ),
      DataPoint( timestamp = new joda.DateTime( 1482537600000L ), value = 105.4030545976555 ),
      DataPoint( timestamp = new joda.DateTime( 1482624000000L ), value = 94.1315031867468 ),
      DataPoint( timestamp = new joda.DateTime( 1482710400000L ), value = 92.9512191646509 ),
      DataPoint( timestamp = new joda.DateTime( 1482796800000L ), value = 102.32854478473683 ),
      DataPoint( timestamp = new joda.DateTime( 1482883200000L ), value = 98.58949140727177 ),
      DataPoint( timestamp = new joda.DateTime( 1482969600000L ), value = 105.13084230784997 ),
      DataPoint( timestamp = new joda.DateTime( 1483056000000L ), value = 92.294651173702 ),
      DataPoint( timestamp = new joda.DateTime( 1483142400000L ), value = 87.96574467760651 )
    )
  )
}