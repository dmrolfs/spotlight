package spotlight.analysis.outlier.algorithm.skyline

import scala.annotation.tailrec
import scala.concurrent.duration._
import akka.testkit._
import org.mockito.Mockito._
import org.joda.{time => joda}
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.akka.envelope._
import spotlight.model.outlier.{NoOutliers, OutlierPlan, SeriesOutliers}
import spotlight.model.timeseries.{DataPoint, ThresholdBoundary, TimeSeries}
import spotlight.analysis.outlier.{DetectOutliersInSeries, DetectUsing, HistoricalStatistics}
import spotlight.analysis.outlier.algorithm.{ AlgorithmProtocol => P}
import spotlight.analysis.outlier.algorithm.AlgorithmModuleSpec


/**
  * Created by rolfsd on 6/9/16.
  */
class SimpleMovingAverageModuleSpec extends AlgorithmModuleSpec[SimpleMovingAverageModuleSpec] {

  override type Module = SimpleMovingAverageModule.type
  override val defaultModule: Module = SimpleMovingAverageModule

  class Fixture extends AlgorithmFixture {
    val testScope: module.TID = identifying tag OutlierPlan.Scope(plan = "TestPlan", topic = "TestTopic")
    override def nextId(): module.TID = testScope

    override def expectedUpdatedState( state: module.State, event: P.Advanced ): module.State = {
      val historicalStatsLens = module.State.History.statsLens compose module.State.historyLens
      val s = super.expectedUpdatedState( state, event ).asInstanceOf[SimpleMovingAverageModule.State]
      val h = s.history
      val stats = h.movingStatistics.copy
      stats addValue event.point.value
      historicalStatsLens.set( s )( stats )
    }
  }

  override def createAkkaFixture( test: OneArgTest ): Fixture = new Fixture


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

  case class Expected( isOutlier: Boolean, floor: Option[Double], expected: Option[Double], ceiling: Option[Double] ) {
    def stepResult( i: Int, intervalSeconds: Int = 10 )( implicit start: joda.DateTime ): (Boolean, ThresholdBoundary) = {
      (
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

  s"${defaultModule.algorithm.label.name} algorithm" should {
    "step to find anomalies from flat signal" in { f: Fixture =>
      import f._

      logger.info( "************** TEST NOW ************" )
      val algorithm = module.algorithm
      implicit val testContext = mock[module.Context]
      val testHistory: module.State.History = module.State.History.empty

      implicit val testState = mock[module.State]
      when( testState.history ).thenReturn( testHistory )

      def advanceWith( v: Double ): Unit = testHistory.movingStatistics addValue v

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
        testHistory.movingStatistics.getN mustBe i
        val ts = nowTimestamp.plusSeconds( 10 * i )
        algorithm.step( ts.getMillis.toDouble, value ) mustBe expected.stepResult( i )
        advanceWith( value )
      }
    }

    "happy path process two batches" in { f: Fixture =>
      import f._

      val destination = TestProbe()
      val plan = mock[OutlierPlan]
      when( plan.name ).thenReturn( f.testScope.id.plan )
      when( plan.appliesTo ).thenReturn( appliesToAll )
      when( plan.algorithms ).thenReturn( Set(module.algorithm.label) )


      logger.info( "****************** TEST NOW ****************" )
      def evaluateMessage(
        series: TimeSeries,
        history: HistoricalStatistics,
        expectedCalculations: Seq[Expected],
        hint: String
      ): Unit = {
        aggregate.sendEnvelope(
          DetectUsing(
            algorithm = module.algorithm.label,
            payload = DetectOutliersInSeries( series, plan, subscriber.ref, Set() ),
            history = history
          )
        )(
          destination.ref
        )

        val expectedAnomalies = expectedCalculations.exists{ e => e.isOutlier }

        destination.expectMsgPF( 500.millis.dilated, hint ) {
          case m @ Envelope( SeriesOutliers( a, s, p, o, t ), _ ) if expectedAnomalies => {
            a mustBe Set( module.algorithm.label )
            s mustBe series
            o.size mustBe 1
            o mustBe Seq( series.points.last )

            t( module.algorithm.label ).zip( expectedCalculations ).zipWithIndex foreach { case ( ((actual, expected), i) ) =>
              (i, actual.floor) mustBe(i, expected.floor)
              (i, actual.expected) mustBe(i, expected.expected)
              (i, actual.ceiling) mustBe(i, expected.ceiling)
            }
          }

          case m @ Envelope( NoOutliers( a, s, p, t ), _ ) if !expectedAnomalies => {
            a mustBe Set( module.algorithm.label )
            s mustBe series

            t( module.algorithm.label ).zip( expectedCalculations ).zipWithIndex foreach { case ( ((actual, expected), i) ) =>
              logger.debug( "evaluating expectation: {}", i.toString )
              actual.floor.isDefined mustBe expected.floor.isDefined
              for {
                af <- actual.floor
                ef <- expected.floor
              } { af mustBe ef +- 0.000001 }

              actual.expected.isDefined mustBe expected.expected.isDefined
              for {
                ae <- actual.expected
                ee <- expected.expected
              } { ae mustBe ee +- 0.000001 }

              actual.ceiling.isDefined mustBe expected.ceiling.isDefined
              for {
                ac <- actual.ceiling
                ec <- expected.ceiling
              } { ac mustBe ec +- 0.000001 }
            }
          }
        }

        import akka.pattern.ask

        val actual = ( aggregate ? P.GetStateSnapshot( id ) ).mapTo[P.StateSnapshot]
        whenReady( actual ) { a =>
          val as = a.snapshot
          logger.info( "{}: ACTUAL = [{}]", hint, as )
          logger.info( "{}: EXPECTED = [{}]", hint, expectedCalculations.mkString(",\n") )
          a.snapshot mustBe defined
          as mustBe defined
          as.value mustBe an [module.State]
          val sas = as.value.asInstanceOf[module.State]
          sas.id.id mustBe id.id
          sas.algorithm.name mustBe module.algorithm.label.name
          sas.thresholds.size mustBe ( history.N )
          logger.info( "{}: history size = {}", hint, sas.history.movingStatistics.getN.toString )
          sas.history.movingStatistics.getN mustBe ( history.N )
          sas.history.movingStatistics.getMean mustBe history.mean(1)

          sas.thresholds
          .drop( sas.thresholds.size - expectedCalculations.size )
          .zip( expectedCalculations )
          .zipWithIndex
          .foreach { case ( ((actual, expected), i) ) =>
            logger.info( "{}: evaluating expectation: {}", hint, i.toString )
            actual.floor.isDefined mustBe expected.floor.isDefined
            for {
              af <- actual.floor
              ef <- expected.floor
            } { af mustBe ef +- 0.000001 }

            actual.expected.isDefined mustBe expected.expected.isDefined
            for {
              ae <- actual.expected
              ee <- expected.expected
            } { ae mustBe ee +- 0.000001 }

            actual.ceiling.isDefined mustBe expected.ceiling.isDefined
            for {
              ac <- actual.ceiling
              ec <- expected.ceiling
            } { ac mustBe ec +- 0.000001 }
          }
        }
      }


      val flatline1 = makeDataPoints( values = Seq.fill( 5 ){ 1.0 }, timeWiggle = (0.97, 1.03) )
      val series1 = spike( f.testScope.id.topic, flatline1, 1000 )()
      val history1 = historyWith( None, series1 )
      val expectedCalculations1 = Seq(
        Expected( false, None, None, None ),
        Expected( false, Some( 1.0 ), Some( 1.0 ), Some( 1.0 ) ),
        Expected( false, Some( 1.0 ), Some( 1.0 ), Some( 1.0 ) ),
        Expected( false, Some( 1.0 ), Some( 1.0 ), Some( 1.0 ) ),
        Expected( true, Some( 1.0 ), Some( 1.0 ), Some( 1.0 ) )
      )
      evaluateMessage( series1, history1, expectedCalculations1, "first-detect" )

      val flatline2 = makeDataPoints(
        values = Seq.fill( 5 ){ 1.0 },
        start = flatline1.last.timestamp.plusSeconds( 10 ),
        timeWiggle = (0.97, 1.03)
      )
      val series2 = spike( f.testScope.id.topic, flatline2, 1000 )( 0 )
      val history2 = historyWith( Option(history1.recordLastPoints(series1.points)), series2 )
      logger.error( "HISTORY2 = [{}]", history2)
      logger.error( "HISTORY2-LAST PTS = [{}]", history2.lastPoints.mkString(",") )
      val expectedCalculations2 = Seq(
        Expected( false, Some(-1139.499146), Some(200.8), Some(1541.099146) ),
        Expected( false, Some(-1213.644145), Some(334), Some(1881.644145) ),
        Expected( false, Some(-1175.957688), Some(286.4285714), Some(1748.814831) ),
        Expected( false, Some(-1136.59142), Some(250.75), Some(1638.09142) ),
        Expected( false, Some(-1098.55278), Some(223), Some(1544.55278) )
      )
      evaluateMessage( series2, history2, expectedCalculations2, "second-detect" )
    }
  }
}
