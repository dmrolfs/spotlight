package spotlight.analysis.algorithm.business

import akka.actor.ActorSystem
import akka.testkit._
import com.persist.logging._
import com.typesafe.config.{ Config, ConfigFactory }
import omnibus.akka.envelope._
import org.joda.{ time ⇒ joda }
import org.mockito.Mockito._
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced
import spotlight.analysis.algorithm.business.PastPeriod.Period
import spotlight.analysis.algorithm.{ AlgorithmSpec, AlgorithmProtocol ⇒ AP }
import spotlight.analysis.{ DetectOutliersInSeries, DetectUsing }
import spotlight.model.outlier.{ NoOutliers, SeriesOutliers }
import spotlight.model.statistics.MovingStatistics
import spotlight.model.timeseries.{ DataPoint, ThresholdBoundary, TimeSeries }

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration.DurationInt

/** Created by rolfsd on 3/6/17.
  */
class PastPeriodAverageAlgorithmSpec extends AlgorithmSpec[PastPeriod.Shape] {
  override type Algo = PastPeriodAverageAlgorithm.type
  override val defaultAlgorithm: Algo = PastPeriodAverageAlgorithm
  override val memoryPlateauNr: Int = 10

  val periodWindow = 3

  override def calculateControlBoundaries(
    points: Seq[DataPoint],
    tolerance: Double,
    lastPoints: Seq[DataPoint]
  ): Seq[ThresholdBoundary] = {
    @tailrec def loop( pts: List[DataPoint], history: Array[Double], acc: Seq[ThresholdBoundary] ): Seq[ThresholdBoundary] = {
      pts match {
        case Nil ⇒ acc

        case p :: tail ⇒ {
          val stats = MovingStatistics( width = periodWindow, history: _* )
          val control = ThresholdBoundary.fromExpectedAndDistance(
            p.timestamp,
            expected = stats.mean,
            distance = math.abs( tolerance * stats.standardDeviation )
          )
          logger.debug( "EXPECTED for point:[{}] Control [{}] = [{}]", ( p.timestamp.getMillis, p.value ), acc.size.toString, control )
          loop( tail, history :+ p.value, acc :+ control )
        }
      }
    }

    loop( points.toList, lastPoints.map { _.value }.toArray, Seq.empty[ThresholdBoundary] )
  }

  implicit val periodValueOrdering: Ordering[PastPeriod.PeriodValue] = new Ordering[PastPeriod.PeriodValue] {
    override def compare( lhs: ( Period, Double ), rhs: ( Period, Double ) ): Int = {
      import com.github.nscala_time.time.Imports._

      if ( lhs._1.timestamp < rhs._1.timestamp ) -1
      else if ( lhs._1.timestamp > rhs._1.timestamp ) 1
      else if ( lhs._2 < rhs._2 ) -1
      else if ( lhs._2 > rhs._2 ) 1
      else 0
    }
  }

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AlgorithmFixture( _config, _system, _slug ) {
    override def expectedUpdatedShape( shape: TestShape, event: Advanced ): TestShape = {
      val ( p, v ) = PastPeriod.Period fromDataPoint event.point
      shape.addPeriodValue( p, v )
    }

    override implicit val shapeOrdering: Ordering[Shape] = new Ordering[Shape] {
      override def compare( lhs: Shape, rhs: Shape ): Int = {
        if ( lhs == rhs ) 0
        else {
          val l = lhs.asInstanceOf[PastPeriod.Shape]
          val r = rhs.asInstanceOf[PastPeriod.Shape]
          if ( l.periods.size != r.periods.size ) ( l.periods.size - r.periods.size )
          else shapeless.the[Ordering[Option[PastPeriod.PeriodValue]]].compare( l.currentPeriodValue, r.currentPeriodValue )
        }
      }
    }

    val series = PastPeriodAverageAlgorithmSpec.testSeries
  }

  bootstrapSuite()
  analysisStateSuite()

  s"${defaultAlgorithm.label} algorithm" should {
    "assign period" in { f: Fixture ⇒
      import f._
      val points = assemble( series.points, Seq( 0, 1, 2, 31, 32, 60, 61, 91, 92, 120, 121, 149 ) )
      val expected = Seq( 201607, 201608, 201608, 201608, 201609, 201609, 201610, 201610, 201611, 201611, 201612, 201612 )
      points.size mustBe expected.size
      points.map( Period.fromDataPoint ).zip( expected ).foreach { case ( ( p, v ), e ) ⇒ p.qualifier mustBe e }
    }

    "identify more recent" in { f: Fixture ⇒
      import f._

      val incumbents = assemble( series.points, Seq( 0, 1, 1, 2 ) ) map { p ⇒ Period.fromTimestamp( p.timestamp ) }
      val candidates = assemble( series.points, Seq( 1, 1, 2, 1 ) ) map { p ⇒ Period.fromTimestamp( p.timestamp ) }
      val expected = Seq( false, false, true, false )
      incumbents.size mustBe candidates.size
      incumbents.size mustBe expected.size

      incumbents.zip( candidates ).zip( expected ).foreach {
        case ( ( i, c ), e ) ⇒ {
          log.info(
            Map(
              "@msg" → "evaluating most recent",
              "incumbent" → i.toString,
              "candidate" → c.toString,
              "expected" → e,
              "actual" → ( Period.isCandidateMoreRecent( i, c ) )
            )
          )
          Period.isCandidateMoreRecent( i, c ) mustBe e
        }
      }
    }

    "add to shape" in { f: Fixture ⇒
      import f._

      val window = 3
      val pvs = series.points map { Period.fromDataPoint }
      val shape = pvs.foldLeft( PastPeriod.Shape( window = window, history = window * 10 ) ) {
        case ( s, ( p, v ) ) ⇒
          val ns = s.addPeriodValue( p, v )
          log.info(
            Map(
              "@msg" → "adding period value to shape",
              "PeriodValue" → Map( "period" → p.toString, "value" → v ),
              "shape" → Map( "window" → s.window, "current" → s.currentPeriodValue.toString, "periods" → s.periods.toString ),
              "new" → Map( "window" → ns.window, "current" → ns.currentPeriodValue.toString, "periods" → ns.periods.toString )
            )
          )
          ns
      }

      val expectedPriors = assemble( series.points, Seq( 91, 120, 149 ) ).map { Period.fromDataPoint }.to[immutable.Vector]

      shape.window mustBe window
      shape.currentPeriodValue mustBe None
      shape.pastPeriodsFromNow.size mustBe window
      shape.pastPeriodsFromNow mustBe expectedPriors

      val now = ( Period fromTimestamp joda.DateTime.now, 3.1415926535897932384264 )
      val s2 = shape.addPeriodValue( now._1, now._2 )
      s2.currentPeriodValue.value mustBe now
      s2.pastPeriodsFromNow.size mustBe window
      shape.pastPeriodsFromNow mustBe expectedPriors
    }

    "advance shape with example data" in { f: Fixture ⇒
      import f._

      val window = 3
      val tol = 0.0000001
      val adv = PastPeriod.Shape.advancing

      def advance( shape: TestShape, events: Seq[AP.Advanced] ): TestShape = {
        val ns = events.foldLeft( shape ) { ( s, e ) ⇒
          log.info(
            Map(
              "@msg" → "advancing event",
              "event" → Map( "timestamp" → e.point.timestamp, "value" → f"${e.point.value}%2.5f" )
            )
          )
          adv.advance( s, e )
        }
        log.info(
          Map(
            "@msg" → "advanced shape forware",
            "prior" → Map( "N" → adv.N( shape ), "shape" → shape.toString ),
            "advanced" → Map( "N" → adv.N( ns ), "shape" → ns.toString )
          )
        )
        ns
      }

      val now = joda.DateTime.now

      val evts = series.points map { p ⇒ AP.Advanced( id, series.topic, p, false, ThresholdBoundary.empty( p.timestamp ) ) }
      val s0 = adv.zero( None )
      adv.N( s0 ) mustBe 0
      s0.pastPeriodsFromNow.size mustBe 0
      s0.currentPeriodValue mustBe None
      log.debug( "+++++++ advanced to s0 +++++++" )

      val s1 = advance( s0, evts.take( 1 ) )
      log.debug( "+++++++ advanced to s1 - 2016.07.30 +++++++" )
      //      countDown await 1.second
      s1.meanFrom( now ).value mustBe evts.head.point.value
      s1.standardDeviationFrom( now ).value mustBe 0.0
      adv.N( s1 ) mustBe 1
      s1.pastPeriodsFromTimestamp( now ).size mustBe 1
      s1.currentPeriodValue mustBe None

      val s2 = advance( s1, evts.drop( 1 ).take( 31 ) )
      log.debug( "+++++++ advanced to s2 - 2016.08.31 +++++++" )
      //      countDown await 1.second
      s2.meanFrom( now ).value mustBe 90.15104633 +- tol
      s2.standardDeviationFrom( now ).value mustBe 42.33646153 +- tol
      adv.N( s2 ) mustBe 32
      s2.pastPeriodsFromTimestamp( now ).size mustBe 2
      s2.currentPeriodValue mustBe None

      val s3 = advance( s2, evts.drop( 32 ).take( 29 ) ) // missing 9/19/2016
      log.debug( "+++++++ advanced to s3 - 2016.9.30 +++++++" )
      //      countDown await 1.second
      s3.meanFrom( now ).value mustBe 94.86997636 +- tol
      s3.standardDeviationFrom( now ).value mustBe 31.03212673 +- tol
      adv.N( s3 ) mustBe 61
      s3.pastPeriodsFromTimestamp( now ).size mustBe 3
      s3.currentPeriodValue mustBe None

      val s4 = advance( s3, evts.drop( 61 ).take( 31 ) )
      log.debug( "+++++++ advanced to s4 - 2016.10.31 +++++++" )
      //      countDown await 1.second
      s4.meanFrom( now ).value mustBe 116.0948298 +- tol
      s4.standardDeviationFrom( now ).value mustBe 10.38331639 +- tol
      adv.N( s4 ) mustBe 92
      s4.pastPeriodsFromTimestamp( now ).size mustBe 3
      s4.currentPeriodValue mustBe None

      val s5 = advance( s4, evts.drop( 92 ) )
      log.debug( "+++++++ advanced to s5 - 2016.12.31 +++++++" )
      //      countDown await 1.second
      s5.meanFrom( now ).value mustBe 97.76704121 +- tol
      s5.standardDeviationFrom( now ).value mustBe 22.85611285 +- tol
      adv.N( s5 ) mustBe 150
      s5.pastPeriodsFromTimestamp( now ).size mustBe 3
      s5.currentPeriodValue mustBe None

      //      countDown await 1.second
    }

    "handle single point" in { f: Fixture ⇒
      import f._

      val smaConfig = ConfigFactory.parseString(
        s"""
          |tail-average: 3
          |tolerance: 3
          |minimum-population: 2
        """.stripMargin
      )

      when( plan.algorithms ) thenReturn { Map( defaultAlgorithm.label → smaConfig ) }

      val s = TimeSeries( topic = series.topic, points = series.points.take( 1 ) )
      aggregate.sendEnvelope(
        DetectUsing(
          targetId = id,
          algorithm = defaultAlgorithm.label,
          payload = DetectOutliersInSeries( s, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
          history = historyWith( None, series ),
          properties = plan.algorithms.getOrElse( defaultAlgorithm.label, emptyConfig )
        )
      )(
          sender.ref
        )

      val anomalyPositions = Set.empty[Int]
      val expectedAnomolies = series.points.zipWithIndex.collect { case ( dp, i ) if anomalyPositions.contains( i ) ⇒ dp }

      sender.expectMsgPF( 3.seconds.dilated, "result" ) {
        case m @ Envelope( NoOutliers( a, ts, p, tb ), _ ) ⇒ {
          log.info(
            Map(
              "@msg" → "evaluate series anomaly results",
              "algorithms" → a.toString,
              "source" → ts.toString,
              "plan" → p.toString,
              "threshold-boundaries" → tb.toString
            )
          )

          a mustBe Set( defaultAlgorithm.label )
          ts mustBe s
        }
      }
    }

    "calculate threshold boundaries" in { f: Fixture ⇒
      import f._

      val tol = 0.0000001

      val smaConfig = ConfigFactory.parseString(
        s"""
          |tail-average: 3
          |tolerance: 2
          |minimum-population: 3
        """.stripMargin
      )

      when( plan.algorithms ) thenReturn { Map( defaultAlgorithm.label → smaConfig ) }

      val pts = assemble( series.points, Seq( 0, 1, 2, 31, 60, 91, 120, 149 ) )
      val s = TimeSeries( series.topic, pts )

      aggregate.sendEnvelope(
        DetectUsing(
          targetId = id,
          algorithm = defaultAlgorithm.label,
          payload = DetectOutliersInSeries( s, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
          history = historyWith( None, series ),
          properties = plan.algorithms.getOrElse( defaultAlgorithm.label, emptyConfig )
        )
      )(
          sender.ref
        )

      //      countDown await 1.second
      val anomalyPositions = Set.empty[Int]
      val expectedAnomolies = pts.zipWithIndex.collect { case ( dp, i ) if anomalyPositions.contains( i ) ⇒ dp }
      val expectedThresholds = Seq(
        ThresholdBoundary( pts( 0 ).timestamp, None, None, None ),
        ThresholdBoundary( pts( 1 ).timestamp, None, None, None ),
        ThresholdBoundary( pts( 2 ).timestamp, None, None, None ),
        ThresholdBoundary( pts( 3 ).timestamp, None, None, None ),
        ThresholdBoundary( pts( 4 ).timestamp, Some( 5.478123273 ), Some( 90.15104633 ), Some( 174.8239694 ) ),
        ThresholdBoundary( pts( 5 ).timestamp, Some( 32.8057229 ), Some( 94.86997636 ), Some( 156.9342298 ) ),
        ThresholdBoundary( pts( 6 ).timestamp, Some( 95.32819703 ), Some( 116.0948298 ), Some( 136.8614626 ) ),
        ThresholdBoundary( pts( 7 ).timestamp, Some( 60.72913575 ), Some( 103.2144051 ), Some( 145.6996745 ) )
      )

      sender.expectMsgPF( 3.seconds.dilated, "result" ) {
        case m @ Envelope( NoOutliers( a, ts, p, tb ), _ ) ⇒ {
          log.info(
            Map(
              "@msg" → "evaluate series anomaly results",
              "algorithms" → a.toString,
              "source" → ts.toString,
              "plan" → p.toString,
              "threshold-boundaries" → tb.toString
            )
          )

          a mustBe Set( defaultAlgorithm.label )
          ts mustBe s
          tb( defaultAlgorithm.label ).zip( expectedThresholds ) foreach {
            case ( actual, expected ) ⇒
              log.info(
                Map(
                  "@msg" → "checking thresholds",
                  "actual" → actual.toString,
                  "expected" → expected.toString
                )
              )
              //              countDown await 1.second
              actual.timestamp mustBe expected.timestamp
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

      //      countDown await 1.second

    }

    "handle example Jul thru Oct" in { f: Fixture ⇒
      import f._

      val ppConfig = ConfigFactory.parseString(
        s"""
          |tail-average: 3
          |tolerance: 3
          |minimum-population: 2
        """.stripMargin
      )

      when( plan.algorithms ) thenReturn { Map( defaultAlgorithm.label → ppConfig ) }

      def beforeCap( points: Seq[DataPoint] ): Seq[DataPoint] = {
        val cap = new joda.LocalDate( 2016, 11, 1 ).toDateTimeAtStartOfDay
        import com.github.nscala_time.time.Imports._
        points filter { _.timestamp < cap }
      }

      val s = TimeSeries( topic = series.topic, points = beforeCap( series.points ) )
      aggregate.sendEnvelope(
        DetectUsing(
          targetId = id,
          algorithm = defaultAlgorithm.label,
          payload = DetectOutliersInSeries( s, plan, Option( subscriber.ref ), Set.empty[WorkId] ),
          history = historyWith( None, series ),
          properties = plan.algorithms.getOrElse( defaultAlgorithm.label, emptyConfig )
        )
      )(
          sender.ref
        )

      //      countDown await 1.second
      val anomalyPositions = Set.empty[Int]
      val expectedAnomolies = series.points.zipWithIndex.collect { case ( dp, i ) if anomalyPositions.contains( i ) ⇒ dp }

      sender.expectMsgPF( 3.seconds.dilated, "result" ) {
        case m @ Envelope( NoOutliers( a, ts, p, tb ), _ ) ⇒ {
          log.info(
            Map(
              "@msg" → "evaluate series anomaly results",
              "algorithms" → a.toString,
              "source" → ts.toString,
              "plan" → p.toString,
              "threshold-boundaries" → tb.toString
            )
          )

          a mustBe Set( defaultAlgorithm.label )
          ts mustBe s
        }
      }

      //      countDown await 1.second
    }

    "handle example data" in { f: Fixture ⇒
      import f._

      val ppConfig = ConfigFactory.parseString(
        s"""
                                                   |tail-average: 3
                                                   |tolerance: 2
                                                   |minimum-population: 2
        """.stripMargin
      )

      when( plan.algorithms ) thenReturn { Map( defaultAlgorithm.label → ppConfig ) }

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
      val anomalyPositions = Set( 98, 99, 100, 115, 116, 117, 118, 119, 120 )
      val expectedAnomolies = series.points.zipWithIndex.collect { case ( dp, i ) if anomalyPositions.contains( i ) ⇒ dp }

      sender.expectMsgPF( 3.seconds.dilated, "result" ) {
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

object PastPeriodAverageAlgorithmSpec {
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
      DataPoint( timestamp = new joda.DateTime( 1472515200000L ), value = 65.37633751909686 ),
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