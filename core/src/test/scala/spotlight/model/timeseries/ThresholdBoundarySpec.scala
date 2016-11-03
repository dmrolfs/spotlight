package spotlight.model.timeseries

import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.scalatest._
import org.joda.{time => joda}
import peds.commons.log.Trace


/**
  * Created by rolfsd on 5/27/16.
  */
class ThresholdBoundarySpec
extends org.scalatest.fixture.WordSpec
with MustMatchers
with ParallelTestExecution
with TryValues {
  val trace = Trace[ThresholdBoundarySpec]

  class Fixture { outer => }

  override type FixtureParam = Fixture

  def makeTestFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = {
    val fixture = makeTestFixture()
    try { test( fixture ) } finally { }
  }


  object WIP extends Tag( "wip" )


  "ThresholdBoundary" should {
    "create empty threshold boundary" in { f: Fixture =>
      val now = joda.DateTime.now
      val actual = ThresholdBoundary.empty( now )
      actual.timestamp mustBe now
      actual.floor mustBe None
      actual.expected mustBe None
      actual.ceiling mustBe None
    }

    "create threshold by distance" in { f: Fixture =>
      val now = joda.DateTime.now
      val data = Seq[Double]( 96, 86, 76, 106, 89, 86, 143, 117, 123, 74, 76, 58, 72, 170, 87 )
      val meanDistanceData = Seq[(Double, Double)](
        (48,0), (46,9.5), (43,15), (59,95), (54,114), (51,120), (55,149), (51,142),
        (57,148), (54,144), (52,143), (50,139), (48,138), (52,155), (51,153)
      )

      val actual = meanDistanceData.zipWithIndex.map{ case ((e,d), i) =>
        ThresholdBoundary.fromExpectedAndDistance( timestamp = now.plusSeconds(i * 10), expected = e, distance = d )
      }

      val expectedData = Seq[(Double,Double,Double)](
        (48,48,48),
        (55.5,46,36.5),
        (58,43,28),
        (154,59,-36),
        (168,54,-60),
        (171,51,-69),
        (204,55,-94),
        (193,51,-91),
        (205,57,-91),
        (198,54,-90),
        (195,52,-91),
        (189,50,-89),
        (186,48,-90),
        (207,52,-103),
        (204,51,-102)
      )
      val expected = expectedData.zipWithIndex.map{ case ((c,e,f),i) =>
        ThresholdBoundary( timestamp = now.plusSeconds(i * 10), ceiling = Some(c), expected = Some(e), floor = Some(f) )
      }

      actual.zip(expected).zipWithIndex foreach { case ((a, e), i) => (i, a) mustBe (i, e) }
    }

    "test actual scenario with rolling average" taggedAs (WIP) in { fixture: Fixture =>
      val expectedData = Seq[(Double,Double,Double)](
        (96,96,96),
        (69.78679656440357, 91.0, 112.21320343559643),
        (56,86,116),
        (52.27016653792583,91,129.72983346207417),
        (56.95182025725611,90.6,124.24817974274387),
        (59.214711548543605,89.83333333333333,120.45195511812305),
        (30.978805224532536,97.42857142857143,163.87833763261034),
        (34.94660537506391,99.875,164.8033946249361),
        (37.45598393028855,102.44444444444444,167.43290495860032),
        (32.64927184861989,99.6,166.5507281513801),
        (30.448168553129975,97.45454545454545,164.46092235596092),
        (21.715429146650365,94.16666666666667,166.61790418668298),
        (20.684640528268346,92.46153846153847,164.2384363948086),
        (5.152730871525449,98,190.84726912847455),
        (7.391992019927926,97.26666666666667,187.1413413134054)
      )

      val now = joda.DateTime.now
      val stats = new SummaryStatistics()
      val data = Seq[Double]( 96, 86, 76, 106, 89, 86, 143, 117, 123, 74, 76, 58, 72, 170, 87 )
      data.zip(expectedData).zipWithIndex foreach { case ((d, (ef, ee, ec)), i) =>
        val s = stats.addValue( d )
        val ts = now.plusSeconds( i * 10 )
        val actual = ThresholdBoundary.fromExpectedAndDistance(
          timestamp = ts,
          expected = stats.getMean,
          distance = (3.0 * stats.getStandardDeviation)
        )

        val expected = ThresholdBoundary( timestamp = ts, floor = Some(ef), expected = Some(ee), ceiling = Some(ec) )
        actual.floor.get mustBe ( expected.floor.get +- 0.00001 )
        actual.expected.get mustBe ( expected.expected.get +- 0.00001 )
        actual.ceiling.get mustBe ( expected.ceiling.get +- 0.00001 )
      }
    }
  }
}
