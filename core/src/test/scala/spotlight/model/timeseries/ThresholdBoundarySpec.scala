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

    "test actual scenario with rolling average" in { fixture: Fixture =>
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

      val now = joda.DateTime.now
      val stats = new SummaryStatistics()
      val data = Seq[Double]( 96, 86, 76, 106, 89, 86, 143, 117, 123, 74, 76, 58, 72, 170, 87 )
      data.zip(expectedData).zipWithIndex foreach { case ((d, (ec, ee, ef)), i) =>
        val s = stats.addValue( d )
        val ts = now.plusSeconds( i * 10 )
        val actual = ThresholdBoundary.fromExpectedAndDistance(
          timestamp = ts,
          expected = stats.getMean,
          distance = (3.0 * stats.getStandardDeviation)
        )

        val expected = ThresholdBoundary( timestamp = ts, floor = Some(ef), expected = Some(ee), ceiling = Some(ec) )
        (i, actual) mustBe (i, expected)
      }
    }
  }
}
