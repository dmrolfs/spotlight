package spotlight.model.statistics

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.scalatest._

/** Created by rolfsd on 2/28/17.
  */
class MovingStatisticsSpec extends fixture.WordSpec with MustMatchers with ParallelTestExecution with TryValues {
  class Fixture { outer ⇒
    val tol = 1E-12
    val window = 5
    val dstats: DescriptiveStatistics = new DescriptiveStatistics( window )
  }

  override type FixtureParam = Fixture

  def makeTestFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = {
    val fixture = makeTestFixture()
    try { test( fixture ) } finally {}
  }

  object WIP extends Tag( "wip" )

  "MovingStatistics" should {
    "test summary" in { f: Fixture ⇒
      import f._

      val DataSize = 20
      val stats: MovingStatistics = ( 0 until DataSize ).foldLeft( MovingStatistics( window ) ) { ( s, v ) ⇒
        dstats.addValue( v )
        s :+ v
      }

      stats.N mustBe DataSize
      stats.window.size mustBe dstats.getN
      stats.sum mustBe dstats.getSum
      stats.minimum mustBe dstats.getMin
      stats.maximum mustBe dstats.getMax
      stats.mean mustBe ( dstats.getMean +- tol )
      stats.variance mustBe ( dstats.getVariance +- tol )
      stats.standardDeviation mustBe ( dstats.getStandardDeviation +- tol )
    }
  }
}
