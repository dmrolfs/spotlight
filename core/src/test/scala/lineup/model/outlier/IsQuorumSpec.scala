package lineup.model.timeseries

import lineup.model.outlier.IsQuorum.{ MajorityQuorumSpecification, AtLeastQuorumSpecification }
import lineup.model.outlier.{ OutlierPlan, Outliers, SeriesOutliers, NoOutliers }
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.joda.{ time => joda }
import com.github.nscala_time.time.Imports._
import peds.commons.log.Trace


class IsQuorumSpec
extends fixture.WordSpec
with MockitoSugar
with MustMatchers
with ParallelTestExecution
with TryValues {
  val trace = Trace[IsQuorumSpec]

  type Fixture = TestFixture
  override type FixtureParam = Fixture

  class TestFixture { outer =>
    val now = joda.DateTime.now
    val points = Row( DataPoint(now, 1.01), DataPoint(now+1.second, 2.02), DataPoint(now+2.seconds, 3.02) )
    val series = TimeSeries( "foo", points )
    val plan = mock[OutlierPlan]
    val noOutliers = NoOutliers( Set('algo), source = series, plan = plan )
    val oneOutlier = SeriesOutliers( Set('algo), source = series, outliers = IndexedSeq(points(0)), plan = plan )
    val twoOutliers = SeriesOutliers( Set('algo), source = series, outliers = IndexedSeq( points(0), points(1) ), plan = plan )
  }

  def createTestFixture(): Fixture = trace.block( "createTestFixture" ) { new Fixture }

  override def withFixture( test: OneArgTest ): Outcome = {
    val fixture = createTestFixture()
    try {
      test( fixture )
    } finally {
    }
  }

  object WIP extends Tag( "wip" )


  "IsQuorm" should {
    "AtLeastQuorum" should {
      "at least: answer given empty results" in { f: Fixture =>
        val fn0 = AtLeastQuorumSpecification( 3, 0 )
        val fn1 = AtLeastQuorumSpecification( 3, 1 )
        val fn2 = AtLeastQuorumSpecification( 3, 2 )
        val fn3 = AtLeastQuorumSpecification( 3, 3 )
        fn0( Map.empty[Symbol, Outliers] ) mustBe true
        fn1( Map.empty[Symbol, Outliers] ) mustBe false
        fn2( Map.empty[Symbol, Outliers] ) mustBe false
        fn3( Map.empty[Symbol, Outliers] ) mustBe false
      }

      "at least: answer false for no anomalies" in { f: Fixture =>
        val fn1 = AtLeastQuorumSpecification( 3, 1 )
        fn1( Map('a -> f.noOutliers) ) mustBe false
      }

      "at least: answer false for too few anomalous series" in { f: Fixture =>
        val fn3 = AtLeastQuorumSpecification( 3, 3 )
        fn3( Map('a -> f.oneOutlier) ) mustBe false
        fn3( Map('a -> f.twoOutliers, 'b -> f.oneOutlier) ) mustBe false

        val fn2 = AtLeastQuorumSpecification( 3, 2 )
        fn2( Map('a -> f.twoOutliers, 'b -> f.noOutliers) ) mustBe false
      }

      "at least: answer true if to few anomalous but all respond" in { f: Fixture =>
        val fn3 = AtLeastQuorumSpecification( 3, 3 )
        fn3( Map('a -> f.noOutliers, 'b -> f.noOutliers, 'c -> f.noOutliers) ) mustBe true
        fn3( Map('a -> f.twoOutliers, 'b -> f.oneOutlier, 'c -> f.noOutliers) ) mustBe true

        val fn2 = AtLeastQuorumSpecification( 3, 2 )
        fn2( Map('a -> f.noOutliers, 'b -> f.noOutliers, 'c -> f.noOutliers) ) mustBe true
        fn2( Map('a -> f.twoOutliers, 'b -> f.noOutliers, 'c -> f.noOutliers) ) mustBe true
      }

      "at least: answer true for minimal anomalies" in { f: Fixture =>
        val fn0 = AtLeastQuorumSpecification( 3, 0 )
        fn0( Map(f.noOutliers.algorithms.head -> f.noOutliers) ) mustBe true

        val fn1 = AtLeastQuorumSpecification( 3, 1 )
        fn1( Map('a -> f.oneOutlier) ) mustBe true

        val fn2 = AtLeastQuorumSpecification( 3, 2 )
        fn2( Map('a -> f.twoOutliers, 'b -> f.oneOutlier) ) mustBe true
      }

      "at least: answer true for over anomalies" in { f: Fixture =>
        val fn0 = AtLeastQuorumSpecification( 3, 0 )
        fn0( Map('a -> f.oneOutlier) ) mustBe true

        val fn1 = AtLeastQuorumSpecification( 3, 1 )
        fn1( Map('a -> f.twoOutliers, 'b -> f.twoOutliers) ) mustBe true
      }
    }

    "MajorityQuorum" should {
      "majority: answer given empty results" in { f: Fixture =>
        val fn0 = MajorityQuorumSpecification( 3, 0.0 )
        val fn1 = MajorityQuorumSpecification( 3, 0.33 )
        val fn2 = MajorityQuorumSpecification( 3, 0.66 )
        val fn3 = MajorityQuorumSpecification( 3, 1.0 )
        fn0( Map.empty[Symbol, Outliers] ) mustBe true
        fn1( Map.empty[Symbol, Outliers] ) mustBe false
        fn2( Map.empty[Symbol, Outliers] ) mustBe false
        fn3( Map.empty[Symbol, Outliers] ) mustBe false
      }

      "majority: answer false for no anomalies" in { f: Fixture =>
        val fn1 = MajorityQuorumSpecification( 3, 0.33 )
        fn1( Map('a -> f.noOutliers) ) mustBe false
      }

      "majority: answer false for too few anomalous series" taggedAs(WIP) in { f: Fixture =>
        val fn3 = MajorityQuorumSpecification( 3, 1.0 )
        fn3( Map('a -> f.oneOutlier) ) mustBe false
        fn3( Map('a -> f.twoOutliers, 'b -> f.oneOutlier) ) mustBe false

        val fn2 = MajorityQuorumSpecification( 3, 0.66 )
        fn2( Map('a -> f.twoOutliers, 'b -> f.noOutliers) ) mustBe false
      }

      "majority: answer true if to few anomalous but all respond" in { f: Fixture =>
        val fn3 = AtLeastQuorumSpecification( 3, 3 )
        fn3( Map('a -> f.noOutliers, 'b -> f.noOutliers, 'c -> f.noOutliers) ) mustBe true
        fn3( Map('a -> f.twoOutliers, 'b -> f.oneOutlier, 'c -> f.noOutliers) ) mustBe true

        val fn2 = AtLeastQuorumSpecification( 3, 2 )
        fn2( Map('a -> f.noOutliers, 'b -> f.noOutliers, 'c -> f.noOutliers) ) mustBe true
        fn2( Map('a -> f.twoOutliers, 'b -> f.noOutliers, 'c -> f.noOutliers) ) mustBe true
      }

      "majority: answer true for minimal anomalies" in { f: Fixture =>
        val fn0 = MajorityQuorumSpecification( 3, 0.0 )
        fn0( Map(f.noOutliers.algorithms.head -> f.noOutliers) ) mustBe true

        val fn1 = MajorityQuorumSpecification( 3, 0.33 )
        fn1( Map('a -> f.oneOutlier) ) mustBe true

        val fn2 = MajorityQuorumSpecification( 3, 0.66 )
        fn2( Map('a -> f.twoOutliers, 'b -> f.oneOutlier) ) mustBe true
      }

      "majority: answer true for over anomalies" in { f: Fixture =>
        val fn0 = MajorityQuorumSpecification( 3, 0.0 )
        fn0( Map('a -> f.oneOutlier) ) mustBe true

        val fn1 = MajorityQuorumSpecification( 3, 0.33 )
        fn1( Map('a -> f.twoOutliers, 'b -> f.twoOutliers) ) mustBe true
      }
    }
  }
}
