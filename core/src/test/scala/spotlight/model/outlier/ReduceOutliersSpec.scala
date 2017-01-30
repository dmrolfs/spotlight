package spotlight.model.outlier

import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.joda.{time => joda}
import com.github.nscala_time.time.Imports._
import com.typesafe.scalalogging.LazyLogging
import peds.commons.log.Trace
import spotlight.model.timeseries.{ThresholdBoundary, DataPoint, TimeSeries}


class ReduceOutliersSpec
extends fixture.WordSpec
with MockitoSugar
with MustMatchers
with ParallelTestExecution
with TryValues
with LazyLogging {
  val trace = Trace[ReduceOutliersSpec]

  override type FixtureParam = Fixture

  class Fixture { outer =>
    val algo1 = 'algo1
    val algo2 = 'algo2
    val algo3 = 'algo3
    val now = joda.DateTime.now
    val points = Seq( DataPoint(now, 1.01), DataPoint(now+1.second, 2.02), DataPoint(now+2.seconds, 3.02) )
    val series = TimeSeries( "foo", points )

    val all = mock[AnalysisPlan.AppliesTo]
    when( all.apply(any) ) thenReturn true

    val plan = mock[AnalysisPlan]
    when( plan.algorithms ) thenReturn Set( algo1, algo2, algo3 )
    when( plan.appliesTo ).thenReturn( all )


    def makeOutliers( algorithms: Set[Symbol], outlierIndexes: Seq[Int], controls: Map[Symbol, Seq[ThresholdBoundary]] ): Outliers = trace.block( s"""makeOutliers( ${outlierIndexes.mkString("," )} )""" ) {
      val noOutliers = NoOutliers( Set(algo1), source = series, plan = plan )

      val outliers = outlierIndexes map { i => points(i) }
      logger.debug( " - outlier points: [{}]", outliers.mkString(",") )

      val result = Outliers.forSeries(
        algorithms = algorithms,
        plan = plan,
        source = series,
        outliers = outliers,
                                       thresholdBoundaries = controls
      )

      logger.debug( "makeOutliers result = [{}]", result )
      result getOrElse noOutliers
    }
//    val oneOutlier = SeriesOutliers( Set(algo1), source = series, outliers = IndexedSeq(points(0)), plan = plan )
//    val twoOutliers = SeriesOutliers( Set(algo1), source = series, outliers = IndexedSeq( points(0), points(1) ), plan = plan )

    def correspndingControlBoundaries( algorithms: Set[Symbol], source: TimeSeries ): Map[Symbol, Seq[ThresholdBoundary]] = {
      val expected = 5D

      val elems = algorithms.toSeq map { a =>
        val boundaries = source.points.zipWithIndex map { case (p, i) =>
          ThresholdBoundary( p.timestamp, floor = Some( expected - i ), Some( expected ), ceiling = Some( expected + i ) )
        }

        (a, boundaries)
      }

      Map( elems:_* )
    }
  }

  def makeTestFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = {
    val fixture = makeTestFixture()
    try {
      test( fixture )
    } finally {
    }
  }

  object WIP extends Tag( "wip" )


  "ReduceOutliers" should {
    "seriesIntersection" should {
      "reduce results - single alogorithm" in { f: Fixture =>
        import f._
        import ReduceOutliers._
        val oneControls = correspndingControlBoundaries( Set(algo1), series )
        val oneOutlier = makeOutliers( Set(algo1), 0 :: Nil, oneControls )
        oneOutlier.anomalySize mustBe 1
        logger.debug( "TEST oneOutlier = {}", oneOutlier )
        val reduce = byCorroborationCount( 1 )
        val actual = reduce( results = Map( algo1 -> oneOutlier ), series, plan )
        val expected = Outliers.forSeries( Set(algo1), plan, series, points.take(1), oneControls ).disjunction
        actual mustBe expected
      }

      "reduce results - two alogorithms" in { f: Fixture =>
        import f._
        import ReduceOutliers._
        val controls1 = correspndingControlBoundaries( Set(algo1), series )
        val controls2 = correspndingControlBoundaries( Set(algo2), series )
        val outliers1 = makeOutliers( Set(algo1), 0 :: 1 :: Nil, controls1 )
        val outliers2 = makeOutliers( Set(algo2), 2 :: 1 :: Nil, controls2 )
        outliers1.anomalySize mustBe 2
        outliers2.anomalySize mustBe 2
        val reduce = byCorroborationCount( 2 )
        val actual = reduce( results = Map( algo1 -> outliers1, algo2 -> outliers2 ), series, plan )
        val expected = Outliers.forSeries( Set(algo1, algo2), plan, series, Seq(points(1)), controls1 ++ controls2 ).disjunction
        actual mustBe expected
      }

      "reduce results - three alogorithms corroborate by percentage" taggedAs (WIP) in { f: Fixture =>
        import f._
        import ReduceOutliers._
        val controls1 = correspndingControlBoundaries( Set(algo1), series )
        val controls2 = correspndingControlBoundaries( Set(algo2), series )
        val controls3 = correspndingControlBoundaries( Set(algo3), series )
        val outliers1 = makeOutliers( Set(algo1), 0 :: 1 :: Nil, controls1 )
        val outliers2 = makeOutliers( Set(algo2), 2 :: 1 :: Nil, controls2 )
        val outliers3 = makeOutliers( Set(algo3), 2 :: Nil, controls3 )
        outliers1.anomalySize mustBe 2
        outliers2.anomalySize mustBe 2
        outliers3.anomalySize mustBe 1
        val reduce = byCorroborationPercentage( 50 )
        val actual = reduce( results = Map( algo1 -> outliers1, algo2 -> outliers2, algo3 -> outliers3 ), series, plan )
        val expected = Outliers.forSeries( Set(algo1, algo2, algo3), plan, series, points.drop(1), controls1 ++ controls2 ++ controls3 ).disjunction
        actual mustBe expected
      }

      "handle empty" in { f: Fixture =>
        import f._
        import ReduceOutliers._

        val noControls = Map.empty[Symbol, Seq[ThresholdBoundary]]
        val noOutliers = makeOutliers( Set(algo1), Nil, noControls )
        val reduce = byCorroborationCount( 1 )
        val actual = reduce( results = Map( algo1 -> noOutliers ), series, plan )
        val expected = Outliers.forSeries( Set(algo1), plan, series, points.take(0), noControls ).disjunction
        actual mustBe expected
      }
    }
  }
}
