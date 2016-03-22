package spotlight.analysis.outlier

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.actor.Terminated
import akka.testkit._
import org.scalatest.{Outcome, Tag}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.typesafe.config.ConfigFactory
import peds.commons.V
import spotlight.model.timeseries.{ControlBoundary, TimeSeries, TimeSeriesBase, Topic}
import spotlight.testkit.ParallelAkkaSpec
import peds.commons.log.Trace
import spotlight.model.outlier._


/**
 * Created by damonrolfs on 9/18/14.
 */
class OutlierQuorumAggregatorSpec extends ParallelAkkaSpec with MockitoSugar {
  override val trace = Trace[OutlierQuorumAggregatorSpec]

  class Fixture extends AkkaFixture { fixture =>
    val none = mock[NoOutliers]
    val some = mock[SeriesOutliers]

    val demoReduce = new ReduceOutliers {
      override def apply( results: OutlierAlgorithmResults, source: TimeSeriesBase, plan: OutlierPlan ): V[ Outliers ] = {
        import scalaz.Scalaz._
        val outlierCount = results.foldLeft( 0 ) { (cnt, r) => cnt + r._2.anomalySize }
        val result = if ( outlierCount > 0 ) fixture.some else fixture.none
        result.right
      }
    }

    def plan( to: FiniteDuration ) = OutlierPlan.default(
      name = "default",
      timeout = to,
      isQuorum = IsQuorum.AtLeastQuorumSpecification( 1, 1 ),
      reduce = demoReduce,
      algorithms = Set( 'foobar ),
      planSpecification = ConfigFactory.empty
    )

  }

  override def makeAkkaFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = trace.block( s"withFixture($test)" ) {
    val f = makeAkkaFixture()

    try {
      f.before()
      test( f )
    } finally {
      import f._
      f.after()
      val terminated = f.system.terminate()
      Await.result( terminated, 3.seconds.dilated )
    }
  }


  "OutlierQuorumAggregator" should  {
    "die after no timely response" in { f: Fixture =>
      import f._
      val p = plan( 5.millis )

      val aggregator = TestActorRef[OutlierQuorumAggregator]( OutlierQuorumAggregator.props(p, TimeSeries("empty")) )

      val probe = TestProbe()
      probe watch aggregator

      probe.expectMsgPF( 1.second.dilated, "death" ) {
        case Terminated( actor ) => actor mustBe aggregator
      }
    }

    "send results upon satisfying quorum" in { f: Fixture =>
      import f._
      val p = plan( 2.seconds )

      val destination = TestProbe()
      val aggregator = TestActorRef[OutlierQuorumAggregator](
        OutlierQuorumAggregator.props(p, TimeSeries("dummy")),
        destination.ref,
        "aggregator"
      )

      val outliers = mock[Outliers]
      when( outliers.topic ) thenReturn Topic( "metric.name" )
      when( outliers.algorithms ) thenReturn p.algorithms.take(1)
      when( outliers.algorithmControlBoundaries ) thenReturn Map.empty[Symbol, Seq[ControlBoundary]]

      aggregator.receive( outliers )
      destination.expectMsg( none )
    }
  }
}

