package lineup.analysis.outlier

import lineup.model.timeseries.{TimeSeriesBase, DataPoint, TimeSeries, Topic}

import scala.concurrent.duration._
import akka.actor.Terminated
import akka.testkit._
import org.scalatest.{ Outcome, Tag }
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import demesne.testkit.ParallelAkkaSpec
import peds.commons.log.Trace
import peds.akka.envelope._
import lineup.model.outlier._


/**
 * Created by damonrolfs on 9/18/14.
 */
class OutlierQuorumAggregatorSpec extends ParallelAkkaSpec with MockitoSugar {
  val trace = Trace[OutlierQuorumAggregatorSpec]

  class Fixture extends AkkaFixture {
    def before(): Unit = { }
    def after(): Unit = { }
    val plan = mock[OutlierPlan]
  }

  override def createAkkaFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = trace.block( s"withFixture($test)" ) {
    val sys = createAkkaFixture()

    try {
      sys.before()
      test( sys )
    } finally {
      sys.after()
      sys.system.shutdown()
    }
  }

  case object WIP extends Tag( "wip" )

  "OutlierQuorumAggregator" should  {
    "die after no timely response" in { f: Fixture =>
      import f._
      when( plan.timeout ) thenReturn 5.millis

      val aggregator = TestActorRef[OutlierQuorumAggregator]( OutlierQuorumAggregator.props(plan, TimeSeries("empty")) )

      val probe = TestProbe()
      probe watch aggregator

      probe.expectMsgPF( 1.second.dilated, "death" ) {
        case Terminated( actor ) => actor mustBe aggregator
      }
    }

    "send results upon satisfying quorum" in { f: Fixture =>
      import f._
      when( plan.timeout ) thenReturn 2.seconds
      when( plan.isQuorum ) thenReturn new IsQuorum {
        override def apply( results: SeriesOutlierResults ): Boolean = true
        override val totalIssued: Int = 1
      }
      when( plan.reduce ) thenReturn new ReduceOutliers {
        override def apply( results: SeriesOutlierResults, source: TimeSeriesBase ): Outliers = results.head._2
      }

      val destination = TestProbe()
      val aggregator = TestActorRef[OutlierQuorumAggregator](
        OutlierQuorumAggregator.props(plan, TimeSeries("dummy")),
        destination.ref,
        "aggregator"
      )

      val outliers = mock[Outliers]
      when( outliers.topic ) thenReturn Topic( "metric.name" )
      when( outliers.algorithms ) thenReturn Set('fooAlgorithm)

      aggregator.receive( outliers )
      destination.expectMsg( outliers )
    }
  }
}

