package lineup.analysis.outlier

import scala.concurrent.{ Future, ExecutionContext, Await }
import scala.concurrent.duration._
import akka.actor.Terminated
import akka.testkit._
import org.scalatest.{ Outcome, Tag }
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.typesafe.config.ConfigFactory
import lineup.model.timeseries.{ TimeSeriesBase, TimeSeries, Topic }
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

    val demoReduce = new ReduceOutliers {
      override def apply(
        results: SeriesOutlierResults,
        source: TimeSeriesBase
      )(
        implicit ec: ExecutionContext
      ): Future[Outliers] = {
        Future { results.headOption map { _._2 } getOrElse { NoOutliers( algorithms = Set('foobar), source = source ) } }
      }
    }

    def plan( to: FiniteDuration ) = OutlierPlan.default(
      name = "default",
      timeout = to,
      isQuorum = IsQuorum.AtLeastQuorumSpecification( 1, 1 ),
      reduce = demoReduce,
      algorithms = Set( 'foobar ),
      specification = ConfigFactory.empty
    )

  }

  override def createAkkaFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = trace.block( s"withFixture($test)" ) {
    val f = createAkkaFixture()

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

  case object WIP extends Tag( "wip" )

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

    "send results upon satisfying quorum" taggedAs(WIP) in { f: Fixture =>
      import f._
//      when( plan.timeout ) thenReturn 2.seconds
//      when( plan.isQuorum ) thenReturn new IsQuorum {
//        override def apply( results: SeriesOutlierResults ): Boolean = true
//        override val totalIssued: Int = 1
//      }
//      when( plan.reduce ) thenReturn new ReduceOutliers {
//        override def apply(
//          results: SeriesOutlierResults,
//          source: TimeSeriesBase
//        )(
//          implicit ec: ExecutionContext
//        ): Future[Outliers] = Future { results.head._2 }
//      }
      val p = plan( 2.seconds )

      val destination = TestProbe()
      val aggregator = TestActorRef[OutlierQuorumAggregator](
        OutlierQuorumAggregator.props(p, TimeSeries("dummy")),
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

