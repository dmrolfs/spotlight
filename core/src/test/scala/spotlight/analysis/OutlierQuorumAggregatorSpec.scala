package spotlight.analysis

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.{ ActorSystem, Terminated }
import akka.testkit._
import org.scalatest.Outcome
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import com.typesafe.config.{ Config, ConfigFactory }
import peds.akka.envelope.Envelope
import peds.commons.V
import spotlight.model.timeseries.{ ThresholdBoundary, TimeSeries, TimeSeriesBase, Topic }
import spotlight.testkit.ParallelAkkaSpec
import peds.commons.log.Trace
import spotlight.model.outlier._

/** Created by damonrolfs on 9/18/14.
  */
class OutlierQuorumAggregatorSpec extends ParallelAkkaSpec with MockitoSugar {
  override val trace = Trace[OutlierQuorumAggregatorSpec]

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AkkaFixture( _config, _system, _slug ) {
    fixture ⇒

    val defaultPlan = plan( 3.seconds )
    val none = mock[NoOutliers]
    when( none.plan ) thenReturn defaultPlan
    when( none.topic ) thenReturn Topic( "metric.none" )
    when( none.algorithms ) thenReturn defaultPlan.algorithms.take( 1 )
    when( none.thresholdBoundaries ) thenReturn Map.empty[String, Seq[ThresholdBoundary]]

    val some = mock[SeriesOutliers]
    when( some.plan ) thenReturn defaultPlan
    when( some.topic ) thenReturn Topic( "metric.some" )
    when( some.algorithms ) thenReturn defaultPlan.algorithms.take( 1 )
    when( some.thresholdBoundaries ) thenReturn Map.empty[String, Seq[ThresholdBoundary]]

    val demoReduce = new ReduceOutliers {
      override def apply( results: OutlierAlgorithmResults, source: TimeSeriesBase, plan: AnalysisPlan ): V[Outliers] = {
        import scalaz.Scalaz._
        val outlierCount = results.foldLeft( 0 ) { ( cnt, r ) ⇒ cnt + r._2.anomalySize }
        val result = if ( outlierCount > 0 ) fixture.some else fixture.none
        result.right
      }
    }

    val grouping: Option[AnalysisPlan.Grouping] = {
      val window = None
      window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
    }

    def plan( to: FiniteDuration ): AnalysisPlan = {
      AnalysisPlan.default(
        name = "default",
        timeout = to,
        isQuorum = IsQuorum.AtLeastQuorumSpecification( 1, 1 ),
        reduce = demoReduce,
        algorithms = Set( "foobar" ),
        grouping = grouping,
        planSpecification = ConfigFactory.empty
      )
    }
  }

  "OutlierQuorumAggregator" should {
    "die after no timely response" in { f: Fixture ⇒
      import f._
      val p = plan( 5.millis )

      val aggregator = TestActorRef[OutlierQuorumAggregator]( OutlierQuorumAggregator.props( p, TimeSeries( "empty" ) ) )

      val probe = TestProbe()
      probe watch aggregator

      probe.expectMsgPF( 1.second.dilated, "death" ) {
        case Terminated( actor ) ⇒ actor mustBe aggregator
      }
    }

    "send results upon satisfying quorum" taggedAs ( WIP ) in { f: Fixture ⇒
      import f._
      val p = plan( 2.seconds )

      val destination = TestProbe()
      val aggregator = TestActorRef[OutlierQuorumAggregator](
        OutlierQuorumAggregator.props( p, TimeSeries( "dummy" ) ),
        destination.ref,
        "aggregator"
      )

      //      val outliers = NoOutliers( algorithms = p.algorithms, source = TimeSeries("metric.name"), plan = p )
      val outliers = mock[Outliers]
      when( outliers.plan ) thenReturn p
      when( outliers.topic ) thenReturn Topic( "metric.specific" )
      when( outliers.algorithms ) thenReturn p.algorithms.take( 1 )
      when( outliers.thresholdBoundaries ) thenReturn Map.empty[String, Seq[ThresholdBoundary]]

      log.info( "outliers = [{}]", outliers )
      log.info( "outliers.plan = [{}]", outliers.plan )
      log.info( "outliers.plan.name = [{}]", outliers.plan.name )
      log.info( "outliers.topic = [{}]", outliers.topic )
      log.info( "outliers.anomalySize = [{}]", outliers.anomalySize )

      aggregator.receive( outliers )
      destination.expectMsgPF( hint = "destination expectation" ) { case Envelope( m, _ ) ⇒ { m mustBe a[NoOutliers] } }
    }
  }
}

