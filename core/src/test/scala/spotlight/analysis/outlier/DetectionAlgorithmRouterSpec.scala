package spotlight.analysis.outlier

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.Config
import org.joda.{time => joda}
import org.scalatest.mockito.MockitoSugar
import peds.akka.envelope.{Envelope, WorkId}
import spotlight.analysis.outlier.AnalysisPlanProtocol.AcceptTimeSeries
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.{DataPoint, TimeSeries}
import spotlight.testkit.ParallelAkkaSpec


/**
 * Created by rolfsd on 10/20/15.
 */
class DetectionAlgorithmRouterSpec extends ParallelAkkaSpec with MockitoSugar {

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AkkaFixture( _config, _system, _slug ) {
    val algo = 'foo
    val algorithm = TestProbe()
    val resolver = DetectionAlgorithmRouter.DirectResolver( algorithm.ref )
    val router = TestActorRef[DetectionAlgorithmRouter]( DetectionAlgorithmRouter.props( Map(algo -> resolver) ) )
    val subscriber = TestProbe()
  }


  "DetectionAlgorithmRouter" should {
    import DetectionAlgorithmRouter._
    "register algorithms" in { f: Fixture =>
      import f._
      val probe = TestProbe()
      router.receive( RegisterAlgorithmReference('foo, probe.ref), probe.ref )
      probe.expectMsgPF( hint = "register", max = 200.millis.dilated ) {
        case Envelope( AlgorithmRegistered(actual), _ ) => actual.name mustBe Symbol("foo").name
      }
    }

    "route detection messages" in { f: Fixture =>
      import f._
      router.receive( RegisterAlgorithmReference(algo, algorithm.ref) )

      val myPoints = Seq(
        DataPoint( new joda.DateTime(448), 8.46 ),
        DataPoint( new joda.DateTime(449), 8.9 ),
        DataPoint( new joda.DateTime(450), 8.58 ),
        DataPoint( new joda.DateTime(451), 8.36 ),
        DataPoint( new joda.DateTime(452), 8.58 ),
        DataPoint( new joda.DateTime(453), 7.5 ),
        DataPoint( new joda.DateTime(454), 7.1 ),
        DataPoint( new joda.DateTime(455), 7.3 ),
        DataPoint( new joda.DateTime(456), 7.71 ),
        DataPoint( new joda.DateTime(457), 8.14 ),
        DataPoint( new joda.DateTime(458), 8.14 ),
        DataPoint( new joda.DateTime(459), 7.1 ),
        DataPoint( new joda.DateTime(460), 7.5 ),
        DataPoint( new joda.DateTime(461), 7.1 ),
        DataPoint( new joda.DateTime(462), 7.1 ),
        DataPoint( new joda.DateTime(463), 7.3 ),
        DataPoint( new joda.DateTime(464), 7.71 ),
        DataPoint( new joda.DateTime(465), 8.8 ),
        DataPoint( new joda.DateTime(466), 8.9 )
      )

      val series = TimeSeries( "series", myPoints )
      val aggregator = TestProbe()
      val plan = mock[OutlierPlan]
      val msg = DetectUsing(
        'foo,
        DetectOutliersInSeries(AcceptTimeSeries(null, Set.empty[WorkId], series), plan, subscriber.ref),
        HistoricalStatistics(2, false)
      )

      implicit val sender = aggregator.ref
      router.receive( msg )
      algorithm.expectMsgPF( 2.seconds.dilated, "route" ) {
        case Envelope( actual, _ ) => actual mustBe msg
      }
    }
  }
}
