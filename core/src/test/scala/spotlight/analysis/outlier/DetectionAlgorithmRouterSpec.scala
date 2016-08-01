package spotlight.analysis.outlier

import scala.concurrent.duration._
import akka.testkit._
import org.joda.{time => joda}
import org.scalatest.mock.MockitoSugar
import peds.akka.envelope.Envelope
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.{DataPoint, TimeSeries}
import spotlight.testkit.ParallelAkkaSpec


/**
 * Created by rolfsd on 10/20/15.
 */
class DetectionAlgorithmRouterSpec extends ParallelAkkaSpec with MockitoSugar {

  class Fixture extends AkkaFixture {
    val algo = 'foo
    val algorithm = TestProbe()
    val router = TestActorRef[DetectionAlgorithmRouter]( DetectionAlgorithmRouter.props( Map(algo -> algorithm.ref) ) )
    val subscriber = TestProbe()
  }

  override def makeAkkaFixture(): Fixture = new Fixture

  "DetectionAlgorithmRouter" should {
    import DetectionAlgorithmRouter.{ AlgorithmRegistered, RegisterDetectionAlgorithm }
    "register algorithms" in { f: Fixture =>
      import f._
      val probe = TestProbe()
      router.receive( RegisterDetectionAlgorithm('foo, probe.ref), probe.ref )
      probe.expectMsgPF( hint = "register", max = 200.millis.dilated ) {
        case Envelope( AlgorithmRegistered(actual), _ ) => actual.name mustBe Symbol("foo").name
      }
    }

    "route detection messages" in { f: Fixture =>
      import f._
      router.receive( RegisterDetectionAlgorithm(algo, algorithm.ref) )

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
        aggregator.ref,
        DetectOutliersInSeries(series, plan, subscriber.ref),
        HistoricalStatistics(2, false)
      )

      router.receive( msg )
      algorithm.expectMsgPF( 2.seconds.dilated, "route" ) {
        case Envelope( actual, _ ) => actual mustBe msg
      }
    }
  }
}
