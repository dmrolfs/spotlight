package lineup.analysis.outlier

import scala.concurrent.duration._
import akka.testkit._
import org.joda.{ time => joda }
import demesne.testkit.ParallelAkkaSpec
import org.scalatest.{ Tag, Outcome }
import org.scalatest.mock.MockitoSugar
import peds.commons.log.Trace
import lineup.model.timeseries.{ TimeSeries, DataPoint }


/**
 * Created by rolfsd on 10/20/15.
 */
class DetectionAlgorithmRouterSpec extends ParallelAkkaSpec with MockitoSugar {
  val trace = Trace[DetectionAlgorithmRouterSpec]

  class Fixture extends AkkaFixture {
    def before(): Unit = { }
    def after(): Unit = { }
    val router = TestActorRef[DetectionAlgorithmRouter]( DetectionAlgorithmRouter.props )
  }

  override def createAkkaFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = trace.block( s"withFixture($test)" ) {
    val f = createAkkaFixture()

    try {
      f.before()
      test( f )
    } finally {
      f.after()
      f.system.shutdown()
    }
  }

  case object WIP extends Tag( "wip" )

  "DetectionAlgorithmRouter" should {
    import DetectionAlgorithmRouter.{ AlgorithmRegistered, RegisterDetectionAlgorithm }
    "register algorithms" in { f: Fixture =>
      import f._
      val probe = TestProbe()
      router.receive( RegisterDetectionAlgorithm('foo, probe.ref), probe.ref )
      probe.expectMsg( AlgorithmRegistered )
    }

    "route detection messages" in { f: Fixture =>
      import f._
      val algo = TestProbe()
      router.receive( RegisterDetectionAlgorithm('foo, algo.ref) )

      val myPoints = IndexedSeq(
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
      val msg = DetectUsing('foo, aggregator.ref, DetectOutliersInSeries(series) )

      router.receive( msg )
      algo.expectMsg( 2.seconds.dilated, "route", msg )
    }
  }
}
