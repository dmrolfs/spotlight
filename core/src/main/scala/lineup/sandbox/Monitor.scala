package lineup.sandbox

import java.awt.image.ComponentSampleModel

import lineup.analysis.outlier.algorithm.DBSCANAnalyzer
import lineup.analysis.outlier.{DetectionAlgorithmRouter, OutlierDetection}
import lineup.model.outlier._

import scala.concurrent.duration._
import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.stream.io.Framing
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._
import akka.util.ByteString
import org.joda.{ time => joda }
import lineup.model.timeseries._


/**
 * Created by rolfsd on 10/21/15.
 */
object Monitor {
  def main( args: Array[String] ): Unit = {
    implicit val system = ActorSystem( "Monitor" )
    implicit val materializer = ActorMaterializer

    val router = system.actorOf( DetectionAlgorithmRouter.props, "router" )
    val dbscan = system.actorOf( DBSCANAnalyzer.props( router ), "dbscan" )

    val plan = OutlierPlan(
      name = "default-plan",
      algorithms = Set( DBSCANAnalyzer.algorithm ),
      timeout = 1.second,
      isQuorum = IsQuorum.AtLeastQuorumSpecification( 1, 1 ),
      reduce = WORK HERE
                          )
    val detector = system.actorOf( OutlierDetection.props( router, ))

    val host = ???
    val port = ???

    val flowBufferSource: Flow[String, String, Unit] = Flow[String].buffer( 1000, OverflowStrategy.backpressure )

    val sinkPrintingOutElements: Sink[String, Future[Unit]]  = Sink.foreach[String]{ println }

    val flowUnmarshalPickle: Flow[String, TimeSeries, Unit] = Flow[String] mapConcat { toDataPoints }


    val extractTopic: (TimeSeriesBase) => Topic = (tsb: TimeSeriesBase) => tsb.topic
    val flowSeriesStreams: Flow[String, (Topic, Source[TimeSeries, Unit]), Unit] = flowUnmarshalPickle groupBy { extractTopic(_) }

    val connections: Source[Tcp.IncomingConnection, Future[Tcp.ServerBinding]] = Tcp().bind( interface = host, port = port )


    connections runForeach { connection =>
      val detection = Flow() { implicit b =>
        import FlowGraph.Implicits._

        val src = b.add(
          Flow[ByteString]
          .via(
            Framing.delimiter(
              delimiter = ByteString("]"),
              maximumFrameLength = scala.math.pow( 2, 20 ).toInt,
              allowTruncation = true
            )
          )
          .map { _.utf8String }
        )

        val window = b.add( new SlidingWindow(1.minute) )
        val detectOutlier = OutlierDetection.detectOutlier( )
        src ~> flowSeriesStreams
        (src.inlet, )
      }

      val seriesWindow = SlidingWindow[TimeSeries]( 1.minute )
      val cohortWindow = SlidingWindow[TimeSeriesCohort]( 1.minute )

//                    .via outierDetection

      connection handleWith outlier
    }
  }


  def toDataPoints( pickles: String ): List[TimeSeries] = {
    val pickle = """\s*\(\s*([^(),]+)\s*,\s*\(\s*([^(),]+)\s*,\s*([^(),]+)\s*\)\s*\)\s*,?""".r

    pickle
    .findAllMatchIn( pickles )
    .toIndexedSeq
    .map { p =>
      ( p.group(1), DataPoint( timestamp = new joda.DateTime(p.group(2).toLong), value = p.group(3).toDouble ) )
    }
    .groupBy { _._1 }
    .map { np => TimeSeries( topic = np._1, points = np._2 map { _._2 } ) }
    .toList
  }


  val demoReduce = new ReduceOutliers {
    override def apply( results: SeriesOutlierResults ): Outliers = results.headOption match {
      case None => NoOutliers( algorithms = Set(DBSCANAnalyzer.algorithm), source = )
    }
  }
}
