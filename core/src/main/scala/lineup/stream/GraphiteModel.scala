package lineup.stream

import java.net.InetAddress

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.io.Framing
import akka.stream.scaladsl.{ Flow, FlowGraph, Source, Tcp }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.joda.{ time => joda }
import lineup.analysis.outlier.algorithm.DBSCANAnalyzer
import lineup.analysis.outlier.{ DetectionAlgorithmRouter, OutlierDetection }
import lineup.model.outlier._
import lineup.model.timeseries.TimeSeriesBase.Merging
import lineup.model.timeseries.{ DataPoint, TimeSeries, TimeSeriesBase }


/**
 * Created by rolfsd on 10/21/15.
 */
object GraphiteModel {
  def main( args: Array[String] ): Unit = {
    Usage.makeUsageConfig.parse( args, Usage.zero ) match {
      case None => System exit -1

      case Some( usage ) => {
        val config = ConfigFactory.load

        val timeout: FiniteDuration = usage.timeout getOrElse {
          if ( config hasPath Usage.DETECTION_TIMEOUT ) {
            FiniteDuration( config.getDuration(Usage.DETECTION_TIMEOUT).toNanos, NANOSECONDS )
          } else {
            FiniteDuration( 500, MILLISECONDS )
          }
        }

        val host = usage.sourceHost getOrElse {
          if ( config hasPath Usage.SOURCE_HOST ) InetAddress getByName config.getString( Usage.SOURCE_HOST )
          else InetAddress.getLocalHost
        }

        val port = usage.sourcePort getOrElse { config getInt Usage.SOURCE_PORT }

        val dbscanEPS = usage.dbscanEps getOrElse config.getDouble( Usage.DBSCAN_EPS )
        val dbscanDensity = usage.dbscanMinDensityConnectedPoints getOrElse {
          config.getInt( Usage.DBSCAN_MIN_DENSITY_CONNECTED_POINTS )
        }

//        println( s"\nUsage=$usage" )
//        println( s"""\nConfig=${if (config.hasPath("lineup")) config.getConfig("lineup") else "<none>"}""" )
//        println( s"\ntimeout=${timeout.toCoarsest}")
//        println( s"\nhost=$host")
//        println( s"\nport=$port")
//        println( s"\neps=$dbscanEPS")
//        println( s"\ndensity=$dbscanDensity")

        implicit val system = ActorSystem( "Monitor" )
        implicit val materializer: Materializer = ActorMaterializer()
        implicit val ec = system.dispatcher

        val router = system.actorOf( DetectionAlgorithmRouter.props, "router" )
        val dbscan = system.actorOf( DBSCANAnalyzer.props( router ), "dbscan" )

        val plan = OutlierPlan(
          name = "default-plan",
          algorithms = Set( DBSCANAnalyzer.algorithm ),
          timeout = timeout,
          isQuorum = IsQuorum.AtLeastQuorumSpecification( 1, 1 ),
          reduce = demoReduce,
          algorithmProperties = Map(
            DBSCANAnalyzer.EPS -> dbscanEPS,
            DBSCANAnalyzer.MIN_DENSITY_CONNECTED_POINTS -> dbscanDensity
          )
        )

        val detector = system.actorOf( OutlierDetection.props(router, plans = Map(), default = Some(plan)) )

        streamGraphiteOutliers( host, port, detector ) onComplete {
          case Success(_) => system.terminate()
          case Failure( e ) => {
            println( "Failure: " + e.getMessage )
            system.terminate()
          }
        }
      }
    }
  }


  def streamGraphiteOutliers(
    host: InetAddress,
    port: Int,
    detector: ActorRef
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Future[Unit] = {
    val serverBinding: Source[Tcp.IncomingConnection, Future[Tcp.ServerBinding]] = Tcp().bind( host.getHostAddress, port )

    serverBinding runForeach { connection =>
      val detection = Flow() { implicit b =>
        import FlowGraph.Implicits._

        val framing = b.add(
          Flow[ByteString]
          .via(
            Framing.delimiter(
              delimiter = ByteString( "]" ),
              maximumFrameLength = scala.math.pow( 2, 20 ).toInt
            )
          )
          .map { _.utf8String }
        )

        val timeSeries = b.add( graphiteTimeSeries() )
        val detectOutlier = OutlierDetection.detectOutlier( detector, 1.second, 4 )

        val print = b.add( Flow[Outliers] map { o => println( s"outlier:${o}" ); ByteString(o.toString) } )

        framing ~> timeSeries ~> detectOutlier ~> print

        (framing.inlet, print.outlet)
      }

      //      val seriesWindow = SlidingWindow[TimeSeries]( 1.minute )
      //      val cohortWindow = SlidingWindow[TimeSeriesCohort]( 1.minute )

      connection.handleWith( detection )
    }
  }

  def graphiteTimeSeries(
    parallelism: Int = 4,
    windowSize: FiniteDuration = 1.minute
  )(
    implicit tsMerging: Merging[TimeSeries],
    materializer: Materializer
  ): Flow[String, TimeSeries, Unit] = {
    Flow[String]
    .mapConcat { toDataPoints }
    .groupBy { _.topic }
    .map {
      case (topic, seriesSource) => {
        seriesSource
        .transform { () => SlidingWindow( windowSize ) }
        .mapConcat { identity }
        .runFold( tsMerging.zero(topic) ) { (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapAsync( parallelism ) { identity }
  }


  def toDataPoints( pickles: String ): List[TimeSeries] = {
    val pickle = """\s*\(\s*([^(),]+)\s*,\s*\(\s*([^(),]+)\s*,\s*([^(),]+)\s*\)\s*\)\s*,?""".r

    pickle
    .findAllMatchIn( pickles )
    .toIndexedSeq
    .map { p => ( p.group(1), DataPoint( timestamp = new joda.DateTime(p.group(2).toLong), value = p.group(3).toDouble ) ) }
    .groupBy { _._1 }
    .map { np => TimeSeries( topic = np._1, points = np._2 map { _._2 } ) }
    .toList
  }


  val demoReduce = new ReduceOutliers {
    override def apply(results: SeriesOutlierResults, source: TimeSeriesBase): Outliers = {
      results.headOption map { _._2 } getOrElse NoOutliers( algorithms = Set(DBSCANAnalyzer.algorithm), source = source )
    }
  }


  case class Usage(
    timeout: Option[FiniteDuration] = None,
    sourceHost: Option[InetAddress] = None,
    sourcePort: Option[Int] = None,
    dbscanEps: Option[Double] = None,
    dbscanMinDensityConnectedPoints: Option[Int] = None
  )

  object Usage {
    val DETECTION_TIMEOUT = "lineup.timeout"
    val SOURCE_HOST = "lineup.source.host"
    val SOURCE_PORT = "lineup.source.port"
    val DBSCAN_EPS = "lineup.dbscan.eps"
    val DBSCAN_MIN_DENSITY_CONNECTED_POINTS = "lineup.dbscan.minDensityConnectedPoints"

    def zero: Usage = Usage()

    def makeUsageConfig = new scopt.OptionParser[Usage]( "lineup" ) {
      //todo remove once release is current with corresponding dev
      implicit val inetAddressRead: scopt.Read[InetAddress] = scopt.Read.reads { InetAddress.getByName(_) }

      head( "lineup", "0.1.a" )

      opt[Long]( 't', "timeout" ) action { (e, c) =>
        c.copy( timeout = Some(FiniteDuration(e, MILLISECONDS)) )
      } text( "timeout budget (in milliseconds) for outlier detection" )

      opt[Double]( 'e', "eps" ) action { (e, c) =>
        c.copy( dbscanEps = Some(e) )
      } text( "dbscan eps parameter" )

      opt[Int]( 'd', "density" ) action { (e, c) =>
        c.copy( dbscanMinDensityConnectedPoints = Some(e) )
      } text( "dbscan minimum density for conntected points" )

      opt[InetAddress]( 'h', "host" ) action { (e, c) =>
        c.copy( sourceHost = Some(e) )
      } text( "connection address to source" )

      opt[Int]( 'p', "port" ) action { (e, c) =>
        c.copy( sourcePort = Some(e) )
      } text( "connection port of source server")

      help( "help" )

      note(
        """
          |DBSCAN eps: The value for ε can then be chosen by using a k-distance graph, plotting the distance to the k = minPts
          |nearest neighbor. Good values of ε are where this plot shows a strong bend: if ε is chosen much too small, a large
          |part of the data will not be clustered; whereas for a too high value of ε, clusters will merge and the majority of
          |objects will be in the same cluster. In general, small values of ε are preferable, and as a rule of thumb only a small
          |fraction of points should be within this distance of each other.
          |
          |DBSCAN density: As a rule of thumb, a minimum minPts can be derived from the number of dimensions D in the data set,
          |as minPts ≥ D + 1. The low value of minPts = 1 does not make sense, as then every point on its own will already be a
          |cluster. With minPts ≤ 2, the result will be the same as of hierarchical clustering with the single link metric, with
          |the dendrogram cut at height ε. Therefore, minPts must be chosen at least 3. However, larger values are usually better
          |for data sets with noise and will yield more significant clusters. The larger the data set, the larger the value of
          |minPts should be chosen.
        """.stripMargin
      )
    }
  }
}
