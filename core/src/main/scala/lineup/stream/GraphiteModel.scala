package lineup.stream

import java.net.InetAddress
import org.slf4j.MarkerFactory

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.stream.{ ActorMaterializer, Materializer }
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.joda.{ time => joda }
import lineup.analysis.outlier.algorithm.DBSCANAnalyzer
import lineup.analysis.outlier.{ DetectionAlgorithmRouter, OutlierDetection }
import lineup.model.outlier._
import lineup.model.timeseries.TimeSeriesBase.Merging
import lineup.model.timeseries.{Topic, DataPoint, TimeSeries, TimeSeriesBase}


/**
 * Created by rolfsd on 10/21/15.
 */
object GraphiteModel extends LazyLogging {
  def main( args: Array[String] ): Unit = {
    Usage.makeUsageConfig.parse( args, Usage.zero ) match {
      case None => System exit -1

      case Some( usage ) => {
        val config = ConfigFactory.load

        val (timeout, host, port, windowSize, dbscanEPS, dbscanDensity) = getConfiguration( usage, config )
        println(
          s"""
             |Running Lineup using the following configuation:
             |\ttimeout       : ${timeout.toCoarsest}
             |\tbinding       : ${host}:${port}
             |\twindow        : ${windowSize.toCoarsest}
             |\tDBSCAN EPS    : ${dbscanEPS}
             |\tDBSCAN Density: ${dbscanDensity}
           """.stripMargin
        )


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

        streamGraphiteOutliers( host, port, windowSize, detector ) onComplete {
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
    windowSize: FiniteDuration,
    detector: ActorRef
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Future[Unit] = {
    implicit val dispatcher = system.dispatcher

    val serverBinding: Source[Tcp.IncomingConnection, Future[Tcp.ServerBinding]] = Tcp().bind( host.getHostAddress, port )

    serverBinding runForeach { connection =>
      val detection = Flow() { implicit b =>
        import FlowGraph.Implicits._

        val framing = b.add(
          Flow[ByteString]
          .via(
            Framing.delimiter(
              delimiter = ByteString( "\n" ),
              maximumFrameLength = scala.math.pow( 2, 20 ).toInt, // from graphite documentation
              allowTruncation = true
            )
          )
          .map { _.utf8String }
        )

        val timeSeries = b.add( graphiteTimeSeries( windowSize = windowSize ) )
        val detectOutlier = b.add( OutlierDetection.detectOutlier(detector, 1.second, 4) )
        val tap = b.add( Flow[Outliers].mapAsyncUnordered(parallelism = 4){ o => Future { System.out.println( o.toString); o } } )
        val last = b.add( Flow[Outliers] map { o => ByteString(o.toString) } )

        framing ~> timeSeries ~> detectOutlier ~> tap ~> last

        ( framing.inlet, last.outlet )
      }

      connection.handleWith( detection )
    }
  }

  def graphiteTimeSeries(
    parallelism: Int = 4,
    windowSize: FiniteDuration = 1.minute
  )(
    implicit ec: ExecutionContext,
    tsMerging: Merging[TimeSeries],
    materializer: Materializer
  ): Flow[String, TimeSeries, Unit] = {
    val numTopics = 1

    Flow[String]
    .mapConcat { toDataPoints }
    .groupedWithin( n = numTopics * windowSize.toMillis.toInt, d = windowSize )// max elems = 1 per milli; duration = windowSize
    .map {
      _.groupBy{ _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
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


  def status[T]( label: String ): Flow[T, T, Unit] = Flow[T].map { e => logger info s"\n$label:${e.toString}"; e }


  def getConfiguration( usage: Usage, config: Config ): (FiniteDuration, InetAddress, Int, FiniteDuration, Double, Int) = {
    val timeout: FiniteDuration = usage.timeout getOrElse {
      if ( config hasPath Usage.DETECTION_TIMEOUT ) {
        FiniteDuration( config.getDuration(Usage.DETECTION_TIMEOUT).toNanos, NANOSECONDS )
      } else {
        500.millis
      }
    }

    val host = usage.sourceHost getOrElse {
      if ( config hasPath Usage.SOURCE_HOST ) InetAddress getByName config.getString( Usage.SOURCE_HOST )
      else InetAddress.getLocalHost
    }

    val port = usage.sourcePort getOrElse { config getInt Usage.SOURCE_PORT }

    val windowSize = usage.windowSize getOrElse {
      if ( config hasPath Usage.SOURCE_WINDOW_SIZE ) {
        FiniteDuration( config.getDuration(Usage.SOURCE_WINDOW_SIZE).toNanos, NANOSECONDS )
      } else {
        1.minute
      }
    }

    val dbscanEPS = usage.dbscanEps getOrElse config.getDouble( Usage.DBSCAN_EPS )
    val dbscanDensity = usage.dbscanMinDensityConnectedPoints getOrElse {
      config.getInt( Usage.DBSCAN_MIN_DENSITY_CONNECTED_POINTS )
    }

    ( timeout, host, port, windowSize, dbscanEPS, dbscanDensity )
  }

  case class Usage(
    timeout: Option[FiniteDuration] = None,
    sourceHost: Option[InetAddress] = None,
    sourcePort: Option[Int] = None,
    windowSize: Option[FiniteDuration] = None,
    dbscanEps: Option[Double] = None, //todo: change to tolerance value independent of axis scaling effects
    dbscanMinDensityConnectedPoints: Option[Int] = None
  )

  object Usage {
    val DETECTION_TIMEOUT = "lineup.timeout"
    val SOURCE_HOST = "lineup.source.host"
    val SOURCE_PORT = "lineup.source.port"
    val SOURCE_WINDOW_SIZE = "lineup.source.windowSize"
    val DBSCAN_EPS = "lineup.dbscan.eps"
    val DBSCAN_MIN_DENSITY_CONNECTED_POINTS = "lineup.dbscan.minDensityConnectedPoints"

    def zero: Usage = Usage()

    def makeUsageConfig = new scopt.OptionParser[Usage]( "lineup" ) {
      //todo remove once release is current with corresponding dev
      implicit val inetAddressRead: scopt.Read[InetAddress] = scopt.Read.reads { InetAddress.getByName(_) }

      head( "lineup", "0.1.a" )

      opt[Long]( 't', "timeout" ) action { (e, c) =>
        c.copy( timeout = Some(FiniteDuration(e, MILLISECONDS)) )
      } text( "timeout budget (in milliseconds) for outlier detection. Default is 500ms." )

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

      opt[Long]( 'w', "window" ) action { (e, c) =>
        c.copy( windowSize = Some(FiniteDuration(e, SECONDS)) )
      } text( "batch window size (in seconds) for collecting time series data. Default = 60s." )

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
