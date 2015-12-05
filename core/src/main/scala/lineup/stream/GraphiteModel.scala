package lineup.stream

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.matching.Regex
import scala.util.{ Failure, Success }

import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.scaladsl._
import akka.stream._
import akka.util.ByteString

import com.typesafe.config.{ ConfigObject, Config, ConfigFactory }
import com.typesafe.scalalogging.{ StrictLogging, Logger }
import org.slf4j.LoggerFactory
import org.apache.commons.math3.linear.MatrixUtils
import peds.commons.math.MahalanobisDistance
import peds.commons.log.Trace

import lineup.analysis.outlier.algorithm.DBSCANAnalyzer
import lineup.analysis.outlier.{ DetectionAlgorithmRouter, OutlierDetection }
import lineup.model.outlier._
import lineup.model.timeseries.TimeSeriesBase.Merging
import lineup.model.timeseries._


/**
 * Created by rolfsd on 10/21/15.
 */
object GraphiteModel extends StrictLogging {

  override protected val logger: Logger = Logger( LoggerFactory.getLogger("Graphite") )

  val trace = Trace[GraphiteModel.type]

  def main( args: Array[String] ): Unit = {
    Settings.makeUsageConfig.parse( args, Settings.zero ) match {
      case None => System exit -1

      case Some( usage ) => {
        val config = ConfigFactory.load

        val (host, port, maxFrameLength, protocol, windowSize, plans) = getConfiguration( usage, config )
        val usageMessage = s"""
          |\nRunning Lineup using the following configuration:
          |\tbinding       : ${host}:${port}
          |\tmax frame size: ${maxFrameLength}
          |\tprotocol      : ${protocol}
          |\twindow        : ${windowSize.toCoarsest}
          |\tplans         : [${plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]
        """.stripMargin

        logger info usageMessage

        implicit val system = ActorSystem( "Graphite" )
        implicit val materializer = ActorMaterializer( ActorMaterializerSettings(system).withSupervisionStrategy(decider) )
        implicit val ec = system.dispatcher

        val router = system.actorOf( DetectionAlgorithmRouter.props, "router" )
        val dbscan = system.actorOf( DBSCANAnalyzer.props( router ), "dbscan" )

        val detector = system.actorOf( OutlierDetection.props(router, plans), "outlierDetector" )

        streamGraphiteOutliers( host, port, maxFrameLength, protocol, windowSize, detector, plans ) onComplete {
          case Success(_) => system.terminate()
          case Failure( e ) => {
            logger.error( "Failure:", e )
            system.terminate()
          }
        }
      }
    }
  }

  val decider: Supervision.Decider = {
    case ex => {
      logger.error( "Error caught by Supervisor:", ex )
      Supervision.Stop
    }
  }

  def streamGraphiteOutliers(
    host: InetAddress,
    port: Int,
    maxFrameLength: Int,
    protocol: GraphiteSerializationProtocol,
    windowSize: FiniteDuration,
    detector: ActorRef,
    plans: Seq[OutlierPlan]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Future[Unit] = {
    implicit val dispatcher = system.dispatcher

    val serverBinding: Source[Tcp.IncomingConnection, Future[Tcp.ServerBinding]] = Tcp().bind( host.getHostAddress, port )

    serverBinding runForeach { connection =>
      val detection = Flow() { implicit b =>
        import FlowGraph.Implicits._

        log( logger, 'info )( s"received connection remote[${connection.remoteAddress}] -> local[${connection.localAddress}]" )

        val framing = b.add( Monitor.source('framing).watch( protocol.framingFlow( maxFrameLength ) ) )
        val timeSeries = b.add( Monitor.source('timeseries).watch( protocol.loadTimeSeriesData ) )
        val planned = b.add(
          Monitor.flow('planned).watch(
            Flow[TimeSeries]
            .map { e =>
              log( logger, 'debug )( s"""Plan for ${e.topic}: ${plans.find{ _ appliesTo e }.getOrElse{"NONE"}}""" )
              e
            }
            .filter { ts => plans exists { _ appliesTo ts } }
          )
        )

        val batch = Monitor.sink('batch).watch( batchSeries( windowSize ) )

        val buffer = b.add(
          Monitor.source('groups).watch(
            Flow[TimeSeries]
            .via(
              Monitor.flow('buffer).watch(
                Flow[TimeSeries].buffer(1000, OverflowStrategy.backpressure)
              )
            )
          )
        )

        val detectOutlier = Monitor.flow('detect).watch(
          OutlierDetection.detectOutlier( detector, 15.seconds, Runtime.getRuntime.availableProcessors() * 3 )
        )

        val broadcast = b.add( Broadcast[Outliers](outputPorts = 2, eagerCancel = false) )
        val tcpOut = b.add( Monitor.sink('tcpOut).watch( Flow[Outliers] map { _ => ByteString() } ) )
        val stats = Monitor.sink('stats).watch( recordOutlierStats )
        val term = b.add( Sink.ignore )

        Monitor.set( 'framing, 'timeseries, 'planned, 'batch, 'groups, 'buffer, 'detect, 'tcpOut, 'stats )

        framing ~> timeSeries ~> planned ~> batch ~> buffer ~> detectOutlier ~> broadcast ~> tcpOut
                                                                                broadcast ~> stats ~> term

        ( framing.inlet, tcpOut.outlet )
      }

      connection.handleWith( detection )
    }
  }

  /**
    * Limit downstream rate to one element every 'interval' by applying back-pressure on upstream.
 *
    * @param interval time interval to send one element downstream
    * @tparam A
    * @return
    */
  def rateLimiter[A]( interval: FiniteDuration ): Flow[A, A, Unit] = {
    case object Tick

    val flow = Flow() { implicit builder =>
      import FlowGraph.Implicits._

      val rateLimiter = Source.apply( 0.second, interval, Tick )

      val zip = builder add Zip[A, Tick.type]()

      rateLimiter ~> zip.in1

      ( zip.in0, zip.out.map(_._1).outlet )
    }

    // We need to limit input buffer to 1 to guarantee the rate limiting feature
    flow.withAttributes( Attributes.inputBuffer(initial = 1, max = 64) )
  }


  def recordOutlierStats(implicit system: ActorSystem ): Flow[Outliers, Outliers, Unit] = {
    import org.apache.commons.math3.stat.descriptive._

    val outlierLogger = Logger( LoggerFactory getLogger "Outliers" )
    val elementsSeen = new AtomicInteger()

    Flow[Outliers].
    map { e => (elementsSeen.incrementAndGet(), e) }
    .map { case (i, o) =>
      val (stats, euclidean, mahalanobis, eucOutliers, mahalOutliers) = o.source match {
        case s: TimeSeries => {
          val valueStats = new DescriptiveStatistics
          s.points foreach { valueStats addValue _.value }

          val data = s.points map { p => Array(p.timestamp.getMillis.toDouble, p.value) }

          val euclideanDistance = new org.apache.commons.math3.ml.distance.EuclideanDistance
          val mahalanobisDistance = MahalanobisDistance( MatrixUtils.createRealMatrix(data.toArray) )
          val edStats = new DescriptiveStatistics//acc euclidean stats
          val mdStats = new DescriptiveStatistics //acc mahalanobis stats

          val distances = data.take(data.size - 1).zip(data.drop(1)) map { case (l, r) =>
            val e = euclideanDistance.compute( l, r )
            val m = mahalanobisDistance.compute( l, r )
            edStats addValue e
            mdStats addValue m
            (e, m)
          }

          val (ed, md) = distances.unzip
          val edOutliers = ed count { _ > edStats.getPercentile(95) }
          val mdOutliers = md count { _ > mdStats.getPercentile(95) }
          ( valueStats, edStats, mdStats, edOutliers, mdOutliers )
        }

        case c: TimeSeriesCohort => {
          val st = new DescriptiveStatistics
          for {
            s <- c.data
            p <- s.points
          } { st addValue p.value }

          ( st, new DescriptiveStatistics, new DescriptiveStatistics, 0, 0 )
        }
      }

      log( outlierLogger, 'info ){
        s"""
           |${i}:\t${o.toString}
           |${i}:\tsource:[${o.source.toString}]
           |${i}:\tValue ${stats.toString}
           |${i}:\tEuclidean ${euclidean.toString}
           |${i}:\tEuclidean Percentiles: 95th:[${euclidean.getPercentile(95)}] 99th:[${euclidean.getPercentile(99)}]
           |${i}:\tEuclidean Outliers >95%:[${eucOutliers}]
           |${i}:\tMahalanobis ${mahalanobis.toString}
           |${i}:\tMahalanobis Percentiles: 95th:[${mahalanobis.getPercentile(95)}] 99th:[${mahalanobis.getPercentile(99)}]
           |${i}:\tMahalanobis Outliers >95%:[${mahalOutliers}]
       """.stripMargin
      }

      o
    }
  }

  def batchSeries(
    windowSize: FiniteDuration = 1.minute,
    parallelism: Int = 4
  )(
    implicit system: ActorSystem,
    tsMerging: Merging[TimeSeries],
    materializer: Materializer
  ): Flow[TimeSeries, TimeSeries, Unit] = {
    val numTopics = 1
//    val metricsLogger = Logger( LoggerFactory getLogger "Metrics" )
//    val metricsSeen = new AtomicInteger()

    Flow[TimeSeries]
//    .map { e =>
//      log( metricsLogger, 'info ){ s"${metricsSeen.incrementAndGet()}:${e.toString}" }
//      e
//    }
    .groupedWithin( n = numTopics * windowSize.toMillis.toInt, d = windowSize ) // max elems = 1 per milli; duration = windowSize
    .map {
      _.groupBy{ _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
  }


  val demoReduce = new ReduceOutliers {
    override def apply(
      results: SeriesOutlierResults,
      source: TimeSeriesBase
    )(
      implicit ec: ExecutionContext
    ): Future[Outliers] = {
      Future {
        results.headOption map { _._2 } getOrElse { NoOutliers( algorithms = Set(DBSCANAnalyzer.Algorithm ), source = source ) }
      }
    }
  }


  def loggerDispatcher( system: ActorSystem ): ExecutionContext = system.dispatchers lookup "logger-dispatcher"

  def log( logr: Logger, level: Symbol )( msg: => String )( implicit system: ActorSystem ): Future[Unit] = {
    Future {
      level match {
        case 'debug => logr debug msg
        case 'info => logr info msg
        case 'warn => logr warn msg
        case 'error => logr error msg
        case _ => logr error msg
      }
    }( loggerDispatcher(system) )
  }


  def getConfiguration(
    usage: Settings,
    config: Config
  ): (InetAddress, Int, Int, GraphiteSerializationProtocol, FiniteDuration, Seq[OutlierPlan]) = {
    val host = usage.sourceHost getOrElse {
      if ( config hasPath Settings.SOURCE_HOST ) InetAddress getByName config.getString( Settings.SOURCE_HOST )
      else InetAddress.getLocalHost
    }

    val port = usage.sourcePort getOrElse { config getInt Settings.SOURCE_PORT }

    val maxFrameLength = {
      if ( config hasPath Settings.SOURCE_MAX_FRAME_LENGTH) config getInt Settings.SOURCE_MAX_FRAME_LENGTH
      else 4 + scala.math.pow( 2, 20 ).toInt // from graphite documentation
    }

    val protocol = {
      if ( config hasPath Settings.SOURCE_PROTOCOL ) {
        config.getString(Settings.SOURCE_PROTOCOL).toLowerCase match {
          case "messagepack" | "message-pack" => MessagePackProtocol
          case "pickle" => PythonPickleProtocol
          case _ => PythonPickleProtocol
        }
      } else {
        PythonPickleProtocol
      }
    }

    val windowSize = usage.windowSize getOrElse {
      if ( config hasPath Settings.SOURCE_WINDOW_SIZE ) {
        FiniteDuration( config.getDuration( Settings.SOURCE_WINDOW_SIZE ).toNanos, NANOSECONDS )
      } else {
        1.minute
      }
    }

    val plans = makePlans( config.getConfig( "lineup.detection-plans" ))

    ( host, port, maxFrameLength, protocol, windowSize, plans )
  }

  private def makePlans( planSpecifications: Config ): Seq[OutlierPlan] = {
    import scala.collection.JavaConversions._

    val result = planSpecifications.root.collect{ case (n, s: ConfigObject) => (n, s.toConfig) }.toSeq.map {
      case (name, spec) => {
        val IS_DEFAULT = "is-default"
        val TOPICS = "topics"
        val REGEX = "regex"

        val ( timeout, algorithms ) = pullCommonPlanFacets( spec )

        if ( spec.hasPath( IS_DEFAULT ) && spec.getBoolean( IS_DEFAULT ) ) {
          Some(
            OutlierPlan.default(
              name = name,
              timeout = timeout,
              isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = algorithms.size, triggerPoint = 1 ),
              reduce = demoReduce,
              algorithms = algorithms,
              specification = spec
            )
          )
        } else if ( spec hasPath TOPICS ) {
          import scala.collection.JavaConverters._
          println( s"TOPIC [$name] SPEC Origin: ${spec.origin} LINE:${spec.origin.lineNumber}")

          Some(
            OutlierPlan.forTopics(
              name = name,
              timeout = timeout,
              isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = algorithms.size, triggerPoint = 1 ),
              reduce = demoReduce,
              algorithms = algorithms,
              specification = spec,
              extractTopic = OutlierDetection.extractOutlierDetectionTopic,
              topics = spec.getStringList(TOPICS).asScala.map{ Topic(_) }.toSet
            )
          )
        } else if ( spec hasPath REGEX ) {
          Some(
            OutlierPlan.forRegex(
              name = name,
              timeout = timeout,
              isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = algorithms.size, triggerPoint = 1 ),
              reduce = demoReduce,
              algorithms = algorithms,
              specification = spec,
              extractTopic = OutlierDetection.extractOutlierDetectionTopic,
              regex = new Regex( spec.getString(REGEX) )
            )
          )
        } else {
          None
        }
      }
    }

    result.flatten.sorted
  }


  private def pullCommonPlanFacets( spec: Config ): (FiniteDuration, Set[Symbol]) = {
    import scala.collection.JavaConversions._

    (
      FiniteDuration( spec.getDuration("timeout").toNanos, NANOSECONDS ),
      spec.getStringList("algorithms").toSet.map{ a: String => Symbol(a) }
    )
  }


  case class Settings(
    sourceHost: Option[InetAddress] = None,
    sourcePort: Option[Int] = None,
    windowSize: Option[FiniteDuration] = None,
    plans: Seq[OutlierPlan] = Seq.empty[OutlierPlan]
  )

  object Settings {
    val SOURCE_HOST = "lineup.source.host"
    val SOURCE_PORT = "lineup.source.port"
    val SOURCE_MAX_FRAME_LENGTH = "lineup.source.max-frame-length"
    val SOURCE_PROTOCOL = "lineup.source.protocol"
    val SOURCE_WINDOW_SIZE = "lineup.source.window-size"

    def zero: Settings = Settings( )

    def makeUsageConfig = new scopt.OptionParser[Settings]( "lineup" ) {
      //todo remove once release is current with corresponding dev
      implicit val inetAddressRead: scopt.Read[InetAddress] = scopt.Read.reads { InetAddress.getByName(_) }

      head( "lineup", "0.1.a" )

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
