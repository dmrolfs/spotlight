package lineup.stream

import java.net.{ InetSocketAddress, InetAddress}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.matching.Regex
import scala.util.{ Try, Failure, Success }
import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.scaladsl._
import akka.stream._
import akka.util.ByteString
import akka.stream.stage.{ SyncDirective, Context, PushStage }

import better.files.{ ManagedResource => _, _ }
import com.typesafe.config._
import com.typesafe.scalalogging.{ StrictLogging, Logger }
import org.slf4j.LoggerFactory
import nl.grons.metrics.scala.MetricName
import peds.akka.metrics.{ StreamMonitor, Reporter, Instrumented }
import peds.commons.log.Trace
import peds.commons.collection.BloomFilter
import peds.akka.stream.Limiter
import lineup.analysis.outlier.algorithm.DBSCANAnalyzer
import lineup.analysis.outlier.{ DetectionAlgorithmRouter, OutlierDetection }
import lineup.model.outlier._
import lineup.model.timeseries.TimeSeriesBase.Merging
import lineup.model.timeseries._

/**
 * Created by rolfsd on 10/21/15.
 */
object GraphiteModel extends Instrumented with StrictLogging {

  val OutlierMetricPrefix = "lineup.outlier."

  override lazy val metricBaseName: MetricName = MetricName( "" )

  override protected val logger: Logger = Logger( LoggerFactory.getLogger("Graphite") )

  val trace = Trace[GraphiteModel.type]

  case class UsageConfiguration(
    sourceHostPort: (InetAddress, Int),
    maxFrameLength: Int,
    protocol: GraphiteSerializationProtocol,
    windowDuration: FiniteDuration,
    graphiteHostPort: Option[(InetAddress, Int)]
  ) {
    def serverBinding( implicit system: ActorSystem ): Source[Tcp.IncomingConnection, Future[Tcp.ServerBinding]] = {
      val (host, port) = sourceHostPort
      Tcp().bind( host.getHostAddress, port )
    }
  }

  def main( args: Array[String] ): Unit = {
    Settings.makeUsageConfig.parse( args, Settings.zero ) match {
      case None => System exit -1

      case Some( usage ) => {
        val config = ConfigFactory.load
        val usageConfig = getConfiguration( usage, config )

        val detectTimeout = FiniteDuration( config.getDuration("lineup.workflow.detect.timeout", MILLISECONDS), MILLISECONDS )
        val PlanConfigPath = "lineup.detection-plans"
        val plansFn: () => Try[Seq[OutlierPlan]] = () => Try {
          ConfigFactory.invalidateCaches()
          makePlans( detectTimeout, ConfigFactory.load.getConfig(PlanConfigPath) )
        }

        plansFn() foreach { plans =>
          val usageMessage = s"""
            |\nRunning Lineup using the following configuration:
            |\tsource binding  : ${usageConfig.sourceHostPort._1}:${usageConfig.sourceHostPort._2}
            |\tpublish binding : ${usageConfig.graphiteHostPort map { case (h,p) => h.toString + ":" + p.toString }}
            |\tmax frame size  : ${usageConfig.maxFrameLength}
            |\tprotocol        : ${usageConfig.protocol}
            |\twindow          : ${usageConfig.windowDuration.toCoarsest}
            |\tAvail Processors: ${Runtime.getRuntime.availableProcessors}
            |\tplans           : [${plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]
          """.stripMargin

          logger info usageMessage
        }

        val decider: Supervision.Decider = {
          case ex => {
            logger.error( "Error caught by Supervisor:", ex )
            Supervision.Stop
          }
        }

        if ( config hasPath "lineup.metrics" ) {
          logger info s"""starting metric reporting with config: [${config getConfig "lineup.metrics"}]"""
          val reporter = Reporter.startReporter( config getConfig "lineup.metrics" )
          logger info s"metric reporter: [${reporter}]"
        } else {
          logger warn """metric report configuration missing at "lineup.metrics""""
        }

        implicit val system = ActorSystem( "Graphite" )
        implicit val materializer = ActorMaterializer( ActorMaterializerSettings(system).withSupervisionStrategy(decider) )
        implicit val ec = system.dispatcher

        import java.nio.file.{ StandardWatchEventKinds => Events }
        import better.files.FileWatcher._

        val limiter = system.actorOf( Limiter.props(100, 1.second, 10), "limiter" )

        val publisher = usageConfig.graphiteHostPort map { case (host, port) =>
          system.actorOf( GraphitePublisher.props( new InetSocketAddress(host, port) ), "graphite-publisher" )
        } getOrElse {
          system.actorOf( LogPublisher.props, "log-publisher" )
        }

        val router = system.actorOf( DetectionAlgorithmRouter.props, "router" )
        val dbscan = system.actorOf( DBSCANAnalyzer.props( router ), "dbscan" )

        val detector = system.actorOf(
          props = OutlierDetection.props(router ){ r =>
            new OutlierDetection(r) with OutlierDetection.ConfigPlanPolicy {
              override lazy val metricBaseName = MetricName( classOf[OutlierDetection] )
              override def getPlans: () => Try[Seq[OutlierPlan]] = plansFn
              override def refreshInterval: FiniteDuration = 15.minutes
            }
          },
          name = "outlierDetector"
        )

        val configOrigin = config.getConfig( PlanConfigPath ).origin
        logger info s"origin for ${PlanConfigPath}: [${configOrigin}]"

        determineConfigFileComponents( configOrigin ) foreach { filename =>
          // note: attempting to watch a shared file wrt VirtualBox will not work (https://www.virtualbox.org/ticket/9069)
          // so dev testing of watching should be done by running the Java locally
          logger info s"watching for changes in ${filename}"
          val configWatcher = File( filename ).newWatcher( true )
          configWatcher ! on( Events.ENTRY_MODIFY ) {
            case _ => {
              logger info s"config file watcher sending reload command due to change in ${configOrigin.description}"
              detector ! OutlierDetection.ReloadPlans
            }
          }
        }

        streamGraphiteOutliers( usageConfig, detector, limiter, publisher, PlanConfigPath ) onComplete {
          case Success(_) => system.terminate()
          case Failure( e ) => {
            logger.error( "Failure:", e )
            system.terminate()
          }
        }
      }
    }
  }

  def determineConfigFileComponents(origin: ConfigOrigin ): List[String] = {
    val Path = "@\\s+file:(.*):\\s+\\d+,".r
    Path.findAllMatchIn( origin.toString ).map{ _ group 1 }.toList
  }


  def streamGraphiteOutliers(
    usageConfig: UsageConfiguration,
    detector: ActorRef,
    limiter: ActorRef,
    publisher: ActorRef,
    planConfigPath: String
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Future[Unit] = {
    implicit val dispatcher = system.dispatcher

    usageConfig.serverBinding runForeach { connection =>
      connection.handleWith( detectionWorkflow(detector, limiter, publisher, usageConfig, planConfigPath) )
    }
  }

  def detectionWorkflow(
    detector: ActorRef,
    limiter: ActorRef,
    publisher: ActorRef,
    usageConfig: UsageConfiguration,
    planConfigPath: String
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[ByteString, ByteString, Unit] = {
    ConfigFactory.invalidateCaches()
    val config = ConfigFactory.load
    val tcpInBufferSize = config.getInt( "lineup.source.buffer" )
    val workflowBufferSize = config.getInt( "lineup.workflow.buffer" )
    val detectTimeout = FiniteDuration( config.getDuration("lineup.workflow.detect.timeout", MILLISECONDS), MILLISECONDS )
    val plans = makePlans( detectTimeout, config.getConfig(planConfigPath) )
    log( logger, 'info ){
      s"""
         |\nConnection made using the following configuration:
         |\tTCP-In Buffer Size   : ${tcpInBufferSize}
         |\tWorkflow Buffer Size : ${workflowBufferSize}
         |\tDetect Timeout       : ${detectTimeout.toCoarsest}
         |\tplans                : [${plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]
       """.stripMargin
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val framing = b.add( StreamMonitor.source('framing).watch(usageConfig.protocol.framingFlow(usageConfig.maxFrameLength)) )
      val timeSeries = b.add( StreamMonitor.source('timeseries).watch(usageConfig.protocol.loadTimeSeriesData) )
      val planned = b.add( StreamMonitor.flow('planned).watch( filterPlanned(plans) ) )

      val batch = StreamMonitor.sink('batch).watch( batchSeries(usageConfig.windowDuration) )

      val buf1 = b.add(
        StreamMonitor.flow('buffer1).watch(Flow[ByteString].buffer(tcpInBufferSize, OverflowStrategy.backpressure))
      )

      val buf2 = b.add(
        StreamMonitor.source('groups).watch(
          Flow[TimeSeries]
          .via( StreamMonitor.flow('buffer2).watch( Flow[TimeSeries].buffer(workflowBufferSize, OverflowStrategy.backpressure) ) )
        )
      )

      val detectOutlier = StreamMonitor.flow('detect).watch(
        OutlierDetection.detectOutlier( detector, detectTimeout, Runtime.getRuntime.availableProcessors * 8 )
      )

      val broadcast = b.add( Broadcast[Outliers](outputPorts = 2, eagerCancel = false) )
      val publish = b.add( StreamMonitor.flow('publish).watch( publishOutliers(limiter, publisher) ) )
      val tcpOut = b.add( StreamMonitor.sink('tcpOut).watch( Flow[Outliers] map { _ => ByteString() } ) )
      val train = StreamMonitor.sink('train).watch( TrainOutlierAnalysis.feedOutlierTraining )
      val term = b.add( Sink.ignore )

      StreamMonitor.set(
        'framing,
        'buffer1,
        'timeseries,
        'planned,
        'batch,
        'groups,
        'buffer2,
        'detect,
        'publish,
        'tcpOut,
        'train
      )

      framing ~> buf1 ~> timeSeries ~> planned ~> batch ~> buf2 ~> detectOutlier ~> broadcast ~> publish ~> tcpOut
                                                                                    broadcast ~> train ~> term

      FlowShape( framing.in, tcpOut.out )
    }

    Flow.fromGraph( graph )
  }

  def filterPlanned( plans: Seq[OutlierPlan] )( implicit system: ActorSystem ): Flow[TimeSeries, TimeSeries, Unit] = {
    def logMetric: PushStage[TimeSeries, TimeSeries] = new PushStage[TimeSeries, TimeSeries] {
      val count = new AtomicInteger( 0 )
      var bloom = BloomFilter[Topic]( maxFalsePosProbability = 0.001, 500000 )
      val metricLogger = Logger( LoggerFactory getLogger "Metrics" )

      override def onPush( elem: TimeSeries, ctx: Context[TimeSeries] ): SyncDirective = {
        if ( bloom has_? elem.topic ) ctx push elem
        else {
          bloom += elem.topic
          if ( !elem.topic.name.startsWith(OutlierMetricPrefix) ) {
            log( metricLogger, 'debug ){
              s"""[${count.incrementAndGet}] Plan for ${elem.topic}: ${plans.find{ _ appliesTo elem }.getOrElse{"NONE"}}"""
            }
          }

          ctx push elem
        }
      }
    }

    Flow[TimeSeries]
    .transform( () => logMetric )
    .filter { ts => !ts.topic.name.startsWith( OutlierMetricPrefix ) && plans.exists{ _ appliesTo ts } }
  }

  def publishOutliers(
    limiter: ActorRef,
    publisher: ActorRef
  )(
    implicit system: ActorSystem
  ): Flow[Outliers, Outliers, Unit] = {
    import scala.collection.immutable

    implicit val ec = system.dispatcher

    Flow[Outliers]
    .conflate( immutable.Seq( _ ) ) { _ :+ _ }
    .mapConcat( identity )
    .via( GraphitePublisher.publish( limiter, publisher, 2, 90.seconds ) )
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

    val graph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val rateLimiter = Source.tick( 0.second, interval, Tick )
      val zip = builder add Zip[A, Tick.type]()
      rateLimiter ~> zip.in1

      FlowShape( zip.in0, zip.out.map{_._1}.outlet )
    }

    // We need to limit input buffer to 1 to guarantee the rate limiting feature
    Flow.fromGraph( graph ).withAttributes( Attributes.inputBuffer(initial = 1, max = 64) )
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

    val n = if ( numTopics * windowSize.toMicros.toInt < 0 ) { numTopics * windowSize.toMicros.toInt } else { Int.MaxValue }
    logger info s"n = [${n}] for windowSize=[${windowSize.toCoarsest}]"
    Flow[TimeSeries]
    .groupedWithin( n, d = windowSize ) // max elems = 1 per micro; duration = windowSize
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


  def getConfiguration( usage: Settings, config: Config ): UsageConfiguration = {
    val sourceHost = usage.sourceHost getOrElse {
      if ( config hasPath Settings.SOURCE_HOST ) InetAddress getByName config.getString( Settings.SOURCE_HOST )
      else InetAddress.getLocalHost
    }

    val sourcePort = usage.sourcePort getOrElse { config getInt Settings.SOURCE_PORT }

    val maxFrameLength = {
      if ( config hasPath Settings.SOURCE_MAX_FRAME_LENGTH) config getInt Settings.SOURCE_MAX_FRAME_LENGTH
      else 4 + scala.math.pow( 2, 20 ).toInt // from graphite documentation
    }

    val protocol = {
      if ( config hasPath Settings.SOURCE_PROTOCOL ) {
        config.getString(Settings.SOURCE_PROTOCOL).toLowerCase match {
          case "messagepack" | "message-pack" => MessagePackProtocol
          case "pickle" => new PythonPickleProtocol
          case _ => new PythonPickleProtocol
        }
      } else {
        new PythonPickleProtocol
      }
    }

    val windowSize = usage.windowSize getOrElse {
      if ( config hasPath Settings.SOURCE_WINDOW_SIZE ) {
        FiniteDuration( config.getDuration( Settings.SOURCE_WINDOW_SIZE ).toNanos, NANOSECONDS )
      } else {
        1.minute
      }
    }

    val graphiteHost = if ( config hasPath Settings.PUBLISH_GRAPHITE_HOST ) {
      Some( InetAddress getByName config.getString( Settings.PUBLISH_GRAPHITE_HOST ) )
    } else {
      None
    }

    val graphitePort = if ( config hasPath Settings.PUBLISH_GRAPHITE_PORT ) {
      Some( config getInt Settings.PUBLISH_GRAPHITE_PORT )
    } else {
      Some( 2004 )
    }

    UsageConfiguration(
      sourceHostPort = (sourceHost, sourcePort),
      maxFrameLength,
      protocol,
      windowSize,
      graphiteHostPort = {
        for {
          h <- graphiteHost
          p <- graphitePort
        } yield (h,p)
      }
    )
  }

  private def makePlans( detectTimeout: FiniteDuration, planSpecifications: Config ): Seq[OutlierPlan] = {
    import scala.collection.JavaConversions._

    val result = planSpecifications.root.collect{ case (n, s: ConfigObject) => (n, s.toConfig) }.toSeq.map {
      case (name, spec) => {
        val IS_DEFAULT = "is-default"
        val TOPICS = "topics"
        val REGEX = "regex"

        val ( timeout, algorithms ) = pullCommonPlanFacets( detectTimeout, spec )

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
          logger info s"topic[$name] plan origin: ${spec.origin} line:${spec.origin.lineNumber}"

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


  private def pullCommonPlanFacets( detectTimeout: FiniteDuration, spec: Config ): (FiniteDuration, Set[Symbol]) = {
    import scala.collection.JavaConversions._

    def plannedTimeout( timeoutBudget: FiniteDuration, utilization: Double ): FiniteDuration = {
      val utilized = timeoutBudget * utilization
      if ( utilized.isFinite ) utilized.asInstanceOf[FiniteDuration] else timeoutBudget
    }

    (
      plannedTimeout(detectTimeout, 0.8), // FiniteDuration( spec.getDuration("timeout").toNanos, NANOSECONDS ),
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
    val PUBLISH_GRAPHITE_HOST = "lineup.publish.graphite.host"
    val PUBLISH_GRAPHITE_PORT = "lineup.publish.graphite.port"

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
