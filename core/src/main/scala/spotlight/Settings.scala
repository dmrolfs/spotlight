package spotlight

import java.net.{InetAddress, InetSocketAddress}

import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import peds.commons.{V, Valid}
import spotlight.analysis.OutlierDetection
import spotlight.model.outlier._
import spotlight.model.timeseries.Topic
import spotlight.protocol.{GraphiteSerializationProtocol, MessagePackProtocol, PythonPickleProtocol}

import scala.concurrent.duration._
import scala.util.matching.Regex
import scalaz.Scalaz._
import scalaz._


//todo refactor into base required settings and allow for app-specific extension
/**
  * Created by rolfsd on 1/12/16.
  */
trait Settings extends LazyLogging {
  def sourceAddress: InetSocketAddress
  def clusterPort: Int
  def maxFrameLength: Int
  def protocol: GraphiteSerializationProtocol
  def windowDuration: FiniteDuration
  def graphiteAddress: Option[InetSocketAddress]
  def detectionBudget: Duration
  def parallelismFactor: Double
  def plans: Set[OutlierPlan]
  def planOrigin: ConfigOrigin
  def tcpInboundBufferSize: Int
  def workflowBufferSize: Int
  def parallelism: Int = ( parallelismFactor * Runtime.getRuntime.availableProcessors() ).toInt
  def args: Seq[String]

  def config: Config

  def toConfig: Config = {
    val settingsConfig = {
      s"""
         | spotlight.settings {
         |   source-address: "${sourceAddress}"
         |   cluster-port: ${clusterPort}
         |   max-frame-length: ${maxFrameLength}
         |   protocol: ${protocol.getClass.getCanonicalName}
         |   window-duration: ${windowDuration}
         |   ${graphiteAddress.map{ ga => "graphite-address:\""+ga.toString+"\"" } getOrElse "" }
         |   detection-budget: ${detectionBudget}
         |   parallelism-factor: ${parallelismFactor}
         |   parallelism: ${parallelism}
         |   tcp-inbound-buffer-size: ${tcpInboundBufferSize}
         |   workflow-buffer-size: ${workflowBufferSize}
         | }
       """.stripMargin
    }

    logger.info( "Settings.toConfig base:[{}]", settingsConfig )

    ConfigFactory.parseString( settingsConfig ) withFallback config
  }

  def usage: String = {
    s"""
      |\nRunning Spotlight using the following configuration:
      |\tsource binding  : ${sourceAddress}
      |\tcluster port    : ${clusterPort}
      |\tpublish binding : ${graphiteAddress}
      |\tmax frame size  : ${maxFrameLength}
      |\tprotocol        : ${protocol}
      |\twindow          : ${windowDuration.toCoarsest}
      |\tdetection budget: ${detectionBudget}
      |\tAvail Processors: ${Runtime.getRuntime.availableProcessors}
      |\tplans           : [${plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]
    """.stripMargin
  }
}

object Settings extends LazyLogging {
  val SettingsPathRoot = "spotlight.settings."

//  def sourceAddressFrom( c: Config ): Option[InetSocketAddress] = {
//    val Path = SettingsPathRoot + "source-address"
//    if ( c hasPath Path ) {
//      //todo look into InetSocketAddress to reverse string into object
////      val addr = c getString Path
////      Some( InetSocketAddress.createUnresolved(addr, 0) )
//      ???
//    } else {
//      None
//    }
//  }

  def clusterPortFrom( c: Config ): Option[Int] = {
    val Path = SettingsPathRoot + "cluster-port"
    if ( c hasPath Path ) Some( c getInt Path ) else None
  }

  def maxFrameLengthFrom( c: Config ): Option[Int] = {
    val Path = SettingsPathRoot + "max-frame-length"
    if ( c hasPath Path ) Some( c getInt Path ) else None
  }

//  def protocolFrom( c: Config ): Option[GraphiteSerializationProtocol] = {
//    val Path = SettingsPathRoot + "protocol"
//    //todo use reflection to load and instantiate object from class name
//    if ( c hasPath Path ) Some( c getInt Path ) else None
//  }

  def windowDurationFrom( c: Config ): Option[FiniteDuration] = {
    val Path = SettingsPathRoot + "window-duration"
    if ( c hasPath Path ) Some( FiniteDuration(c.getDuration(Path, NANOSECONDS), NANOSECONDS) ) else None
  }

  //  def graphiteAddressFrom( c: Config ): Option[InetSocketAddress] = {
  //    val Path = SettingsPathRoot + "graphite-address"
  //    if ( c hasPath Path ) {
  //      //todo look into InetSocketAddress to reverse string into object
  ////      val addr = c getString Path
  ////      Some( InetSocketAddress.createUnresolved(addr, 0) )
  //      ???
  //    } else {
  //      None
  //    }
  //  }

  def detectionBudgetFrom( c: Config ): Option[Duration] = {
    spotlight.analysis.durationFrom( c, SettingsPathRoot + "detection-budget" )
  }

  def maxInDetectionCpuFactorFrom( c: Config ): Option[Double] = {
    val Path = SettingsPathRoot + "max-in-detection-cpu-factor"
    if ( c hasPath Path ) Some( c getDouble Path ) else None
  }

  def tcpInboundBufferSizeFrom( c: Config ): Option[Int] = {
    val Path = SettingsPathRoot + "tcp-inbound-buffer-size"
    if ( c hasPath Path ) Some( c getInt Path ) else None
  }

  def workflowBufferSizeFrom( c: Config ): Option[Int] = {
    val Path = SettingsPathRoot + "workflow-buffer-size"
    if ( c hasPath Path ) Some( c getInt Path ) else None
  }


  type Reload = () => V[Settings]


  def reloader(
    args: Array[String]
  )(
    config: => Config = { ConfigFactory.load }
  )(
    invalidateCache: () => Unit = () => { ConfigFactory.invalidateCaches() }
  ): Reload = {
    val usage = checkUsage( args )

    () => {
      invalidateCache()
      for {
        u <- usage.disjunction
        c <- checkConfiguration( config, u ).disjunction
      } yield makeSettings( u, c )
    }
  }

  def apply( args: Array[String], config: => Config = ConfigFactory.load ): Valid[Settings] = {
    import scalaz.Validation.FlatMap._

    for {
      u <- checkUsage( args )
      c <- checkConfiguration( config, u )
    } yield makeSettings( u, c )
  }

  private def checkUsage( args: Array[String] ): Valid[UsageSettings] = {
    val parser = UsageSettings.makeUsageConfig
    logger.info( "Settings args:[{}]", args )
    parser.parse( args, UsageSettings.zero ) match {
      case Some( settings ) => settings.successNel
      case None => Validation.failureNel( UsageConfigurationError(parser.usage) )
    }
  }

  private object Directory {
    val SOURCE_HOST = "spotlight.source.host"
    val SOURCE_PORT = "spotlight.source.port"
    val SOURCE_MAX_FRAME_LENGTH = "spotlight.source.max-frame-length"
    val SOURCE_PROTOCOL = "spotlight.source.protocol"
    val SOURCE_WINDOW_SIZE = "spotlight.source.window-size"
    val PUBLISH_GRAPHITE_HOST = "spotlight.publish.graphite.host"
    val PUBLISH_GRAPHITE_PORT = "spotlight.publish.graphite.port"
    val DETECTION_BUDGET = "spotlight.workflow.detect.timeout"
    val PLAN_PATH = "spotlight.detection-plans"
    val TCP_INBOUND_BUFFER_SIZE = "spotlight.source.buffer"
    val WORKFLOW_BUFFER_SIZE = "spotlight.workflow.buffer"
  }

  private def checkConfiguration( config: Config, usage: UsageSettings ): Valid[Config] = {
    object Req {
      def withUsageSetting( path: String, setting: Option[_] ): Req = new Req( path, () => { setting.isDefined } )
      def withoutUsageSetting( path: String ): Req = new Req( path, NotDefined )
      val NotDefined = () => { false }

      def check( r: Req ): Valid[String] = {
        if ( r.inUsage() || config.hasPath(r.path) ) r.path.successNel
        else Validation.failureNel( UsageConfigurationError( s"expected configuration path [${r.path}]" ) )
      }
    }
    case class Req( path: String, inUsage: () => Boolean )

    val required: List[Req] = List(
      Req.withUsageSetting( Directory.SOURCE_HOST, usage.sourceHost ),
      Req.withUsageSetting( Directory.SOURCE_PORT, usage.sourcePort ),
//      Req.withoutUsageSetting( Directory.PUBLISH_GRAPHITE_HOST ),
//      Req.withoutUsageSetting( Directory.PUBLISH_GRAPHITE_PORT ),
      Req.withoutUsageSetting( Directory.DETECTION_BUDGET ),
      Req.withoutUsageSetting( Directory.PLAN_PATH ),
      Req.withoutUsageSetting( Directory.TCP_INBOUND_BUFFER_SIZE ),
      Req.withoutUsageSetting( Directory.WORKFLOW_BUFFER_SIZE )
    )

    required.traverseU{ Req.check }.map{ _ => config }
  }

  private def makeSettings( usage: UsageSettings, config: Config ): Settings = {
    val sourceHost: InetAddress = usage.sourceHost getOrElse {
      if ( config.hasPath(Directory.SOURCE_HOST) ) InetAddress.getByName( config.getString(Directory.SOURCE_HOST) )
      else InetAddress.getLocalHost
    }

    val sourcePort = usage.sourcePort getOrElse { config.getInt( Directory.SOURCE_PORT ) }

    val maxFrameLength = {
      if ( config.hasPath(Directory.SOURCE_MAX_FRAME_LENGTH) ) config.getInt( Directory.SOURCE_MAX_FRAME_LENGTH )
      else 4 + scala.math.pow( 2, 20 ).toInt // from graphite documentation
    }

    val protocol = {
      if ( config.hasPath(Directory.SOURCE_PROTOCOL) ) {
        config.getString(Directory.SOURCE_PROTOCOL).toLowerCase match {
          case "messagepack" | "message-pack" => MessagePackProtocol
          case "pickle" => new PythonPickleProtocol
          case _ => new PythonPickleProtocol
        }
      } else {
        new PythonPickleProtocol
      }
    }

    val windowSize = usage.windowSize getOrElse {
      if ( config.hasPath(Directory.SOURCE_WINDOW_SIZE) ) {
        FiniteDuration( config.getDuration( Directory.SOURCE_WINDOW_SIZE ).toNanos, NANOSECONDS )
      } else {
        2.minutes
      }
    }

    val graphiteHost = if ( config.hasPath(Directory.PUBLISH_GRAPHITE_HOST) ) {
      Some( InetAddress.getByName(config.getString(Directory.PUBLISH_GRAPHITE_HOST)) )
    } else {
      None
    }

    val graphitePort = if ( config.hasPath(Directory.PUBLISH_GRAPHITE_PORT) ) {
      Some( config.getInt(Directory.PUBLISH_GRAPHITE_PORT) )
    } else {
      Some( 2004 )
    }

    SimpleSettings(
      sourceAddress = new InetSocketAddress( sourceHost, sourcePort ),
      maxFrameLength = maxFrameLength,
      protocol = protocol,
      windowDuration = windowSize,
      graphiteAddress = {
        for {
          h <- graphiteHost
          p <- graphitePort
        } yield new InetSocketAddress( h, p )
      },
      config = ConfigFactory.parseString( SimpleSettings.AkkaRemotePortPath + "=" + usage.clusterPort ).withFallback( config ),
      args = usage.args
    )
  }

  def makeOutlierReducer( spec: Config ): ReduceOutliers = {
    val MAJORITY = "majority"
    val AT_LEAST = "at-least"
    if ( spec hasPath AT_LEAST ) {
      val threshold = spec getInt AT_LEAST
      ReduceOutliers.byCorroborationCount( threshold )
    } else {
      val threshold = if ( spec hasPath MAJORITY ) spec.getDouble( MAJORITY ) else 50D
      ReduceOutliers.byCorroborationPercentage( threshold )
    }
  }


  object SimpleSettings {
    val AkkaRemotePortPath = "akka.remote.netty.tcp.port"
  }

  final case class SimpleSettings private[Settings](
    override val sourceAddress: InetSocketAddress,
    override val maxFrameLength: Int,
    override val protocol: GraphiteSerializationProtocol,
    override val windowDuration: FiniteDuration,
    override val graphiteAddress: Option[InetSocketAddress],
    override val config: Config,
    override val args: Seq[String]
  ) extends Settings with LazyLogging {
    override def clusterPort: Int = config.getInt( SimpleSettings.AkkaRemotePortPath )

    override def detectionBudget: Duration = spotlight.analysis.durationFrom( config, Directory.DETECTION_BUDGET ).get

    override def parallelismFactor: Double = {
      val path = "spotlight.workflow.detect.parallelism-cpu-factor"
      if ( config hasPath path ) config.getDouble( path ) else 1.0
    }

    override val plans: Set[OutlierPlan] = makePlans( config.getConfig( Settings.Directory.PLAN_PATH ) ) //todo support plan reloading
    override def planOrigin: ConfigOrigin = config.getConfig( Settings.Directory.PLAN_PATH ).origin()
    override def workflowBufferSize: Int = config.getInt( Directory.WORKFLOW_BUFFER_SIZE )
    override def tcpInboundBufferSize: Int = config.getInt( Directory.TCP_INBOUND_BUFFER_SIZE )

    private def makeIsQuorum( spec: Config, algorithmSize: Int ): IsQuorum = {
      val MAJORITY = "majority"
      val AT_LEAST = "at-least"
      if ( spec hasPath AT_LEAST ) {
        val trigger = spec getInt AT_LEAST
        IsQuorum.AtLeastQuorumSpecification( totalIssued = algorithmSize, triggerPoint = trigger )
      } else {
        val trigger = if ( spec hasPath MAJORITY ) spec.getDouble( MAJORITY ) else 50D
        IsQuorum.MajorityQuorumSpecification( totalIssued = algorithmSize, triggerPoint = ( trigger / 100D) )
      }
    }


    private def makePlans( planSpecifications: Config ): Set[OutlierPlan] = {
      import scala.collection.JavaConversions._

      val result = {
        planSpecifications
        .root
        .collect { case (n, s: ConfigObject) => (n, s.toConfig) }
        .toSeq
        .map {
          case (name, spec) => {
            logger.warn(
              "#TEST #INFO Settings making plan from specification[origin:{} @ {}]: [{}]",
              spec.origin(), spec.origin().lineNumber().toString, spec
            )

            val IS_DEFAULT = "is-default"
            val TOPICS = "topics"
            val REGEX = "regex"

            val grouping: Option[OutlierPlan.Grouping] = {
              val GROUP_LIMIT = "group.limit"
              val GROUP_WITHIN = "group.within"
              val limit = if ( spec hasPath GROUP_LIMIT ) spec getInt GROUP_LIMIT else 10000
              logger.info( "CONFIGURATION spec: [{}]", spec )
              val window = if ( spec hasPath GROUP_WITHIN ) {
                Some( FiniteDuration( spec.getDuration( GROUP_WITHIN ).toNanos, NANOSECONDS ) )
              } else {
                None
              }

              window map { w => OutlierPlan.Grouping( limit, w ) }
            }

            //todo: add configuration for at-least and majority
            val ( timeout, algorithms ) = pullCommonPlanFacets( spec )

            if ( spec.hasPath(IS_DEFAULT) && spec.getBoolean(IS_DEFAULT) ) {
              logger info s"topic[$name] default plan origin: ${spec.origin} line:${spec.origin.lineNumber}"
              Some(
                OutlierPlan.default(
                  name = name,
                  timeout = timeout,
                  isQuorum = makeIsQuorum( spec, algorithms.size ),
                  reduce = makeOutlierReducer( spec ),
                  algorithms = algorithms,
                  grouping = grouping,
                  planSpecification = spec
                )
              )
            } else if ( spec hasPath TOPICS ) {
              import scala.collection.JavaConverters._
              logger info s"topic[$name] topic plan origin: ${spec.origin} line:${spec.origin.lineNumber}"

              Some(
                OutlierPlan.forTopics(
                  name = name,
                  timeout = timeout,
                  isQuorum = makeIsQuorum( spec, algorithms.size ),
                  reduce = makeOutlierReducer( spec ),
                  algorithms = algorithms,
                  grouping = grouping,
                  planSpecification = spec,
                  extractTopic = OutlierDetection.extractOutlierDetectionTopic,
                  topics = spec.getStringList(TOPICS).asScala.map{ Topic(_) }.toSet
                )
              )
            } else if ( spec hasPath REGEX ) {
              logger info s"topic[$name] regex plan origin: ${spec.origin} line:${spec.origin.lineNumber}"
              Some(
                OutlierPlan.forRegex(
                  name = name,
                  timeout = timeout,
                  isQuorum = makeIsQuorum( spec, algorithms.size ),
                  reduce = makeOutlierReducer( spec ),
                  algorithms = algorithms,
                  grouping = grouping,
                  planSpecification = spec,
                  extractTopic = OutlierDetection.extractOutlierDetectionTopic,
                  regex = new Regex( spec.getString(REGEX) )
                )
              )
            } else {
              None
            }
          }
        }
      }

      result.flatten.toSet
    }

    private def pullCommonPlanFacets( spec: Config ): (Duration, Set[Symbol]) = {
      import scala.collection.JavaConversions._

      def effectiveBudget( budget: Duration, utilization: Double ): Duration = {
        budget match {
          case b if b.isFinite() => utilization * b
          case b => b
        }
      }

      val AlgorithmsPath = "algorithms"
      val algorithms = {
        if ( spec hasPath AlgorithmsPath ) spec.getStringList( AlgorithmsPath ).toSet.map{ a: String => Symbol( a ) }
        else Set.empty[Symbol]
      }

      ( effectiveBudget( detectionBudget, 0.8D ), algorithms )
    }
  }

  case class UsageConfigurationError private[Settings]( usage: String ) extends IllegalArgumentException( usage )

  final case class UsageSettings private[Settings](
    clusterPort: Int = 2552,
    sourceHost: Option[InetAddress] = None,
    sourcePort: Option[Int] = None,
    windowSize: Option[FiniteDuration] = None,
    args: Seq[String] = Seq.empty[String]
  )

  private object UsageSettings {
    def zero: UsageSettings = UsageSettings()

    def makeUsageConfig = new scopt.OptionParser[UsageSettings]( "spotlight" ) {
      //todo remove once release is current with corresponding dev
      implicit val inetAddressRead: scopt.Read[InetAddress] = scopt.Read.reads { InetAddress.getByName(_) }

      head( "spotlight", "0.1.a" )

      opt[InetAddress]( 'h', "host" ) action { (e, c) =>
        c.copy( sourceHost = Some(e) )
      } text( "connection address to source" )

      opt[Int]( 'p', "port" ) action { (e, c) =>
        c.copy( sourcePort = Some(e) )
      } text( "connection port of source server")

      opt[Int]( 'c', "cluster-port" ) action { (e, c) =>
        c.copy( clusterPort = e )
      }

      opt[Long]( 'w', "window" ) action { (e, c) =>
        c.copy( windowSize = Some(FiniteDuration(e, SECONDS)) )
      } text( "batch window size (in seconds) for collecting time series data. Default = 60s." )

      arg[String]( "<arg>..." ).unbounded().optional().action { (a, c) => c.copy( args = c.args :+ a )}

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
