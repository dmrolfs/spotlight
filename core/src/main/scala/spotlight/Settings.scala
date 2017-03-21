package spotlight

import java.net.{ InetAddress, InetSocketAddress }

import scala.concurrent.duration._
import scala.util.matching.Regex
import scalaz.Scalaz._
import scalaz._
import com.typesafe.config._
import net.ceedubs.ficus.Ficus._
import com.persist.logging._
import java.net

import omnibus.commons.{ V, Valid }
import omnibus.commons.config._
import spotlight.analysis.OutlierDetection
import spotlight.infrastructure.ClusterRole
import spotlight.model.outlier._
import spotlight.model.timeseries.Topic
import spotlight.protocol.{ GraphiteSerializationProtocol, MessagePackProtocol, PythonPickleProtocol }

//todo refactor into base required settings and allow for app-specific extension
/** Created by rolfsd on 1/12/16.
  */
trait Settings extends ClassLogging {
  def sourceAddress: InetSocketAddress
  def clusterPort: Int
  def maxFrameLength: Int
  def protocol: GraphiteSerializationProtocol
  def windowDuration: FiniteDuration
  def graphiteAddress: Option[InetSocketAddress]
  def detectionBudget: Duration
  def parallelismFactor: Double
  def plans: Set[AnalysisPlan]
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
         |   window-duration: ${windowDuration.toCoarsest}
         |   ${graphiteAddress.map { ga ⇒ "graphite-address:\"" + ga.toString + "\"" } getOrElse ""}
         |   detection-budget: ${detectionBudget.toCoarsest}
         |   parallelism-factor: ${parallelismFactor}
         |   parallelism: ${parallelism}
         |   tcp-inbound-buffer-size: ${tcpInboundBufferSize}
         |   workflow-buffer-size: ${workflowBufferSize}
         | }
       """.stripMargin
    }

    log.info(
      Map(
        "@msg" → "Settings.toConfig",
        "base" → Map(
          "source-address" → sourceAddress.toString,
          "cluster-port" → clusterPort,
          "max-frame-length" → maxFrameLength,
          "protocol" → protocol.getClass.getCanonicalName,
          "window-duration" → windowDuration.toCoarsest.toString,
          "graphite-address" → graphiteAddress.toString,
          "detection-budget" → detectionBudget.toCoarsest.toString,
          "parallelism-factor" → parallelismFactor,
          "parallelism" → parallelism,
          "tcp-inbound-buffer-size" → tcpInboundBufferSize,
          "workflow-buffer-size" → workflowBufferSize
        )
      )
    )

    ConfigFactory.parseString( settingsConfig ) withFallback config
  }

  def usage: ( String, Map[String, Any] ) = {
    val displayUsage = s"""
      |\nRunning Spotlight using the following configuration:
      |\tsource binding  : ${sourceAddress}
      |\tcluster port    : ${clusterPort}
      |\tpublish binding : ${graphiteAddress}
      |\tmax frame size  : ${maxFrameLength}
      |\tprotocol        : ${protocol}
      |\twindow          : ${windowDuration.toCoarsest}
      |\tdetection budget: ${detectionBudget.toCoarsest}
      |\tAvail Processors: ${Runtime.getRuntime.availableProcessors}
      |\tplans           : [${plans.zipWithIndex.map { case ( p, i ) ⇒ f"${i}%2d: ${p}" }.mkString( "\n", "\n", "\n" )}]
    """.stripMargin

    val planMap: Map[String, Any] = Map(
      plans.toSeq.map {
        case p ⇒ (
          p.name,
          Map(
            "algorithms" → p.algorithmKeys.mkString( "[", ", ", "]" ),
            "applies-to" → p.appliesTo.toString,
            "reduce" → p.reduce.toString,
            "is-quorum" → p.isQuorum.toString,
            "timeout" → p.timeout.toString
          )
        )
      }: _*
    )

    val richUsage = Map(
      "source-binding" → sourceAddress.toString,
      "cluster-port" → clusterPort,
      "publish-binding" → graphiteAddress.toString,
      "max-frame-size" → maxFrameLength,
      "protocol" → protocol.toString,
      "window" → windowDuration.toCoarsest.toString,
      "detection-budget" → detectionBudget.toCoarsest.toString,
      "available-processors" → Runtime.getRuntime.availableProcessors,
      "plans" → planMap
    )

    ( displayUsage, richUsage )
  }
}

object Settings extends ClassLogging {
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

  def clusterPortFrom( c: Config ): Option[Int] = c.as[Option[Int]]( SettingsPathRoot + "cluster-port" )
  def maxFrameLengthFrom( c: Config ): Option[Int] = c.as[Option[Int]]( SettingsPathRoot + "max-frame-length" )

  //  def protocolFrom( c: Config ): Option[GraphiteSerializationProtocol] = {
  //    val Path = SettingsPathRoot + "protocol"
  //    //todo use reflection to load and instantiate object from class name
  //    if ( c hasPath Path ) Some( c getInt Path ) else None
  //  }

  def windowDurationFrom( c: Config ): Option[FiniteDuration] = {
    c.as[Option[FiniteDuration]]( SettingsPathRoot + "window-duration" )
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

  def detectionBudgetFrom( c: Config ): Option[Duration] = c.as[Option[Duration]]( SettingsPathRoot + "detection-budget" )
  //    {
  //    spotlight.analysis.durationFrom( c, SettingsPathRoot + "detection-budget" )
  //  }

  def maxInDetectionCpuFactorFrom( c: Config ): Option[Double] = {
    c.as[Option[Double]]( SettingsPathRoot + "max-in-detection-cpu-factor" )
  }

  def tcpInboundBufferSizeFrom( c: Config ): Option[Int] = c.as[Option[Int]]( SettingsPathRoot + "tcp-inbound-buffer-size" )
  def workflowBufferSizeFrom( c: Config ): Option[Int] = c.as[Option[Int]]( SettingsPathRoot + "workflow-buffer-size" )

  def detectionPlansConfigFrom( c: Config ): Config = {
    c.as[Option[Config]]( Directory.PLAN_PATH )
      .getOrElse {
        log.warn( Map( "@msg" → "no plan specifications found at expected configuration path", "path" → Directory.PLAN_PATH ) )
        ConfigFactory.empty()
      }
  }

  type Reload = () ⇒ V[Settings]

  def reloader(
    args: Array[String]
  )(
    config: ⇒ Config = { ConfigFactory.load }
  )(
    invalidateCache: () ⇒ Unit = () ⇒ { ConfigFactory.invalidateCaches() }
  ): Reload = {
    val usage = checkUsage( args )

    () ⇒ {
      invalidateCache()
      for {
        u ← usage.disjunction
        c ← checkConfiguration( config, u ).disjunction
      } yield makeSettings( u, c )
    }
  }

  def apply( args: Array[String], config: ⇒ Config = ConfigFactory.load ): Valid[Settings] = {
    import scalaz.Validation.FlatMap._

    for {
      u ← checkUsage( args )
      c ← checkConfiguration( config, u )
    } yield makeSettings( u, c )
  }

  private def checkUsage( args: Array[String] ): Valid[UsageSettings] = {
    val parser = UsageSettings.makeUsageConfig
    log.info( Map( "@msg" → "Settings args", "args" → args ) )
    parser.parse( args, UsageSettings.zero ) match {
      case Some( settings ) ⇒ settings.successNel
      case None ⇒ Validation.failureNel( UsageConfigurationError( parser.usage ) )
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
      def withUsageSetting( path: String, setting: Option[_] ): Req = new Req( path, () ⇒ { setting.isDefined } )
      def withoutUsageSetting( path: String ): Req = new Req( path, NotDefined )
      val NotDefined = () ⇒ { false }

      def check( r: Req ): Valid[String] = {
        if ( r.inUsage() || config.hasPath( r.path ) ) r.path.successNel
        else Validation.failureNel( UsageConfigurationError( s"expected configuration path [${r.path}]" ) )
      }
    }
    case class Req( path: String, inUsage: () ⇒ Boolean )

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

    required.traverseU { Req.check }.map { _ ⇒ config }
  }

  private def makeSettings( usage: UsageSettings, config: Config ): Settings = {
    val sourceHost: InetAddress = {
      usage
        .sourceHost
        .getOrElse {
          config
            .as[Option[InetAddress]]( Directory.SOURCE_HOST )
            .getOrElse { net.InetAddress.getLocalHost }
        }
    }

    val sourcePort = usage.sourcePort getOrElse { config.as[Int]( Directory.SOURCE_PORT ) }

    val maxFrameLength = {
      config.as[Option[Int]]( Directory.SOURCE_MAX_FRAME_LENGTH ) getOrElse { 4 + scala.math.pow( 2, 20 ).toInt } // from graphite documentation
    }

    val protocol = {
      config
        .as[Option[String]]( Directory.SOURCE_PROTOCOL )
        .collect {
          case "messagepack" | "message-pack" ⇒ MessagePackProtocol
          case "pickle" ⇒ new PythonPickleProtocol
          case _ ⇒ new PythonPickleProtocol
        }
        .getOrElse { new PythonPickleProtocol }
    }

    val windowSize = {
      usage
        .windowSize
        .getOrElse {
          config.as[Option[FiniteDuration]]( Directory.SOURCE_WINDOW_SIZE ) getOrElse { 2.minutes }
        }
    }

    val graphiteHost = config.as[Option[InetAddress]]( Directory.PUBLISH_GRAPHITE_HOST )

    val graphitePort = config.as[Option[Int]]( Directory.PUBLISH_GRAPHITE_PORT ) getOrElse { 2004 }

    SimpleSettings(
      sourceAddress = new InetSocketAddress( sourceHost, sourcePort ),
      maxFrameLength = maxFrameLength,
      protocol = protocol,
      windowDuration = windowSize,
      graphiteAddress = {
      for {
        h ← graphiteHost
        p ← Option( graphitePort )
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

  final case class SimpleSettings private[Settings] (
      override val sourceAddress: InetSocketAddress,
      override val maxFrameLength: Int,
      override val protocol: GraphiteSerializationProtocol,
      override val windowDuration: FiniteDuration,
      override val graphiteAddress: Option[InetSocketAddress],
      override val config: Config,
      override val args: Seq[String]
  ) extends Settings {
    override def clusterPort: Int = config.as[Int]( SimpleSettings.AkkaRemotePortPath )

    override def detectionBudget: Duration = config.as[Duration]( Directory.DETECTION_BUDGET )

    override def parallelismFactor: Double = {
      config.as[Option[Double]]( "spotlight.workflow.detect.parallelism-cpu-factor" ) getOrElse { 1.0 }
    }

    override val plans: Set[AnalysisPlan] = {
      PlanFactory.makePlans(
        planSpecifications = detectionPlansConfigFrom( config ),
        globalAlgorithms = PlanFactory.globalAlgorithmConfigurationsFrom( config ),
        detectionBudget = detectionBudget
      )
    } //todo support plan reloading
    override def planOrigin: ConfigOrigin = detectionPlansConfigFrom( config ).origin()
    override def workflowBufferSize: Int = config.as[Int]( Directory.WORKFLOW_BUFFER_SIZE )
    override def tcpInboundBufferSize: Int = config.as[Int]( Directory.TCP_INBOUND_BUFFER_SIZE )

  }

  case class UsageConfigurationError private[Settings] ( usage: String ) extends IllegalArgumentException( usage )

  final case class UsageSettings private[Settings] (
    role: ClusterRole,
    clusterPort: Int = 2552,
    sourceHost: Option[InetAddress] = None,
    sourcePort: Option[Int] = None,
    windowSize: Option[FiniteDuration] = None,
    args: Seq[String] = Seq.empty[String]
  )

  private object UsageSettings {
    def zero: UsageSettings = UsageSettings( role = ClusterRole.All )

    def makeUsageConfig = new scopt.OptionParser[UsageSettings]( "spotlight" ) {
      //todo remove once release is current with corresponding dev
      implicit val inetAddressRead: scopt.Read[InetAddress] = scopt.Read.reads { InetAddress.getByName( _ ) }

      head( "spotlight", spotlight.BuildInfo.version )

      opt[ClusterRole]( 'r', "role" )
        .required()
        .action { ( r, c ) ⇒ c.copy( role = r ) }
        .text( "role played in analysis cluster" )

      opt[InetAddress]( 'h', "host" )
        .action { ( e, c ) ⇒ c.copy( sourceHost = Some( e ) ) }
        .text( "connection address to source" )

      opt[Int]( 'p', "port" )
        .action { ( e, c ) ⇒ c.copy( sourcePort = Some( e ) ) }
        .text( "connection port of source server" )

      opt[Int]( 'c', "cluster-port" )
        .action { ( e, c ) ⇒ c.copy( clusterPort = e ) }
        .text( "listening port for this node in the processing cluster. " +
          "There must be at least one seed at 2551 or 2552; otherwise can be 0" )

      opt[Long]( 'w', "window" )
        .action { ( e, c ) ⇒ c.copy( windowSize = Some( FiniteDuration( e, SECONDS ) ) ) }
        .text( "batch window size (in seconds) for collecting time series data. Default = 60s." )

      arg[String]( "<arg>..." )
        .unbounded()
        .optional()
        .action { ( a, c ) ⇒ c.copy( args = c.args :+ a ) }

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

  object PlanFactory {
    def makePlans(
      planSpecifications: Config,
      globalAlgorithms: Map[String, Config],
      detectionBudget: Duration
    ): Set[AnalysisPlan] = {
      import scala.collection.JavaConverters._
      val budget = effectiveBudget( detectionBudget )

      val result = {
        planSpecifications
          .root
          .asScala
          .collect { case ( n, s: ConfigObject ) ⇒ ( n, s.toConfig ) }
          .toSeq
          .map {
            case ( name, spec ) ⇒ {
              log.info(
                Map(
                  "@msg" → "Settings making plan from specification",
                  "origin" → Map( "origin" → spec.origin().toString, "line-number" → spec.origin().lineNumber ),
                  "spec" → spec.toString
                )
              )

              val IS_DEFAULT = "is-default"
              val TOPICS = "topics"
              val REGEX = "regex"

              val grouping: Option[AnalysisPlan.Grouping] = {
                val GROUP_LIMIT = "group.limit"
                val GROUP_WITHIN = "group.within"
                val limit = if ( spec hasPath GROUP_LIMIT ) spec getInt GROUP_LIMIT else 10000
                log.info( Map( "@msg" → "grouping spec", "grouping" → spec.toString ) )
                val window = if ( spec hasPath GROUP_WITHIN ) {
                  Some( FiniteDuration( spec.getDuration( GROUP_WITHIN ).toNanos, NANOSECONDS ) )
                } else {
                  None
                }

                window map { w ⇒ AnalysisPlan.Grouping( limit, w ) }
              }

              //todo: add configuration for at-least and majority
              //              val ( timeout, algorithms ) = pullCommonPlanFacets( spec, detectionBudget )
              val algorithms: Map[String, Config] = PlanFactory.algorithmConfigurationsFrom( spec, globalAlgorithms )

              if ( spec.hasPath( IS_DEFAULT ) && spec.getBoolean( IS_DEFAULT ) ) {
                log.info(
                  Map(
                    "@msg" → "default plan",
                    "topic" → name,
                    "origin" → Map( "origin" → spec.origin, "line-number" → spec.origin().lineNumber() )
                  )
                )

                Some(
                  AnalysisPlan.default(
                    name = name,
                    timeout = budget,
                    isQuorum = makeIsQuorum( spec, algorithms.size ),
                    reduce = makeOutlierReducer( spec ),
                    algorithms = algorithms,
                    grouping = grouping,
                    planSpecification = spec
                  )
                )
              } else if ( spec hasPath TOPICS ) {
                import scala.collection.JavaConverters._
                log.info(
                  Map(
                    "@msg" → "topic-specific plan",
                    "topic" → name,
                    "origin" → Map( "origin" → spec.origin, "line-number" → spec.origin().lineNumber() )
                  )
                )

                Some(
                  AnalysisPlan.forTopics(
                    name = name,
                    timeout = budget,
                    isQuorum = makeIsQuorum( spec, algorithms.size ),
                    reduce = makeOutlierReducer( spec ),
                    algorithms = algorithms,
                    grouping = grouping,
                    planSpecification = spec,
                    extractTopic = OutlierDetection.extractOutlierDetectionTopic,
                    topics = spec.getStringList( TOPICS ).asScala.map { Topic( _ ) }.toSet
                  )
                )
              } else if ( spec hasPath REGEX ) {
                log.info(
                  Map(
                    "@msg" → "regex plan",
                    "topic" → name,
                    "origin" → Map( "origin" → spec.origin, "line-number" → spec.origin().lineNumber() )
                  )
                )

                Some(
                  AnalysisPlan.forRegex(
                    name = name,
                    timeout = budget,
                    isQuorum = makeIsQuorum( spec, algorithms.size ),
                    reduce = makeOutlierReducer( spec ),
                    algorithms = algorithms,
                    grouping = grouping,
                    planSpecification = spec,
                    extractTopic = OutlierDetection.extractOutlierDetectionTopic,
                    regex = new Regex( spec.getString( REGEX ) )
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

    private def effectiveBudget( budget: Duration ): Duration = {
      val utilization = 0.8
      budget match {
        case b if b.isFinite() ⇒ utilization * b
        case b ⇒ b
      }
    }

    def globalAlgorithmConfigurationsFrom( config: Config ): Map[String, Config] = {
      val GlobalAlgorithmsPath = "spotlight.algorithms"

      config
        .as[Option[Config]]( GlobalAlgorithmsPath )
        .map { global ⇒
          import scala.collection.JavaConverters._
          import scala.reflect._
          val ConfigObjectType = classTag[ConfigObject]

          val algos = {
            global.root().entrySet().asScala.toSeq
              .map { entry ⇒ ( entry.getKey, entry.getValue ) }
              .collect { case ( name, ConfigObjectType( cv ) ) ⇒ ( name, cv.toConfig ) }
          }

          Map( algos: _* )
        }
        .getOrElse { Map.empty[String, Config] }
    }

    def algorithmConfigurationsFrom( planSpec: Config, globalAlgorithms: Map[String, Config] ): Map[String, Config] = {
      def correspondingGlobal( name: String ): Config = globalAlgorithms.getOrElse( name, ConfigFactory.empty )

      val AlgorithmsPath = "algorithms"
      planSpec
        .as[Option[Config]]( AlgorithmsPath )
        .map { specAlgorithms ⇒
          import scala.collection.JavaConverters._
          val specAlgorithms = planSpec.getConfig( AlgorithmsPath )
          val algos = {
            import scala.reflect._
            import ConfigValueType.{ BOOLEAN, OBJECT, STRING }
            val ConfigObjectType = classTag[ConfigObject]

            specAlgorithms.root.entrySet.asScala.toSeq
              .map { e ⇒ ( e.getKey, e.getValue ) }
              .collect {
                case ( n, v ) if v.valueType == STRING && specAlgorithms.getBoolean( n ) == true ⇒ ( n, correspondingGlobal( n ) )
                case ( n, v ) if v.valueType == BOOLEAN ⇒ ( n, correspondingGlobal( n ) )
                case ( n, ConfigObjectType( v ) ) if v.valueType == OBJECT ⇒ ( n, v.withFallback( correspondingGlobal( n ) ).toConfig )

                //            case ( n, v ) => {
                //              log.warn(
                //                Map(
                //                  "@msg" -> "UNMATCHED algo",
                //                  "name" -> n
                //                  "v" -> v
                //                  "unwrapped" ->  v.unwrapped,
                //                  "unwrapped-class" -> v.unwrapped.getClass
                //                )
                //              )
                //
                //              ( n, ConfigFactory.parseString("WILL-BE-REMOVED: yes") )
                //            }
              }
          }

          Map( algos: _* )
        }
        .getOrElse { Map.empty[String, Config] }

      //      if ( !planSpec.hasPath( AlgorithmsPath ) ) Map.empty[String, Config]
      //      else {
      //        import scala.collection.JavaConverters._
      //        val specAlgorithms = planSpec.getConfig( AlgorithmsPath )
      //        val algos = {
      //          import scala.reflect._
      //          import ConfigValueType.{ BOOLEAN, OBJECT, STRING }
      //          val ConfigObjectType = classTag[ConfigObject]
      //
      //          specAlgorithms.root.entrySet.asScala.toSeq
      //            .map { e ⇒ ( e.getKey, e.getValue ) }
      //            .collect {
      //              case ( n, v ) if v.valueType == STRING && specAlgorithms.getBoolean( n ) == true ⇒ ( n, correspondingGlobal( n ) )
      //              case ( n, v ) if v.valueType == BOOLEAN ⇒ ( n, correspondingGlobal( n ) )
      //              case ( n, ConfigObjectType( v ) ) if v.valueType == OBJECT ⇒ ( n, v.withFallback( correspondingGlobal( n ) ).toConfig )
      //
      //              //            case ( n, v ) => {
      //              //              log.warn(
      //              //                Map(
      //              //                  "@msg" -> "UNMATCHED algo",
      //              //                  "name" -> n
      //              //                  "v" -> v
      //              //                  "unwrapped" ->  v.unwrapped,
      //              //                  "unwrapped-class" -> v.unwrapped.getClass
      //              //                )
      //              //              )
      //              //
      //              //              ( n, ConfigFactory.parseString("WILL-BE-REMOVED: yes") )
      //              //            }
      //            }
      //        }
      //
      //        Map( algos: _* )
      //      }
    }

    //    private def pullCommonPlanFacets( spec: Config, detectionBudget: Duration ): ( Duration, Set[String] ) = {
    //      import scala.collection.JavaConverters._
    //
    //      def effectiveBudget( budget: Duration, utilization: Double ): Duration = {
    //        budget match {
    //          case b if b.isFinite() ⇒ utilization * b
    //          case b ⇒ b
    //        }
    //      }
    //
    //      val AlgorithmsPath = "algorithms"
    //      val algorithms: Set[String] = {
    //        if ( spec hasPath AlgorithmsPath ) spec.getStringList( AlgorithmsPath ).asScala.toSet else Set.empty[String]
    //      }
    //
    //      ( effectiveBudget( detectionBudget, 0.8D ), algorithms )
    //    }

    private def makeIsQuorum( spec: Config, algorithmSize: Int ): IsQuorum = {
      val MAJORITY = "majority"
      val AT_LEAST = "at-least"

      spec
        .as[Option[Int]]( AT_LEAST )
        .map { trigger ⇒ IsQuorum.AtLeastQuorumSpecification( totalIssued = algorithmSize, triggerPoint = trigger ) }
        .getOrElse {
          val trigger = spec.as[Option[Double]]( MAJORITY ) getOrElse { 50D }
          IsQuorum.MajorityQuorumSpecification( totalIssued = algorithmSize, triggerPoint = ( trigger / 100D ) )
        }
    }
  }
}

