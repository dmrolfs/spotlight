package spotlight.stream

import java.net.{ InetAddress, InetSocketAddress }
import java.{ lang, time, util }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.matching.Regex
import scala.collection.immutable
import scalaz.Scalaz._
import scalaz._
import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import spotlight.analysis.outlier.OutlierDetection
import spotlight.model.outlier._
import spotlight.model.timeseries.{ TimeSeriesBase, Topic }
import spotlight.protocol.{ GraphiteSerializationProtocol, MessagePackProtocol, PythonPickleProtocol }
import peds.commons.{ V, Valid }


/**
  * Created by rolfsd on 1/12/16.
  */
trait Configuration extends Config {
  def sourceAddress: InetSocketAddress
  def maxFrameLength: Int
  def protocol: GraphiteSerializationProtocol
  def windowDuration: FiniteDuration
  def graphiteAddress: Option[InetSocketAddress]
  def detectionBudget: FiniteDuration
  def plans: immutable.Seq[OutlierPlan]
  def planOrigin: ConfigOrigin
  def tcpInboundBufferSize: Int
  def workflowBufferSize: Int

  def usage: String = {
    s"""
      |\nRunning Spotlight using the following configuration:
      |\tsource binding  : ${sourceAddress}
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

object Configuration {
  type Reload = () => V[Configuration]

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
      } yield makeConfiguration( u, c)
    }
  }

  def apply( args: Array[String], config: => Config = ConfigFactory.load ): Valid[Configuration] = {
    import scalaz.Validation.FlatMap._

    for {
      u <- checkUsage( args )
      c <- checkConfiguration( config, u )
    } yield makeConfiguration( u, c )
  }

  private def checkUsage( args: Array[String] ): Valid[UsageSettings] = {
    val parser = UsageSettings.makeUsageConfig
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

  private def makeConfiguration( usage: UsageSettings, config: Config ): Configuration = {
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

    SimpleConfiguration(
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
      underlying = config
    )
  }

  val defaultOutlierReducer: ReduceOutliers = new ReduceOutliers {
    override def apply(
      results: OutlierAlgorithmResults,
      source: TimeSeriesBase,
      plan: OutlierPlan
    )(
      implicit ec: ExecutionContext
    ): Future[Outliers] = {
      Future {
        results.headOption map { _._2 } getOrElse { NoOutliers( algorithms = results.keySet, source = source, plan = plan ) }
      }
    }
  }



  final case class SimpleConfiguration private[stream](
    override val sourceAddress: InetSocketAddress,
    override val maxFrameLength: Int,
    override val protocol: GraphiteSerializationProtocol,
    override val windowDuration: FiniteDuration,
    override val graphiteAddress: Option[InetSocketAddress],
    underlying: Config
  ) extends Configuration with LazyLogging {
    override def detectionBudget: FiniteDuration = {
      FiniteDuration( getDuration(Directory.DETECTION_BUDGET, MILLISECONDS), MILLISECONDS )
    }

    override def plans: immutable.Seq[OutlierPlan] = makePlans( getConfig( Configuration.Directory.PLAN_PATH ) )
    override def planOrigin: ConfigOrigin = getConfig( Configuration.Directory.PLAN_PATH ).origin()
    override def workflowBufferSize: Int = getInt( Directory.WORKFLOW_BUFFER_SIZE )
    override def tcpInboundBufferSize: Int = getInt( Directory.TCP_INBOUND_BUFFER_SIZE )

    private def makePlans( planSpecifications: Config ): immutable.Seq[OutlierPlan] = {
      import scala.collection.JavaConversions._

      val result = planSpecifications.root.collect{ case (n, s: ConfigObject) => (n, s.toConfig) }.toSeq.map {
        case (name, spec) => {
          val IS_DEFAULT = "is-default"
          val TOPICS = "topics"
          val REGEX = "regex"

          val ( timeout, algorithms ) = pullCommonPlanFacets( spec )

          if ( spec.hasPath(IS_DEFAULT) && spec.getBoolean(IS_DEFAULT) ) {
            Some(
              OutlierPlan.default(
                name = name,
                timeout = timeout,
                isQuorum = IsQuorum.AtLeastQuorumSpecification( totalIssued = algorithms.size, triggerPoint = 1 ),
                reduce = defaultOutlierReducer,
                algorithms = algorithms,
                planSpecification = spec
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
                reduce = defaultOutlierReducer,
                algorithms = algorithms,
                planSpecification = spec,
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
                reduce = defaultOutlierReducer,
                algorithms = algorithms,
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

      result.flatten.sorted.to[immutable.Seq]
    }

    private def pullCommonPlanFacets( spec: Config ): (FiniteDuration, Set[Symbol]) = {
      import scala.collection.JavaConversions._

      def effectiveBudget( budget: FiniteDuration, utilization: Double ): FiniteDuration = {
        val utilized = budget * utilization
        if ( utilized.isFinite ) utilized.asInstanceOf[FiniteDuration] else budget
      }

      val AlgorithmsPath = "algorithms"
      val algorithms = {
        if ( spec hasPath AlgorithmsPath ) spec.getStringList( AlgorithmsPath ).toSet.map{ a: String => Symbol( a ) }
        else Set.empty[Symbol]
      }

      ( effectiveBudget( detectionBudget, 0.8D ), algorithms )
    }


    override def getAnyRefList(s: String) = underlying.getAnyRefList(s)
    override def getIntList(s: String): util.List[Integer] = underlying.getIntList(s)
    override def getValue(s: String): ConfigValue = underlying.getValue(s)
    override def root(): ConfigObject = underlying.root()
    override def getAnyRef(s: String): AnyRef = underlying.getAnyRef(s)
    override def getConfigList(s: String): util.List[_ <: Config] = underlying.getConfigList(s)
    override def getIsNull(s: String): Boolean = underlying.getIsNull(s)
    override def withFallback(configMergeable: ConfigMergeable): Config = underlying.withFallback(configMergeable)
    override def checkValid(config: Config, strings: String*): Unit = underlying.checkValid(config, strings:_*)
    override def resolveWith(config: Config): Config = underlying.resolveWith(config)
    override def resolveWith(config: Config, configResolveOptions: ConfigResolveOptions): Config = {
      underlying.resolveWith(config, configResolveOptions )
    }
    override def getList(s: String): ConfigList = underlying.getList(s)
    override def getDouble(s: String): Double = underlying.getDouble(s)
    override def getLongList(s: String): util.List[lang.Long] = underlying.getLongList(s)
    override def getObjectList(s: String): util.List[_ <: ConfigObject] = underlying.getObjectList(s)
    override def withOnlyPath(s: String): Config = underlying.withOnlyPath(s)
    override def entrySet(): java.util.Set[java.util.Map.Entry[String, ConfigValue]] = underlying.entrySet()
    override def getDoubleList(s: String): java.util.List[java.lang.Double] = underlying.getDoubleList(s)
    override def hasPathOrNull(s: String): Boolean = underlying.hasPathOrNull(s)
    override def hasPath(s: String): Boolean = underlying.hasPath(s)
    override def getLong(s: String): Long = underlying.getLong(s)
    override def getMemorySizeList(s: String): util.List[ConfigMemorySize] = underlying.getMemorySizeList(s)
    override def getBooleanList(s: String): util.List[lang.Boolean] = underlying.getBooleanList(s)
    override def getBytesList(s: String): util.List[lang.Long] = underlying.getBytesList(s)
    override def getBoolean(s: String): Boolean = underlying.getBoolean(s)
    override def getConfig(s: String): Config = underlying.getConfig(s)
    override def getObject(s: String): ConfigObject = underlying.getObject(s)
    override def getStringList(s: String): util.List[String] = underlying.getStringList(s)
    override def getNumberList(s: String): util.List[Number] = underlying.getNumberList(s)
    override def atPath(s: String): Config = underlying.atPath(s)
    override def isResolved: Boolean = underlying.isResolved
    override def isEmpty: Boolean = underlying.isEmpty
    override def atKey(s: String): Config = underlying.atKey(s)
    override def getDuration(s: String, timeUnit: TimeUnit): Long = underlying.getDuration(s, timeUnit)
    override def getDuration(s: String): time.Duration = underlying.getDuration(s)
    override def withValue(s: String, configValue: ConfigValue): Config = underlying.withValue(s, configValue)
    override def getInt(s: String): Int = underlying.getInt(s)
    override def resolve(): Config = underlying.resolve()
    override def resolve(configResolveOptions: ConfigResolveOptions): Config = underlying.resolve(configResolveOptions)
    override def getNumber(s: String): Number = underlying.getNumber(s)
    override def getDurationList(s: String, timeUnit: TimeUnit): util.List[lang.Long] = underlying.getDurationList(s, timeUnit)
    override def getDurationList(s: String): util.List[time.Duration] = underlying.getDurationList(s)
    override def origin(): ConfigOrigin = underlying.origin()
    override def withoutPath(s: String): Config = underlying.withoutPath(s)
    override def getMemorySize(s: String): ConfigMemorySize = underlying.getMemorySize(s)
    override def getBytes(s: String): lang.Long = underlying.getBytes(s)
    override def getString(s: String): String = underlying.getString(s)

    @deprecated( "replaced by {@link #getDurationList(String, TimeUnit)}", "1.1" )
    override def getNanoseconds(s: String): lang.Long = underlying.getNanoseconds(s)

    @deprecated( "replaced by {@link #getDurationList(String, TimeUnit)}", "1.1" )
    override def getNanosecondsList(s: String): util.List[lang.Long] = underlying.getNanosecondsList(s)

    @deprecated( "replaced by {@link #getDurationList(String, TimeUnit)}", "1.1" )
    override def getMilliseconds(s: String): lang.Long = underlying.getMilliseconds(s)

    @deprecated( "replaced by {@link #getDurationList(String, TimeUnit)}", "1.1" )
    override def getMillisecondsList(s: String): util.List[lang.Long] = underlying.getMillisecondsList(s)
  }

  case class UsageConfigurationError private[stream]( usage: String ) extends IllegalArgumentException( usage )

  final case class UsageSettings private[stream](
    sourceHost: Option[InetAddress] = None,
    sourcePort: Option[Int] = None,
    windowSize: Option[FiniteDuration] = None
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
