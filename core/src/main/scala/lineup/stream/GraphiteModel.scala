package lineup.stream

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.matching.Regex
import scala.util.{ Failure, Success }

import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.stream.{ ActorMaterializerSettings, Supervision, ActorMaterializer, Materializer }
import akka.util.ByteString

import com.typesafe.config.{ ConfigObject, Config, ConfigFactory }
import com.typesafe.scalalogging.{ Logger, LazyLogging }
import org.slf4j.LoggerFactory
import org.joda.{ time => joda }
import peds.commons.log.Trace

import lineup.analysis.outlier.algorithm.DBSCANAnalyzer
import lineup.analysis.outlier.{ DetectionAlgorithmRouter, OutlierDetection }
import lineup.model.outlier._
import lineup.model.timeseries.TimeSeriesBase.Merging
import lineup.model.timeseries._


/**
 * Created by rolfsd on 10/21/15.
 */
object GraphiteModel extends LazyLogging {
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

        implicit val system = ActorSystem( "Monitor" )
        implicit val materializer = ActorMaterializer( ActorMaterializerSettings(system).withSupervisionStrategy(decider) )
        implicit val ec = system.dispatcher

        val router = system.actorOf( DetectionAlgorithmRouter.props, "router" )
        val dbscan = system.actorOf( DBSCANAnalyzer.props( router ), "dbscan" )

        val detector = system.actorOf( OutlierDetection.props(router, plans) )

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
    protocol: SerializationProtocol,
    windowSize: FiniteDuration,
    detector: ActorRef,
    plans: Seq[OutlierPlan]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Future[Unit] = {
    implicit val dispatcher = system.dispatcher

    val serverBinding: Source[Tcp.IncomingConnection, Future[Tcp.ServerBinding]] = Tcp().bind( host.getHostAddress, port )

    val outlierLogger = Logger( LoggerFactory getLogger "Outliers" )

    val dumpId = new AtomicInteger()

    serverBinding runForeach { connection =>
      val detection = Flow() { implicit b =>
        import FlowGraph.Implicits._

        logger debug s"received connection remote[${connection.remoteAddress}] -> local[${connection.localAddress}]"

        val framing = b.add( protocol.framingFlow( maxFrameLength ) )
        val timeSeries = b.add( graphiteTimeSeries( plans = plans, protocol = protocol, windowSize = windowSize ) )
        val detectOutlier = b.add( OutlierDetection.detectOutlier(detector, 1.second, 4) )
        val tap = b.add( Flow[Outliers].mapAsyncUnordered(parallelism = 4){ o => Future { outlierLogger info o.toString; o } } )
        val last = b.add( Flow[Outliers] map { o => ByteString(o.toString) } )

        framing ~> timeSeries ~> detectOutlier ~> tap ~> last

//        val broadcast = b.add( Broadcast[ByteString](outputPorts = 2, eagerCancel = false) )
//        val counter = b.add( Flow[ByteString] map { e => (dumpId.incrementAndGet(), e) } )
//        val dump = Sink.foreach[(Int, ByteString)] { case (c, e) =>
//          import better.files._
//          val out = "/var/log"/s"dump-${c}.obj"
//          out.newOutputStream.autoClosed foreach { sout =>
//            sout write e.toArray
//            sout.flush()
//          }
//        }
//
//        framing ~> broadcast ~> timeSeries ~> detectOutlier ~> tap ~> last
//                   broadcast ~> Flow[ByteString].take(50) ~> counter ~> dump

        ( framing.inlet, last.outlet )
      }

      connection.handleWith( detection )
    }
  }

  def graphiteTimeSeries(
    plans: Seq[OutlierPlan],
    protocol: SerializationProtocol = PickleProtocol,
    windowSize: FiniteDuration = 1.minute,
    parallelism: Int = 4
  )(
    implicit ec: ExecutionContext,
    tsMerging: Merging[TimeSeries],
    materializer: Materializer
  ): Flow[ByteString, TimeSeries, Unit] = {
    val numTopics = 1
    val metricsLogger = Logger( LoggerFactory getLogger "Metrics" )

    Flow[ByteString]
    .via( protocol.loadTimeSeriesData )
    .map { e =>
      val r = plans.find{ _ appliesTo e }
      logger info s"""Plan for ${e.topic}: ${r getOrElse "NONE"}"""
      e
    }
    .filter { ts => plans exists { _ appliesTo ts } }
    .groupedWithin( n = numTopics * windowSize.toMillis.toInt, d = windowSize ) // max elems = 1 per milli; duration = windowSize
    .map {
      _.groupBy{ _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
    .mapAsyncUnordered( parallelism = 4 ) { e => Future { metricsLogger info e.toString; e } }
  }


  sealed trait SerializationProtocol {
    def charset: String = ByteString.UTF_8
    def framingFlow( maximumFrameLength: Int ): Flow[ByteString, ByteString, Unit]
    def loadTimeSeriesData: Flow[ByteString, TimeSeries, Unit] = Flow[ByteString] mapConcat { toDataPoints }
    def toDataPoints( bytes: ByteString ): List[TimeSeries]
  }

  case object PickleProtocol extends SerializationProtocol {
    final case class PickleException private[stream]( message: String ) extends Exception( message )

    val GraphiteMaximumFrameLength = scala.math.pow( 2, 20 ).toInt

    override def charset: String = "ISO-8859-1"

    override def framingFlow( maximumFrameLength: Int = GraphiteMaximumFrameLength ): Flow[ByteString, ByteString, Unit] = {
      val FieldLength = 4

      Framing.lengthField(
        fieldLength = FieldLength,
        fieldOffset = 0,
        maximumFrameLength = maximumFrameLength,
        byteOrder = ByteOrder.BIG_ENDIAN
      ).map { _ drop FieldLength }
    }

    override def toDataPoints( bytes: ByteString ): List[TimeSeries] = trace.briefBlock( "toDataPoints" ) {
      import net.razorvine.pickle.Unpickler

      import scala.collection.convert.wrapAll._
      import scala.collection.mutable

      case class Metric( topic: Topic, timestamp: joda.DateTime, value: Double )

      if ( bytes.isEmpty ) throw PickleException( "stream pushing with empty bytes" )
      else {
        val unpickler = new Unpickler
        val pck = unpickler.loads( bytes.toArray )
//        trace( s"UNPICKLED CLASS = ${pck.getClass}" )
//        trace.block( s"UNPICKLED DUMP" ) {
//          pck.asInstanceOf[java.util.ArrayList[Array[AnyRef]]].foreach( p => trace( s"Class[${p.getClass}]: $p" ) )
//        }

        val pickles: mutable.Buffer[Array[AnyRef]] = pck.asInstanceOf[java.util.ArrayList[Array[AnyRef]]]

        val metrics = for {
          p <- pickles
        } yield {
          val tuple = p
          val dp = tuple(1).asInstanceOf[Array[AnyRef]]

          val topic: String = tuple( 0 ).asInstanceOf[Any] match {
            case s: String => s
            case unknown => throw PickleException( s"failed to parse topic [$unknown] type not handled [${unknown.getClass}]" )
          }

          val unixEpochSeconds: Long = dp(0).asInstanceOf[Any] match {
            case l: Long => l
            case i: Int => i.toLong
            case bi: BigInteger => bi.longValue
            case d: Double => d.toLong
            case unknown => throw PickleException( s"failed to parse timestamp [$unknown] type not handled [${unknown.getClass}]" )
          }

          val v: Double = dp(1).asInstanceOf[Any] match {
            case d: Double => d
            case f: Float => f.toDouble
            case bd: BigDecimal => bd.doubleValue
            case i: Int => i.toDouble
            case l: Long => l.toDouble
            case unknown => throw PickleException( s"failed to parse value [$unknown] type not handled [${unknown.getClass}]" )
          }

          Metric( topic = topic, timestamp = new joda.DateTime(unixEpochSeconds * 1000L), value = v )
        }

        val result = metrics groupBy { _.topic } map { case (t, ms) =>
          val points = ms map { m => DataPoint( timestamp = m.timestamp, value = m.value ) }
          TimeSeries( topic = t, points = points.toIndexedSeq )
        }

        result.toList
      }
    }
  }

  case object MessagePackProtocol extends SerializationProtocol {

    override def framingFlow(maximumFrameLength: Int): Flow[ByteString, ByteString, Unit] = ???

    override def toDataPoints( bytes: ByteString ): List[TimeSeries] = {
      import org.velvia.MsgPack
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      import com.fasterxml.jackson.databind.SerializationFeature
      org.json4s.jackson.JsonMethods.mapper.configure( SerializationFeature.CLOSE_CLOSEABLE, false )

      import org.velvia.msgpack.Json4sCodecs._
      logger info "unpacking..."
      val payload = MsgPack unpack bytes.toArray
      logger info "...unpacked"
      logger error s"payload class: ${payload.getClass}"
      logger error s"payload: $payload"
      throw new IllegalArgumentException( s"UNPACKED: [[${payload}]]" )
//      List.empty[TimeSeries]
    }
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


  def status[T]( label: String ): Flow[T, T, Unit] = Flow[T].map { e => logger info s"\n$label:${e.toString}"; e }

  def getConfiguration( usage: Settings, config: Config ): (InetAddress, Int, Int, SerializationProtocol, FiniteDuration, Seq[OutlierPlan]) = {
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
          case "pickle" => PickleProtocol
          case _ => PickleProtocol
        }
      } else {
        PickleProtocol
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
