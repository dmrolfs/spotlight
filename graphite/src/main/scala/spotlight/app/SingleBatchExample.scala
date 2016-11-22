package spotlight.app

import scala.concurrent.Future
import akka.NotUsed
import akka.actor.{Actor, ActorSystem, DeadLetter, Props}
import akka.event.LoggingReceive
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source}
import akka.stream._
import akka.util.ByteString
import com.typesafe.scalalogging.{Logger, StrictLogging}
import nl.grons.metrics.scala.MetricName
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.json4s._
import org.json4s.jackson.JsonMethods
import peds.akka.metrics.Instrumented
import demesne.BoundedContext
import peds.akka.stream.StreamMonitor
import spotlight.model.outlier.{Outliers, SeriesOutliers}
import spotlight.model.timeseries.{DataPoint, ThresholdBoundary, TimeSeries}
import spotlight.protocol.GraphiteSerializationProtocol
import spotlight.stream.{Bootstrap, BootstrapContext, Settings}


case class OutlierInfo( metricName: String, metricWebId: String, metricSegment: String )

case class OutlierTimeSeriesObject( timeStamp: DateTime, value: Double )

case class Threshold( timeStamp: DateTime, ceiling: Option[Double], expected: Option[Double], floor: Option[Double] )

case class SimpleFlattenedOutlier(
  algorithm: Symbol,
  outliers: Seq[OutlierTimeSeriesObject],
  threshold: Seq[Threshold],
  topic: String,
  metricName: String,
  webId: String,
  segment: String
)


object DeadListenerActor {
  def props: Props = Props( new DeadListenerActor )
}

class DeadListenerActor extends Actor  {

  val logger = Logger(LoggerFactory.getLogger("DeadListenerActor"))
  override def receive: Receive = LoggingReceive {
    case DeadLetter( m, s, r ) => {
      logger.debug("sender: {}",s)
      logger.debug("recipient: {}",r)
      logger.debug("message: {}",m.toString)
    }
  }
}


/**
  * Created by rolfsd on 11/17/16.
  */
object SingleBatchExample extends Instrumented with StrictLogging {
  def main( args: Array[String] ): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val logger = Logger( LoggerFactory.getLogger("Application") )
    logger.info( "Starting Application Up" )

    implicit val actorSystem = ActorSystem( "Spotlight" )
    val deadListener = actorSystem.actorOf( DeadListenerActor.props, "dead-listener" )
    actorSystem.eventStream.subscribe( deadListener, classOf[DeadLetter] )

    implicit val materializer = ActorMaterializer( ActorMaterializerSettings(actorSystem) )

    start( args )
    .map { results =>
      logger.info( "Example completed successfully and found {} result(s)", results.size )
      results
    }
    .onComplete {
      case result => {
        logger.info( "Example processing completed with: [{}]", result )
        actorSystem.terminate()
      }
    }
  }


  override lazy val metricBaseName: MetricName = {
    import peds.commons.util._
    MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  }

  override protected val logger: Logger = Logger( LoggerFactory.getLogger("DetectionFlow") )

  def start( args: Array[String] )( implicit system: ActorSystem, materializer: Materializer ): Future[Seq[SimpleFlattenedOutlier]] = {

    logger.debug("starting the detecting flow logic ")


    import scala.concurrent.ExecutionContext.Implicits.global

    val context = {
      BootstrapContext
      .builder
      .set( BootstrapContext.Name, "DetectionFlow" )
      .set( BootstrapContext.StartTasks, Set( SharedLeveldbStore.start(true) /*, Bootstrap.kamonStartTask*/ ) )
      .set( BootstrapContext.System, Some(system) )
      .build()
    }

    Bootstrap( context )
    .run( args )
    .map { e => logger.debug( "bootstrapping process..." ); e }
    .flatMap { case ( boundedContext, configuration, flow ) =>
      logger.debug("process bootstrapped. processing data...")

      sourceData()
      .limit( 100 )
      .map { e => logger.debug("after the source ingestion step"); e }
      .map { m => akka.util.ByteString( m.getBytes ) }
      .via( detectionWorkflow(boundedContext,configuration,flow) )
      .runWith( Sink.seq )
    }
  }

  def sourceData(): Source[String, NotUsed] = {
    Source(
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762517,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762518,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762519,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762520,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762521,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762522,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762523,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762524,"value":200.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762525,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762526,"value":2.0}]}""" ::
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1467762527,"value":2.0}]}""" ::
      Nil
    )
  }


  def detectionWorkflow(
    context: BoundedContext,
    configuration: Settings,
    scoring: Flow[TimeSeries, Outliers, NotUsed]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[ByteString, SimpleFlattenedOutlier, NotUsed] = {
    val conf = configuration

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      import peds.akka.stream.StreamMonitor._

      def watch[T]( label: String ): Flow[T, T, NotUsed] = Flow[T].map { e => logger.debug( s"${label}: ${e}" ); e }

      val intakeBuffer = b.add(
        Flow[ByteString]
        .buffer(conf.tcpInboundBufferSize, OverflowStrategy.backpressure)
        .watchFlow( 'intake )
      )

      val timeSeries = b.add(
        Flow[ByteString]
        .via( watch("unpacking") )
        .via( unmarshalTimeSeriesData )
        .via( watch("unpacked") )
        .watchFlow( 'timeseries )
      )

      val score = scoring.watchFlow( 'scoring )

      //todo remove after working
      val publishBuffer = b.add(
        Flow[Outliers]
        .via(watch("spotlightoutliers"))
        .buffer(1000, OverflowStrategy.backpressure)
        .watchFlow( 'publish )
      )

      val filterOutliers : FlowShape[Outliers,SeriesOutliers] = b.add(
        Flow[Outliers]
        .map { m => logger.info( "FILTER:BEFORE class:[{}] message:[{}]", m.getClass.getCanonicalName, m ); m }
        .collect {
          case s: SeriesOutliers => s
        }
        .map { m => logger.info( "FILTER:AFTER class:[{}] message:[{}]", m.getClass.getCanonicalName, m ); m }
        .watchFlow( 'filter )
     )

      val flatter: Flow[SeriesOutliers, List[SimpleFlattenedOutlier], NotUsed] = {
        Flow[SeriesOutliers]
        .map(s => flattenObject(s))
        .watchFlow( 'flatter )
      }

      val flatterFlow: FlowShape[SeriesOutliers, List[SimpleFlattenedOutlier]] = b.add(flatter)

      val unwrap = b.add(
        Flow[List[SimpleFlattenedOutlier]]
        .mapConcat(identity)
        .map { o => logger.info( "RESULT: {}", o ); o }
        .watchFlow('unwrap)
      )

      intakeBuffer ~> timeSeries ~> score ~> publishBuffer ~> filterOutliers ~> flatterFlow ~> unwrap

      import spotlight.stream.OutlierScoringModel.{ WatchPoints => OSM }
      import spotlight.analysis.outlier.PlanCatalog.{ WatchPoints => C }
      StreamMonitor.set(
        'intake,
        'scoring,
        OSM.Catalog,
        C.Intake,
        C.Collector,
        C.Outlet,
        'publish,
        'filter,
//        'flatter,
//        'unwrap,
        OSM.ScoringUnrecognized
      )

      FlowShape( intakeBuffer.in, unwrap.out )
    }

    Flow.fromGraph( graph ).withAttributes( ActorAttributes.supervisionStrategy(workflowSupervision) )
  }

  val workflowSupervision: Supervision.Decider = {
    case ex => {
      logger.info("Error caught by Supervisor:", ex)
      Supervision.Restart
    }
  }

  def flatten: Flow[SeriesOutliers , List[SimpleFlattenedOutlier],NotUsed] = {
    Flow[SeriesOutliers ]
    .map[List[SimpleFlattenedOutlier]](flattenObject)
  }

  def flattenObject(outlier: SeriesOutliers): List[SimpleFlattenedOutlier] = {
    val details = parseTopic( outlier.topic.name )
    val list = {
      outlier.algorithms.toList.map{ a =>
        SimpleFlattenedOutlier(
          algorithm = a,
          outliers = parseOutlierObject(outlier.outliers),
          threshold = parseThresholdBoundaries(outlier.thresholdBoundaries(a)) ,
          topic = outlier.topic.name,
          metricName = details.metricName,
          webId = details.metricWebId,
          segment = details.metricSegment
        )
      }
    }
    list
  }

  def parseThresholdBoundaries(thresholdBoundaries: Seq[ThresholdBoundary]) : Seq[Threshold] = {
    thresholdBoundaries map { a => Threshold(a.timestamp, a.ceiling, a.expected, a.floor ) }
  }

  def parseOutlierObject(dataPoints: Seq[DataPoint]) : Seq[OutlierTimeSeriesObject] = {
    dataPoints map { a => OutlierTimeSeriesObject( a.timestamp, a.value ) }
  }

  def parseTopic(topic: String) : OutlierInfo = {
    val splits = topic.split("""\.""")
    val metricType = splits(1)
    val webId = splits(2).concat( "." ).concat( splits(3).split("_")(0) )
    val segment = splits(3).split( "_" )(1)
    OutlierInfo( metricType, webId, segment )
  }

  def unmarshalTimeSeriesData: Flow[ByteString, TimeSeries, NotUsed] = {
    Flow[ByteString]
    .mapConcat { toTimeSeries }
    .withAttributes( ActorAttributes.supervisionStrategy(GraphiteSerializationProtocol.decider) )
  }

  def toTimeSeries( bytes: ByteString ): List[TimeSeries] = {
    import spotlight.model.timeseries._

    for {
      JObject( obj ) <- JsonMethods parse bytes.utf8String
      JField( "topic", JString(topic) ) <- obj
      JField( "points", JArray(points) ) <- obj
    } yield {
      val datapoints = for {
        JObject( point ) <- points
        JField( "timestamp", JInt(ts) ) <- point
        JField( "value", JDouble(v) ) <- point
      } yield DataPoint( new DateTime( ts.toLong ), v )

      TimeSeries.apply( topic, datapoints )
    }
  }
}
