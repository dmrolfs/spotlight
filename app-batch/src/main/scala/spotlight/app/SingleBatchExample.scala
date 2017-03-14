package spotlight.app

import scala.concurrent.Future
import scala.util.{ Failure, Success }
import akka.NotUsed
import akka.actor.{ Actor, ActorSystem, DeadLetter, Props }
import akka.event.LoggingReceive
import akka.stream.scaladsl.{ Flow, GraphDSL, Sink, Source }
import akka.stream._
import akka.util.ByteString
import com.typesafe.scalalogging.{ Logger, StrictLogging }
import nl.grons.metrics.scala.MetricName
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.json4s._
import org.json4s.jackson.JsonMethods
import omnibus.akka.metrics.Instrumented
import demesne.BoundedContext
import omnibus.akka.stream.StreamMonitor
import spotlight.{ Spotlight, SpotlightContext, Settings }
import spotlight.analysis.DetectFlow
import spotlight.infrastructure.SharedLeveldbStore
import spotlight.model.outlier.{ Outliers, SeriesOutliers }
import spotlight.model.timeseries.{ DataPoint, ThresholdBoundary, TimeSeries }
import spotlight.protocol.GraphiteSerializationProtocol

/** Created by rolfsd on 11/17/16.
  */
object SingleBatchExample extends Instrumented with StrictLogging {
  def main( args: Array[String] ): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val logger = Logger( LoggerFactory.getLogger( "Application" ) )
    logger.info( "Starting Application Up" )

    implicit val actorSystem = ActorSystem( "Spotlight" )
    val deadListener = actorSystem.actorOf( DeadListenerActor.props, "dead-listener" )
    actorSystem.eventStream.subscribe( deadListener, classOf[DeadLetter] )

    implicit val materializer = ActorMaterializer( ActorMaterializerSettings( actorSystem ) )

    start( args )
      .map { results ⇒
        logger.info( "Example completed successfully and found {} result(s)", results.size )
        results
      }
      .onComplete {
        case Success( results ) ⇒ {
          println( "\n\n  ********************************************** " )
          println( s"\nbatch completed finding ${results.size} outliers:" )
          results.zipWithIndex foreach { case ( o, i ) ⇒ println( s"${i + 1}: ${o}" ) }
          println( "  **********************************************\n\n" )
          actorSystem.terminate()
        }

        case Failure( ex ) ⇒ {
          println( "\n\n  ********************************************** " )
          println( s"\n batch completed with ERROR: ${ex}" )
          println( "  **********************************************\n\n" )
          actorSystem.terminate()
        }
      }
  }

  case class OutlierInfo( metricName: String, metricWebId: String, metricSegment: String )

  case class OutlierTimeSeriesObject( timeStamp: DateTime, value: Double )

  case class Threshold( timeStamp: DateTime, ceiling: Option[Double], expected: Option[Double], floor: Option[Double] )

  case class SimpleFlattenedOutlier(
    algorithm: String,
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

  class DeadListenerActor extends Actor {

    val logger = Logger( LoggerFactory.getLogger( "DeadListenerActor" ) )
    override def receive: Receive = LoggingReceive {
      case DeadLetter( m, s, r ) ⇒ {
        logger.debug( "sender: {}", s )
        logger.debug( "recipient: {}", r )
        logger.debug( "message: {}", m.toString )
      }
    }
  }

  override lazy val metricBaseName: MetricName = {
    import omnibus.commons.util._
    MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  }

  override protected val logger: Logger = Logger( LoggerFactory.getLogger( "DetectionFlow" ) )

  def start( args: Array[String] )( implicit system: ActorSystem, materializer: Materializer ): Future[Seq[SimpleFlattenedOutlier]] = {

    logger.debug( "starting the detecting flow logic " )

    import scala.concurrent.ExecutionContext.Implicits.global

    val context = {
      SpotlightContext
        .builder
        .set( SpotlightContext.StartTasks, Set( SharedLeveldbStore.start( true ) /*, Spotlight.kamonStartTask*/ ) )
        .set( SpotlightContext.System, Some( system ) )
        .build()
    }

    Spotlight( context )
      .run( args )
      .map { e ⇒ logger.debug( "bootstrapping process..." ); e }
      .flatMap {
        case ( boundedContext, configuration, flow ) ⇒
          logger.debug( "process bootstrapped. processing data..." )

          sourceData()
            .limit( 100 )
            .map { e ⇒ logger.debug( "after the source ingestion step" ); e }
            .map { m ⇒ akka.util.ByteString( m.getBytes ) }
            .via( detectionWorkflow( boundedContext, configuration, flow ) )
            .runWith( Sink.seq )
      }
  }

  def sourceData(): Source[String, NotUsed] = {
    Source(
      """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1499992052518,"value":1.9}]}""" :: // 2017-07-13 17:27:32.518
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1500905822018,"value":2.0}]}""" :: // 2017-07-24 07:17:02.018
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1501766514783,"value":1.91}]}""" :: // 2017-08-03 06:21:54.783
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1501811957165,"value":2.1}]}""" :: // 2017-08-03 18:59:17.165
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1504118229872,"value":2.0}]}""" :: // 2017-08-30 11:37:09.872
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1504249200000,"value":2.01}]}""" :: // 2017-09-01 00:00:00.000
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1506841199999,"value":2.0}]}""" :: // 2017-09-30 23:59:59.999
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1506841200000,"value":2.0}]}""" :: // 2017-10-01 00:00:00.000
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1509950064783,"value":1.92}]}""" :: // 2017-11-05 22:34:24.783
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1510973520333,"value":200.0}]}""" :: // 2017-11-17 18:52:00.333
        """{"topic" : "host.foo.bar.zed_delta.mean","points": [{"timestamp":1511367127872,"value":2.0}]}""" :: // 2017-11-22 08:12:07.872
        Nil
    )
  }

  def detectionWorkflow(
    context: BoundedContext,
    settings: Settings,
    scoring: DetectFlow
  )(
    implicit
    system: ActorSystem,
    materializer: Materializer
  ): Flow[ByteString, SimpleFlattenedOutlier, NotUsed] = {
    val graph = GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      import omnibus.akka.stream.StreamMonitor._

      def watch[T]( label: String ): Flow[T, T, NotUsed] = Flow[T].map { e ⇒ logger.debug( s"${label}: ${e}" ); e }

      val intakeBuffer = b.add(
        Flow[ByteString]
          .buffer( settings.tcpInboundBufferSize, OverflowStrategy.backpressure )
          .watchFlow( 'intake )
      )

      val timeSeries = b.add(
        Flow[ByteString]
          .via( watch( "unpacking" ) )
          .via( unmarshalTimeSeriesData )
          .via( watch( "unpacked" ) )
          .watchFlow( 'timeseries )
      )

      val score = scoring.via( watch( "scoring" ) ).watchFlow( 'scoring )

      //todo remove after working
      val publishBuffer = b.add(
        Flow[Outliers]
          .via( watch( "spotlightoutliers" ) )
          .buffer( 1000, OverflowStrategy.backpressure )
          .watchFlow( 'publish )
      )

      val filterOutliers: FlowShape[Outliers, SeriesOutliers] = b.add(
        Flow[Outliers]
          .map { m ⇒ logger.info( "FILTER:BEFORE class:[{}] message:[{}]", m.getClass.getCanonicalName, m ); m }
          .collect {
            case s: SeriesOutliers ⇒ s
          }
          .map { m ⇒ logger.info( "FILTER:AFTER class:[{}] message:[{}]", m.getClass.getCanonicalName, m ); m }
          .watchFlow( 'filter )
      )

      val flatter: Flow[SeriesOutliers, List[SimpleFlattenedOutlier], NotUsed] = {
        Flow[SeriesOutliers]
          .map( s ⇒ flattenObject( s ) )
          .watchFlow( 'flatter )
      }

      val flatterFlow: FlowShape[SeriesOutliers, List[SimpleFlattenedOutlier]] = b.add( flatter )

      val unwrap = b.add(
        Flow[List[SimpleFlattenedOutlier]]
          .mapConcat( identity )
          .map { o ⇒ logger.info( "RESULT: {}", o ); o }
          .watchFlow( 'unwrap )
      )

      intakeBuffer ~> timeSeries ~> score ~> publishBuffer ~> filterOutliers ~> flatterFlow ~> unwrap

      import spotlight.analysis.OutlierScoringModel.{ WatchPoints ⇒ OSM }
      import spotlight.analysis.PlanCatalog.{ WatchPoints ⇒ C }
      StreamMonitor.set(
        'intake,
        'scoring,
        OSM.Catalog,
        C.Intake,
        C.Collector,
        C.Outlet,
        'publish,
        'filter,
        OSM.ScoringUnrecognized
      )

      FlowShape( intakeBuffer.in, unwrap.out )
    }

    Flow.fromGraph( graph ).withAttributes( ActorAttributes.supervisionStrategy( workflowSupervision ) )
  }

  val workflowSupervision: Supervision.Decider = {
    case ex ⇒ {
      logger.info( "Error caught by Supervisor:", ex )
      Supervision.Restart
    }
  }

  def flatten: Flow[SeriesOutliers, List[SimpleFlattenedOutlier], NotUsed] = {
    Flow[SeriesOutliers]
      .map[List[SimpleFlattenedOutlier]]( flattenObject )
  }

  def flattenObject( outlier: SeriesOutliers ): List[SimpleFlattenedOutlier] = {
    val details = parseTopic( outlier.topic.name )
    val list = {
      outlier.algorithms.toList.map { a ⇒
        SimpleFlattenedOutlier(
          algorithm = a,
          outliers = parseOutlierObject( outlier.outliers ),
          threshold = parseThresholdBoundaries( outlier.thresholdBoundaries( a ) ),
          topic = outlier.topic.name,
          metricName = details.metricName,
          webId = details.metricWebId,
          segment = details.metricSegment
        )
      }
    }
    list
  }

  def parseThresholdBoundaries( thresholdBoundaries: Seq[ThresholdBoundary] ): Seq[Threshold] = {
    thresholdBoundaries map { a ⇒ Threshold( a.timestamp, a.ceiling, a.expected, a.floor ) }
  }

  def parseOutlierObject( dataPoints: Seq[DataPoint] ): Seq[OutlierTimeSeriesObject] = {
    dataPoints map { a ⇒ OutlierTimeSeriesObject( a.timestamp, a.value ) }
  }

  def parseTopic( topic: String ): OutlierInfo = {
    val splits = topic.split( """\.""" )
    val metricType = splits( 1 )
    val webId = splits( 2 ).concat( "." ).concat( splits( 3 ).split( "_" )( 0 ) )
    val segment = splits( 3 ).split( "_" )( 1 )
    OutlierInfo( metricType, webId, segment )
  }

  def unmarshalTimeSeriesData: Flow[ByteString, TimeSeries, NotUsed] = {
    Flow[ByteString]
      .mapConcat { toTimeSeries }
      .withAttributes( ActorAttributes.supervisionStrategy( GraphiteSerializationProtocol.decider ) )
  }

  def toTimeSeries( bytes: ByteString ): List[TimeSeries] = {
    import spotlight.model.timeseries._

    for {
      JObject( obj ) ← JsonMethods parse bytes.utf8String
      JField( "topic", JString( topic ) ) ← obj
      JField( "points", JArray( points ) ) ← obj
    } yield {
      val datapoints = for {
        JObject( point ) ← points
        JField( "timestamp", JInt( ts ) ) ← point
        JField( "value", JDouble( v ) ) ← point
      } yield DataPoint( new DateTime( ts.toLong ), v )

      TimeSeries.apply( topic, datapoints )
    }
  }
}
