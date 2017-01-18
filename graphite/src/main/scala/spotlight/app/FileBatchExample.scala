package spotlight.app

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import akka.NotUsed
import akka.actor.{Actor, ActorSystem, DeadLetter, Props}
import akka.event.LoggingReceive
import akka.stream.scaladsl.{FileIO, Flow, Framing, GraphDSL, Keep, Sink, Source}
import akka.stream._
import akka.util.ByteString

import scalaz.{-\/, \/, \/-}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import nl.grons.metrics.scala.MetricName
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.json4s._
import org.json4s.jackson.JsonMethods
import peds.akka.metrics.Instrumented
import peds.akka.stream.StreamMonitor
import demesne.BoundedContext
import peds.akka.stream.Limiter
import peds.commons.TryV
import spotlight.{Settings, Spotlight, SpotlightContext}
import spotlight.analysis.outlier.DetectFlow
import spotlight.model.outlier._
import spotlight.model.timeseries.{DataPoint, ThresholdBoundary, TimeSeries}
import spotlight.protocol.GraphiteSerializationProtocol


/**
  * Created by rolfsd on 11/17/16.
  */
object FileBatchExample extends Instrumented with StrictLogging {
  def main( args: Array[String] ): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val logger = Logger( LoggerFactory.getLogger("Application") )
    logger.info( "Starting Application Up" )
    logger.info( "spotlight build info: {}", spotlight.BuildInfo )
    logger.info( "demesne build info: {}", demesne.BuildInfo )

    implicit val actorSystem = ActorSystem( "Spotlight" )
    val deadListener = actorSystem.actorOf( DeadListenerActor.props, "dead-listener" )
    actorSystem.eventStream.subscribe( deadListener, classOf[DeadLetter] )

    implicit val materializer = ActorMaterializer( ActorMaterializerSettings(actorSystem) )

    start( args )
    .map { results =>
      logger.info("APP: Example processed {} records successfully and found {} result(s)", count.get().toString, results.size.toString )
      results
    }
    .onComplete {
      case Success( results ) => {
        println( "\n\nAPP:  ********************************************** " )
        println( s"\nAPP:${count.get()} batch completed finding ${results.size} outliers:" )
        results.zipWithIndex foreach { case (o, i) => println( s"${i+1}: ${o}") }
        println( "APP:  **********************************************\n\n" )
        actorSystem.terminate()
      }

      case Failure( ex ) => {
        println( "\n\nAPP:  ********************************************** " )
        println( s"\nAPP: ${count.get()} batch completed with ERROR: ${ex}" )
        println( "APP:  **********************************************\n\n" )
        actorSystem.terminate()
      }
    }
  }


//  case class OutlierInfo( metricName: String, metricWebId: String, metricSegment: String )

  case class OutlierTimeSeriesObject( timeStamp: DateTime, value: Double )

//  case class Threshold( timeStamp: DateTime, ceiling: Option[Double], expected: Option[Double], floor: Option[Double] )

  case class SimpleFlattenedOutlier(
    algorithm: Symbol,
    outliers: Seq[OutlierTimeSeriesObject],
    threshold: Seq[ThresholdBoundary],
    topic: String
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


  override lazy val metricBaseName: MetricName = {
    import peds.commons.util._
    MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  }

  override protected val logger: Logger = Logger( LoggerFactory.getLogger("DetectionFlow") )


  object WatchPoints {
    val DataSource = 'source
    val Intake = 'intake
    val Rate = 'rate
    val Scoring = 'scoring
    val Publish = 'publish
    val Results = 'results
  }

  val count: AtomicInteger = new AtomicInteger( 0 )

  def start( args: Array[String] )( implicit system: ActorSystem, materializer: Materializer ): Future[Seq[SimpleFlattenedOutlier]] = {

    logger.debug("starting the detecting flow logic ")


    import scala.concurrent.ExecutionContext.Implicits.global

    val context = {
      SpotlightContext
      .builder
      .set( SpotlightContext.Name, "DetectionFlow" )
      //      .set( SpotlightContext.StartTasks, Set( /*SharedLeveldbStore.start(true), Spotlight.kamonStartTask*/ ) )
      .set( SpotlightContext.System, Some( system ) )
      .build()
    }

    Spotlight( context )
    .run( args )
    .map { e => logger.debug( "bootstrapping process..." ); e }
    .flatMap { case ( boundedContext, settings, scoring ) =>
      logger.debug("process bootstrapped. processing data...")

      import StreamMonitor._
      import WatchPoints._
      import spotlight.analysis.outlier.OutlierScoringModel.{ WatchPoints => OSM }
      import spotlight.analysis.outlier.PlanCatalog.{ WatchPoints => C }
      StreamMonitor.set(
        DataSource,
        Intake,
        Rate,
//       'timeseries,
//        'blockPriors,
//        'preBroadcast,
//        OSM.ScoringPlanned, // keep later?
//        'passPlanned,
//        'passUnrecognizedPreFilter,
//        OSM.ScoringUnrecognized,
        'regulator,
//        OSM.PlanBuffer,
        OSM.Catalog,
//        C.Outlet,
//        Publish,
//       'filterOutliers,
        Results//,
//        OSM.ScoringUnrecognized
      )

      val publish = Flow[SimpleFlattenedOutlier].buffer( 10, OverflowStrategy.backpressure ).watchFlow( Publish )

      sourceData( settings )
      .via( Flow[String].watchSourced( DataSource ) )
//      .via( Flow[String].buffer( 10, OverflowStrategy.backpressure ).watchSourced( Data ) )
      .via( detectionWorkflow(boundedContext, settings, scoring) )
      .via( publish )
      .runWith( Sink.seq )
    }
  }

  def sourceData( settings: Settings ): Source[String, Future[IOResult]] = {
    val data = settings.args.lastOption getOrElse "source.txt"
    logger.info( "using data file: {}", Paths.get(data) )

    FileIO
    .fromPath( Paths.get( data ) )
    .via( Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024) )
    .map { _.utf8String }

//    import better.files._
//
//    val file = File( data )
//    Source.fromIterator( () => file.lines.to[Iterator] )
  }

  def rateLimitFlow( parallelism: Int, refreshPeriod: FiniteDuration )( implicit system: ActorSystem ): Flow[TimeSeries, TimeSeries, NotUsed] = {
//    val limiterRef = system.actorOf( Limiter.props( parallelism, refreshPeriod, parallelism ), "rate-limiter" )
//    val limitDuration = 9.minutes // ( configuration.detectionBudget * 1.1 )
//    val limitWait = FiniteDuration( limitDuration._1, limitDuration._2 )
//    Limiter.limitGlobal[TimeSeries](limiterRef, limitWait)( system.dispatcher )

    Flow[TimeSeries].map{ identity }

//    Flow[TimeSeries].throttle( parallelism, refreshPeriod, parallelism, akka.stream.ThrottleMode.shaping )
  }

  def detectionWorkflow(
    context: BoundedContext,
    configuration: Settings,
    scoring: DetectFlow
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[String, SimpleFlattenedOutlier, NotUsed] = {
    val conf = configuration

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      import peds.akka.stream.StreamMonitor._

      def watch[T]( label: String ): Flow[T, T, NotUsed] = Flow[T].map { e => logger.info( s"${label}: ${e}" ); e }

      val intakeBuffer = b.add(
        Flow[String]
        .buffer(conf.tcpInboundBufferSize, OverflowStrategy.backpressure)
        .watchFlow( WatchPoints.Intake )
      )

      val timeSeries = b.add(
        Flow[String]
        .via( unmarshalTimeSeriesData )
      )

      val limiter = b.add( rateLimitFlow( configuration.parallelism, 25.milliseconds ).watchFlow( WatchPoints.Rate ) )
      val score = b.add( scoring )

      //todo remove after working
//      val publishBuffer = b.add(
//        Flow[Outliers]
//        .buffer( 10, OverflowStrategy.backpressure )
//        .watchFlow( WatchPoints.Publish )
//      )

      val filterOutliers = b.add(
        Flow[Outliers]
//        .buffer( 10, OverflowStrategy.backpressure ).watchFlow( 'filterOutliers )
        .collect { case s: SeriesOutliers => s }.watchFlow( WatchPoints.Results )
      )

      val flatter = b.add(
        Flow[SeriesOutliers]
        .map { s =>
          flattenObject( s ) match {
            case \/-( f ) => f
            case -\/( ex ) => {
              logger.error( s"Failure: flatter.flattenObject[${s}]:", ex )
              throw ex
            }
          }
        }
      )

      val unwrap = b.add(
        Flow[List[SimpleFlattenedOutlier]]
        .mapConcat(identity)
        .map { o => logger.info( "RESULT: {}", o ); o }
      )

      intakeBuffer ~> timeSeries ~> limiter ~> score ~> /*publishBuffer ~>*/ filterOutliers ~> flatter ~> unwrap

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

//  def flatten: Flow[SeriesOutliers , List[SimpleFlattenedOutlier],NotUsed] = {
//    Flow[SeriesOutliers ]
//    .map[List[SimpleFlattenedOutlier]] { so =>
//      flattenObject( so ) match {
//        case \/-( f ) => f
//        case -\/( ex ) => {
//          logger.error( s"Failure: flatten.flattenObject[${so}]:", ex )
//          throw ex
//        }
//      }
//    }
//  }

  def flattenObject( outlier: SeriesOutliers ): TryV[List[SimpleFlattenedOutlier]] = {
    \/ fromTryCatchNonFatal {
      outlier.algorithms.toList.map{ a =>
        val o = parseOutlierObject( outlier.outliers )
        val t = outlier.thresholdBoundaries.get(a) getOrElse {
          outlier.source.points.map{ dp => ThresholdBoundary.empty(dp.timestamp) }
        }

        SimpleFlattenedOutlier( algorithm = a, outliers = o, threshold = t, topic = outlier.topic.toString )
      }
    }
  }

//  def parseThresholdBoundaries( thresholdBoundaries: Seq[ThresholdBoundary] ) : Seq[Threshold] = trace.briefBlock(s"parseThresholdBoundaries(${thresholdBoundaries})"){
//    thresholdBoundaries map { a => Threshold(a.timestamp, a.ceiling, a.expected, a.floor ) }
//  }

  def parseOutlierObject(dataPoints: Seq[DataPoint]) : Seq[OutlierTimeSeriesObject] = {
    dataPoints map { a => OutlierTimeSeriesObject( a.timestamp, a.value ) }
  }

//  def parseTopic( topic: String ) : TryV[OutlierInfo] = trace.briefBlock( s"parseTopic(${topic})" ) {
//    val result = \/ fromTryCatchNonFatal {
//      val splits = topic.split("""[.-]""")
//      val metricType = splits(0)
//      val webId = splits(1).concat( "." ).concat( splits(2).split("_")(0) )
//      val segment = splits(2).split( "_" )(1)
//      OutlierInfo( metricType, webId, segment )
//    }
//
//    result.leftMap( ex => logger.error( s"PARSE_TOPIC ERROR on [${topic}]", ex ))
//
//    result
//  }

  def unmarshalTimeSeriesData: Flow[String, TimeSeries, NotUsed] = {
    Flow[String]
    .mapConcat { s =>
      toTimeSeries( s ) match {
        case \/-( tss ) => tss
        case -\/( ex ) => {
          logger.error( s"Failure: unmarshalTimeSeries.toTimeSeries[${s}]:", ex )
          throw ex
        }
      }
    }
//    .map { ts => logger.info( "unmarshalled time series: [{}]", ts ); ts }
    .withAttributes( ActorAttributes.supervisionStrategy(GraphiteSerializationProtocol.decider) )
  }

  def toTimeSeries( bytes: String ): TryV[List[TimeSeries]] = {
    import spotlight.model.timeseries._

    \/ fromTryCatchNonFatal {
      for {
        JObject( obj ) <- JsonMethods parse bytes
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
}
