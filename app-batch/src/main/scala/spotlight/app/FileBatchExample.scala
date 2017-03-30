package spotlight.app

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import akka.NotUsed
import akka.actor.{ Actor, ActorSystem, DeadLetter, Props }
import akka.event.LoggingReceive
import akka.stream.scaladsl.{ FileIO, Flow, Framing, GraphDSL, Sink, Source }
import akka.stream._
import akka.util.ByteString
import net.ceedubs.ficus.Ficus._
import com.persist.logging._

import scalaz.{ -\/, \/, \/- }
import nl.grons.metrics.scala.MetricName
import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.JsonMethods
import omnibus.akka.metrics.Instrumented
import omnibus.akka.stream.StreamMonitor
import demesne.BoundedContext
import omnibus.akka.stream.Limiter
import omnibus.commons.TryV
import spotlight.{ Settings, Spotlight, SpotlightContext }
import spotlight.analysis.DetectFlow
import spotlight.model.outlier._
import spotlight.model.timeseries.{ DataPoint, ThresholdBoundary, TimeSeries }
import spotlight.protocol.GraphiteSerializationProtocol

/** Created by rolfsd on 11/17/16.
  */
object FileBatchExample extends Instrumented with ClassLogging {
  def main( args: Array[String] ): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import SpotlightContext.{ Builder ⇒ B }

    val context = {
      SpotlightContext.Builder
        .builder
        .set( B.Arguments, args )
        //      .set( SpotlightContext.StartTasks, Set( /*SharedLeveldbStore.start(true), Spotlight.kamonStartTask*/ ) )
        //      .set( SpotlightContext.System, Some( system ) )
        .build()
    }

    //    implicit val actorSystem = ActorSystem( "Spotlight" )
    //    startLogging( actorSystem )
    //    log.info( "Starting Application Up" )

    val deadListener = context.system.actorOf( DeadListenerActor.props, "dead-listener" )
    context.system.eventStream.subscribe( deadListener, classOf[DeadLetter] )

    start( context )
    //      .onComplete {
    //        case Success( results ) ⇒ {
    //          println( "\n\nAPP:  ********************************************** " )
    //          println( s"\nAPP:${count.get()} batch completed finding ${results.size} outliers:" )
    //          results.zipWithIndex foreach { case ( o, i ) ⇒ println( s"${i + 1}: ${o}" ) }
    //          println( "APP:  **********************************************\n\n" )
    //          context.terminate()
    //        }
    //
    //        case Failure( ex ) ⇒ {
    //          log.error( Map( "@msg" → "batch finished with error", "count" → count.get() ), ex )
    //          println( "\n\nAPP:  ********************************************** " )
    //          println( s"\nAPP: ${count.get()} batch completed with ERROR: ${ex}" )
    //          println( "APP:  **********************************************\n\n" )
    //          context.terminate()
    //        }
    //      }
  }

  //  case class OutlierInfo( metricName: String, metricWebId: String, metricSegment: String )

  case class OutlierTimeSeriesObject( timeStamp: DateTime, value: Double )

  //  case class Threshold( timeStamp: DateTime, ceiling: Option[Double], expected: Option[Double], floor: Option[Double] )

  case class SimpleFlattenedOutlier(
    algorithm: String,
    outliers: Seq[OutlierTimeSeriesObject],
    threshold: Seq[ThresholdBoundary],
    topic: String
  )

  object DeadListenerActor {
    def props: Props = Props( new DeadListenerActor )
  }

  class DeadListenerActor extends Actor with ActorLogging {
    override def receive: Receive = LoggingReceive {
      case DeadLetter( m, s, r ) ⇒ {
        log.debug(
          Map(
            "@msg" → "dead letter received",
            "sender" → sender.path.name,
            "recipient" → r.path.name,
            "message" → m.toString
          )
        )
      }
    }
  }

  override lazy val metricBaseName: MetricName = {
    import omnibus.commons.util._
    MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  }

  object WatchPoints {
    val DataSource = 'source
    val Intake = 'intake
    val Rate = 'rate
    val Scoring = 'scoring
    val Publish = 'publish
    val Results = 'results
  }

  val count: AtomicInteger = new AtomicInteger( 0 )

  //todo
  def start( context: SpotlightContext ): Future[Seq[SimpleFlattenedOutlier]] = {

    log.debug( "starting the detecting flow logic" )

    import scala.concurrent.ExecutionContext.Implicits.global

    Spotlight()
      .run( context )
      .map { e ⇒ log.debug( "bootstrapping process..." ); e }
      .flatMap {
        case ( boundedContext, ctx, None ) ⇒ {
          log.info( Map( "@msg" → "spotlight node started", "role" → ctx.settings.role.entryName ) )
          Future.successful( Seq.empty[SimpleFlattenedOutlier] )
        }

        case ( boundedContext, ctx, Some( scoring ) ) ⇒
          log.debug( "process bootstrapped. processing data..." )

          import StreamMonitor._
          import WatchPoints._
          import spotlight.analysis.OutlierScoringModel.{ WatchPoints ⇒ OSM }
          import spotlight.analysis.PlanCatalog.{ WatchPoints ⇒ C }
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
            Results //,
          //        OSM.ScoringUnrecognized
          )

          implicit val system = ctx.system
          implicit val materializer = ActorMaterializer( ActorMaterializerSettings( system ) )

          val publish = Flow[SimpleFlattenedOutlier].buffer( 10, OverflowStrategy.backpressure ).watchFlow( Publish )

          val process = sourceData( ctx.settings )
            .via( Flow[String].watchSourced( DataSource ) )
            //      .via( Flow[String].buffer( 10, OverflowStrategy.backpressure ).watchSourced( Data ) )
            .via( detectionWorkflow( boundedContext, ctx.settings, scoring ) )
            .via( publish )
            .runWith( Sink.seq )

          process
            .map { results ⇒
              log.info(
                Map(
                  "@msg" → "APP: Example processed records successfully and found result(s)",
                  "nr-records" → count.get().toString,
                  "nr-results" → results.size.toString
                )
              )

              results
            }
            .onComplete {
              case Success( results ) ⇒ {
                println( "\n\nAPP:  ********************************************** " )
                println( s"\nAPP:${count.get()} batch completed finding ${results.size} outliers:" )
                results.zipWithIndex foreach { case ( o, i ) ⇒ println( s"${i + 1}: ${o}" ) }
                println( "APP:  **********************************************\n\n" )
                context.terminate()
              }

              case Failure( ex ) ⇒ {
                log.error( Map( "@msg" → "batch finished with error", "count" → count.get() ), ex )
                println( "\n\nAPP:  ********************************************** " )
                println( s"\nAPP: ${count.get()} batch completed with ERROR: ${ex}" )
                println( "APP:  **********************************************\n\n" )
                context.terminate()
              }
            }

          process
      }
  }

  def sourceData( settings: Settings ): Source[String, Future[IOResult]] = {
    val data = settings.args.lastOption getOrElse "source.txt"
    log.info( Map( "@msg" → "using data file", "file" → Paths.get( data ).toString ) )

    FileIO
      .fromPath( Paths.get( data ) )
      .via( Framing.delimiter( ByteString( "\n" ), maximumFrameLength = 1024 ) )
      .map { _.utf8String }

    //    import better.files._
    //
    //    val file = File( data )
    //    Source.fromIterator( () => file.lines.to[Iterator] )
  }

  def rateLimitFlow( parallelism: Int, refreshPeriod: FiniteDuration )( implicit system: ActorSystem ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    //    val limiterRef = system.actorOf( Limiter.props( parallelism, refreshPeriod, parallelism ), "rate-limiter" )
    //    val limitDuration = 9.minutes // ( settings.detectionBudget * 1.1 )
    //    val limitWait = FiniteDuration( limitDuration._1, limitDuration._2 )
    //    Limiter.limitGlobal[TimeSeries](limiterRef, limitWait)( system.dispatcher )

    Flow[TimeSeries].map { identity }

    //    Flow[TimeSeries].throttle( parallelism, refreshPeriod, parallelism, akka.stream.ThrottleMode.shaping )
  }

  def detectionWorkflow(
    context: BoundedContext,
    settings: Settings,
    scoring: DetectFlow
  )(
    implicit
    system: ActorSystem,
    materializer: Materializer
  ): Flow[String, SimpleFlattenedOutlier, NotUsed] = {
    val graph = GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      import omnibus.akka.stream.StreamMonitor._

      def watch[T]( label: String ): Flow[T, T, NotUsed] = Flow[T].map { e ⇒ log.info( Map( label → e.toString ) ); e }

      val intakeBuffer = b.add(
        Flow[String]
          .buffer( settings.tcpInboundBufferSize, OverflowStrategy.backpressure )
          .watchFlow( WatchPoints.Intake )
      )

      val timeSeries = b.add(
        Flow[String]
          .via( unmarshalTimeSeriesData )
          .map { ts ⇒ count.incrementAndGet(); ts }
      )

      val limiter = b.add( rateLimitFlow( settings.parallelism, 25.milliseconds ).watchFlow( WatchPoints.Rate ) )
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
          .collect { case s: SeriesOutliers ⇒ s } //.watchFlow( WatchPoints.Results )
      )

      val flatter = b.add(
        Flow[SeriesOutliers]
          .map { s ⇒
            flattenObject( s ) match {
              case \/-( f ) ⇒ f
              case -\/( ex ) ⇒ {
                log.error( Map( "@msg" → "Failure: flatter.flattenObject", "series-outlier" → s.toString ), ex )
                throw ex
              }
            }
          }
      )

      val unwrap = b.add(
        Flow[List[SimpleFlattenedOutlier]]
          .mapConcat( identity )
      // .map { o => logger.info( "RESULT: {}", o ); o }
      )

      intakeBuffer ~> timeSeries ~> limiter ~> score ~> /*publishBuffer ~>*/ filterOutliers ~> flatter ~> unwrap

      FlowShape( intakeBuffer.in, unwrap.out )
    }

    Flow.fromGraph( graph ).withAttributes( ActorAttributes.supervisionStrategy( workflowSupervision ) )
  }

  val workflowSupervision: Supervision.Decider = {
    case ex ⇒ {
      log.info( "Error caught by Supervisor:", ex )
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
      outlier.algorithms.toList.map { a ⇒
        val o = parseOutlierObject( outlier.outliers )
        val t = outlier.thresholdBoundaries.get( a ) getOrElse {
          outlier.source.points.map { dp ⇒ ThresholdBoundary.empty( dp.timestamp ) }
        }

        SimpleFlattenedOutlier( algorithm = a, outliers = o, threshold = t, topic = outlier.topic.toString )
      }
    }
  }

  //  def parseThresholdBoundaries( thresholdBoundaries: Seq[ThresholdBoundary] ) : Seq[Threshold] = trace.briefBlock(s"parseThresholdBoundaries(${thresholdBoundaries})"){
  //    thresholdBoundaries map { a => Threshold(a.timestamp, a.ceiling, a.expected, a.floor ) }
  //  }

  def parseOutlierObject( dataPoints: Seq[DataPoint] ): Seq[OutlierTimeSeriesObject] = {
    dataPoints map { a ⇒ OutlierTimeSeriesObject( a.timestamp, a.value ) }
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
      .mapConcat { s ⇒
        toTimeSeries( s ) match {
          case \/-( tss ) ⇒ tss
          case -\/( ex ) ⇒ {
            log.error( Map( "@msg" → "Failure: unmarshalTimeSeries.toTimeSeries", "time-series" → s.toString ), ex )
            throw ex
          }
        }
      }
      //    .map { ts => logger.info( "unmarshalled time series: [{}]", ts ); ts }
      .withAttributes( ActorAttributes.supervisionStrategy( GraphiteSerializationProtocol.decider ) )
  }

  def toTimeSeries( bytes: String ): TryV[List[TimeSeries]] = {
    import spotlight.model.timeseries._

    \/ fromTryCatchNonFatal {
      for {
        JObject( obj ) ← JsonMethods parse bytes
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
}
