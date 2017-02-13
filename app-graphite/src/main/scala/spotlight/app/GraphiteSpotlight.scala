package spotlight.app

import java.net.{ InetSocketAddress, Socket }

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import akka.{ Done, NotUsed }
import akka.actor.ActorRef
import akka.stream.scaladsl._
import akka.stream._
import akka.util.{ ByteString, Timeout }
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{ Logger, StrictLogging }
import demesne.BoundedContext
import kamon.Kamon
import nl.grons.metrics.scala.MetricName
import peds.akka.metrics.Instrumented
import peds.akka.stream.StreamMonitor
import spotlight.analysis.OutlierScoringModel
import spotlight.{ Settings, Spotlight, SpotlightContext }
import spotlight.model.outlier._
import spotlight.model.timeseries.{ TimeSeries, Topic }
import spotlight.publish.{ GraphitePublisher, LogPublisher }

/** Created by rolfsd on 1/12/16.
  */
object GraphiteSpotlight extends Instrumented with StrictLogging {
  override lazy val metricBaseName: MetricName = {
    import peds.commons.util._
    MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  }

  override protected val logger: Logger = Logger( LoggerFactory.getLogger( "GraphiteSpotlight" ) )
  val ActorSystemName = "Spotlight"

  def main( args: Array[String] ): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val context = {
      SpotlightContext
        .builder
        .set( SpotlightContext.Name, ActorSystemName )
        //      .set( SpotlightContext.StartTasks, Set( SharedLeveldbStore.start( true ), Spotlight.kamonStartTask ) )
        .set( SpotlightContext.Timeout, Timeout( 30.seconds ) )
        .build()
    }

    Spotlight( context, finishSubscriberOnComplete = false )
      .run( args )
      .foreach {
        case ( boundedContext, configuration, flow ) ⇒
          execute( flow )( boundedContext, configuration ) onComplete {
            case Success( b ) ⇒ logger.info( "Server bindings have completed" )

            case Failure( ex ) ⇒ {
              logger.error( "Server could not bind to source", ex )
              boundedContext.shutdown()
              Kamon.shutdown()
            }
          }
      }
  }

  private object WatchPoints {
    val Framing = 'framing
    val Intake = 'intake
    val PublishBuffer = Symbol( "publish.buffer" )
  }

  def execute(
    scoring: Flow[TimeSeries, Outliers, NotUsed]
  )(
    implicit
    boundedContext: BoundedContext,
    settings: Settings
  ): Future[Done] = {
    logger.info(
      s"""
        |\nServer bound using the following configuration:
        |\tTCP-In Buffer Size   : ${settings.tcpInboundBufferSize}
        |\tWorkflow Buffer Size : ${settings.workflowBufferSize}
        |\tDetect Timeout       : ${settings.detectionBudget.toCoarsest}
        |\tplans                : [${settings.plans.zipWithIndex.map { case ( p, i ) ⇒ f"${i}%2d: ${p}" }.mkString( "\n", "\n", "\n" )}]
      """.stripMargin
    )

    implicit val system = boundedContext.system
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings( system ) withSupervisionStrategy Spotlight.supervisionDecider
    )
    val address = settings.sourceAddress

    val detectionFlow = {
      inlet
        .via( scoring )
        .via( outlet )
    }

    import spotlight.app.GraphiteSpotlight.{ WatchPoints ⇒ GS }
    import spotlight.analysis.OutlierScoringModel.{ WatchPoints ⇒ OSM }
    StreamMonitor.set( GS.Framing, GS.Intake, OSM.ScoringPlanned, OSM.PlanBuffer, GS.PublishBuffer, OSM.ScoringUnrecognized )

    val connections = Tcp().bind( address.getHostName, address.getPort )
    connections runForeach { connection ⇒
      logger.info( "New connection from: {}", connection.remoteAddress )
      connection handleWith detectionFlow
    }
  }

  def inlet( implicit boundedContext: BoundedContext, settings: Settings ): Flow[ByteString, TimeSeries, NotUsed] = {
    import StreamMonitor._
    import WatchPoints._
    settings.protocol.framingFlow( settings.maxFrameLength ).watchSourced( Framing )
      .via( Flow[ByteString].buffer( settings.tcpInboundBufferSize, OverflowStrategy.backpressure ).watchFlow( Intake ) ) //todo fix StreamMonitor flow measurement so able to watch .buffer(..) and not need Flow structure
      .via( settings.protocol.unmarshalTimeSeriesData )
  }

  def outlet( implicit boundedContext: BoundedContext, settings: Settings ): Flow[Outliers, ByteString, NotUsed] = {
    val graph = GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      import StreamMonitor._
      import WatchPoints._

      val egressBroadcast = b.add( Broadcast[Outliers]( outputPorts = 2, eagerCancel = true ) )
      val publishBuffer = b.add(
        Flow[Outliers].buffer( 1000, OverflowStrategy.backpressure ).watchFlow( PublishBuffer )
      )
      val publish = b.add( publishOutliers( settings.graphiteAddress ) )
      val tcpOut = b.add( Flow[Outliers].map { _ ⇒ ByteString() } )

      egressBroadcast ~> tcpOut
      egressBroadcast ~> publishBuffer ~> publish

      FlowShape( egressBroadcast.in, tcpOut.out )
    }

    Flow.fromGraph( graph ).withAttributes( ActorAttributes supervisionStrategy Spotlight.supervisionDecider )
  }

  def publishOutliers( graphiteAddress: Option[InetSocketAddress] ): Sink[Outliers, ActorRef] = {
    val props = graphiteAddress map { address ⇒
      GraphitePublisher.props {
        new GraphitePublisher with GraphitePublisher.PublishProvider {
          // cannot use vals; compiler is setting to null regardless of value.
          override lazy val maxOutstanding: Int = 1000000
          override lazy val metricBaseName = MetricName( classOf[GraphitePublisher] )
          override lazy val destinationAddress: InetSocketAddress = address
          override lazy val batchSize: Int = 1000
          override def createSocket( address: InetSocketAddress ): Socket = {
            new Socket( destinationAddress.getAddress, destinationAddress.getPort )
          }
          override def publishingTopic( p: AnalysisPlan, t: Topic ): Topic = {
            OutlierScoringModel.OutlierMetricPrefix + super.publishingTopic( p, t )
          }
        }
      }
    } getOrElse {
      LogPublisher.props
    }

    Sink.actorSubscriber[Outliers]( props withDispatcher GraphitePublisher.DispatcherPath ).named( "graphite" )
  }
}
