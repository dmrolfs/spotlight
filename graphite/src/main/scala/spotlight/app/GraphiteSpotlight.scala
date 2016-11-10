package spotlight.app

import java.net.{InetSocketAddress, Socket}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.NotUsed
import akka.actor.ActorRef
import akka.stream.scaladsl._
import akka.stream._
import akka.util.{ByteString, Timeout}
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{Logger, StrictLogging}
import demesne.BoundedContext
import kamon.Kamon
import nl.grons.metrics.scala.MetricName
import peds.commons.log.Trace
import peds.akka.metrics.Instrumented
import peds.akka.stream.StreamMonitor
import spotlight.model.outlier._
import spotlight.model.timeseries.{TimeSeries, Topic}
import spotlight.publish.{GraphitePublisher, LogPublisher}
import spotlight.stream.{Bootstrap, BootstrapContext, Configuration, OutlierScoringModel}


/**
  * Created by rolfsd on 1/12/16.
  */
object GraphiteSpotlight extends Instrumented with StrictLogging {
  override lazy val metricBaseName: MetricName = {
    import peds.commons.util._
    MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  }

  override protected val logger: Logger = Logger( LoggerFactory.getLogger("GraphiteSpotlight") )
  val trace = Trace[GraphiteSpotlight.type]
  val ActorSystemName = "Spotlight"

  def main( args: Array[String] ): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val context = {
      import spotlight.stream.{ BootstrapContext => BC }
      BootstrapContext
      .builder
      .set( BC.Name, ActorSystemName )
      .set( BC.StartTasks, Set( SharedLeveldbStore.start( true ), Bootstrap.kamonStartTask ) )
      .set( BC.Timeout, Timeout(30.seconds) )
      .build()
    }

    Bootstrap( context )
    .run( args )
    .foreach { case (boundedContext, configuration, flow) =>
      execute( flow )( boundedContext, configuration ) onComplete {
        case Success(b) => logger.info( "Server started, listening on: " + b.localAddress )

        case Failure( ex ) => {
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
    implicit boundedContext: BoundedContext,
    configuration: Configuration
  ): Future[Tcp.ServerBinding] = {
    logger.info(
      s"""
        |\nConnection made using the following configuration:
        |\tTCP-In Buffer Size   : ${configuration.tcpInboundBufferSize}
        |\tWorkflow Buffer Size : ${configuration.workflowBufferSize}
        |\tDetect Timeout       : ${configuration.detectionBudget.toCoarsest}
        |\tplans                : [${configuration.plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]
      """.stripMargin
    )

    implicit val system = boundedContext.system
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings( system ) withSupervisionStrategy Bootstrap.supervisionDecider
    )
    val address = configuration.sourceAddress
    val connection = Tcp().bind( address.getHostName, address.getPort )
    val sink = Sink.foreach[Tcp.IncomingConnection] { connection =>
      val detectionFlow = {
        inlet
        .via( scoring )
        .via( outlet )
      }

      connection handleWith detectionFlow
    }

    import spotlight.app.GraphiteSpotlight.{ WatchPoints => GS }
    import spotlight.stream.OutlierScoringModel.{ WatchPoints => OSM }
    StreamMonitor.set( GS.Framing, GS.Intake, OSM.PlanBuffer, GS.PublishBuffer )

    ( connection to sink ).run()
  }

  def inlet( implicit boundedContext: BoundedContext, configuration: Configuration ): Flow[ByteString, TimeSeries, NotUsed] = {
    import StreamMonitor._
    import WatchPoints._
    configuration.protocol.framingFlow( configuration.maxFrameLength ).watchSourced( Framing )
    .via( Flow[ByteString].buffer( configuration.tcpInboundBufferSize, OverflowStrategy.backpressure ).watchFlow( Intake ) ) //todo fix StreamMonitor flow measurement so able to watch .buffer(..) and not need Flow structure
    .via( configuration.protocol.unmarshalTimeSeriesData )
  }

  def outlet( implicit boundedContext: BoundedContext, configuration: Configuration ): Flow[Outliers, ByteString, NotUsed] = {
    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      import StreamMonitor._
      import WatchPoints._

      val egressBroadcast = b.add( Broadcast[Outliers](outputPorts = 2, eagerCancel = true) )
      val publishBuffer = b.add(
        Flow[Outliers].buffer( 1000, OverflowStrategy.backpressure ).watchFlow( PublishBuffer )
      )
      val publish = b.add( publishOutliers(configuration.graphiteAddress) )
      val tcpOut = b.add( Flow[Outliers].map{ _ => ByteString() } )

      egressBroadcast ~> tcpOut
      egressBroadcast ~> publishBuffer ~> publish

      FlowShape( egressBroadcast.in, tcpOut.out )
    }

    Flow.fromGraph( graph ).withAttributes( ActorAttributes supervisionStrategy Bootstrap.supervisionDecider )
  }

  def publishOutliers( graphiteAddress: Option[InetSocketAddress] ): Sink[Outliers, ActorRef] = {
    val props = graphiteAddress map { address =>
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
          override def publishingTopic( p: OutlierPlan, t: Topic ): Topic = {
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
