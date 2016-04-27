package spotlight.app

import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.TimeoutException

import akka.NotUsed

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.matching.Regex
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl._
import akka.stream._
import akka.util.{ByteString, Timeout}

import scalaz.{Sink => _, _}
import Scalaz._
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{Logger, StrictLogging}
import com.typesafe.config.Config
import kamon.Kamon
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.supervision.IsolatedLifeCycleSupervisor.{ChildStarted, WaitForStart}
import peds.commons.log.Trace
import peds.akka.supervision.OneForOneStrategyFactory
import peds.akka.metrics.{Instrumented, Reporter}
import peds.commons.V
import peds.akka.stream.{ProcessorAdapter, StreamMonitor}
import spotlight.analysis.outlier.{OutlierDetection, OutlierPlanDetectionRouter}
import spotlight.model.outlier._
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase, Topic}
import spotlight.protocol.GraphiteSerializationProtocol
import spotlight.publish.{GraphitePublisher, LogPublisher}
import spotlight.stream.{Configuration, OutlierDetectionBootstrap, OutlierScoringModel}
import spotlight.stream.OutlierDetectionBootstrap.{GetOutlierDetector, GetOutlierPlanDetectionRouter}
import spotlight.train.{AvroFileTrainingRepositoryInterpreter, LogStatisticsTrainingRepositoryInterpreter, TrainOutlierAnalysis}


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
  val PlanConfigPath = "spotlight.detection-plans"


  case class BootstrapContext(
    config: Configuration,
    reloader: () => V[Configuration],
    detector: ActorRef,
    planRouter: ActorRef
  )


  def main( args: Array[String] ): Unit = {
    Configuration( args ).disjunction match {
      case \/-( config ) => {
        implicit val system = ActorSystem( "Spotlight" )
        implicit val ec = system.dispatcher

        val serverBinding = for {
          context <- bootstrap( args, config )
          binding <- execute( context )
        } yield binding

        serverBinding.onComplete {
          case Success(b) => logger.info( "Server started, listening on: " + b.localAddress )

          case Failure( ex ) => {
            logger.error( s"Server could not bind to ${config.sourceAddress.getAddress}: ${ex.getMessage}")
            system.terminate()
            Kamon.shutdown()
          }
        }
      }

      case -\/( exs ) => {
        logger error s"""Failed to start: ${exs.toList.map {_.getMessage}.mkString( "\n", "\n", "\n" )}"""
        System exit -1
      }
    }
  }

  def bootstrap( args: Array[String], config: Configuration )( implicit system: ActorSystem ): Future[BootstrapContext] = {
    Kamon.start( config )
    logger info config.usage

    startMetricsReporter( config )

    implicit val ec = system.dispatcher

    val reloader = Configuration.reloader( args )()()
    val workflow = startDetection( config, reloader )

    import akka.pattern.ask

    implicit val timeout = Timeout( 30.seconds )
    val actors = for {
      _ <- workflow ? WaitForStart
      ChildStarted( d ) <- ( workflow ? GetOutlierDetector ).mapTo[ChildStarted]
      ChildStarted( pr ) <- ( workflow ? GetOutlierPlanDetectionRouter ).mapTo[ChildStarted]
    } yield ( d, pr )

    val bootstrapTimeout = akka.pattern.after( 1.second, system.scheduler ) {
      Future.failed( new TimeoutException( "failed to bootstrap detection workflow core actors" ) )
    }

    Future.firstCompletedOf( actors :: bootstrapTimeout :: Nil ) map {
      case (detector, planRouter) => {
        logger.info( "bootstrap detector:[{}] planRouter:[{}]", detector, planRouter )
//        startPlanWatcher( config, Set(detector) )
        BootstrapContext( config, reloader, detector, planRouter )
      }
    }
  }


  def execute( context: BootstrapContext )( implicit system: ActorSystem ): Future[Tcp.ServerBinding] = {
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings( system ) withSupervisionStrategy workflowSupervision
    )

    val address = context.config.sourceAddress

    val connections = Tcp().bind( address.getHostName, address.getPort )

    val handler = Sink.foreach[Tcp.IncomingConnection] { connection =>
      detectionWorkflow( context ) match {
        case \/-( model ) => connection handleWith model
        case f @ -\/( exs ) => {
          exs foreach { ex => logger error s"failed to handle connection: ${ex.getMessage}" }
          Failure( exs.head )
        }
      }
    }

    connections.to( handler ).run()
  }

  def detectionWorkflow(
    context: BootstrapContext
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): V[Flow[ByteString, ByteString, NotUsed]] = {
    context.reloader() map { conf =>
      logger.info(
        s"""
           |\nConnection made using the following configuration:
           |\tTCP-In Buffer Size   : ${conf.tcpInboundBufferSize}
           |\tWorkflow Buffer Size : ${conf.workflowBufferSize}
           |\tDetect Timeout       : ${conf.detectionBudget.toCoarsest}
           |\tplans                : [${conf.plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]
        """.stripMargin
      )

      val graph = GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        import StreamMonitor._

        //todo add support to watch FlowShapes

        val framing = b.add( conf.protocol.framingFlow(conf.maxFrameLength).watchSourced( 'framing ) )

        val intakeBuffer = b.add(
          Flow[ByteString]
          .buffer( conf.tcpInboundBufferSize, OverflowStrategy.backpressure )
          .watchFlow( 'intakeBuffer )
        )

        val timeSeries = b.add( conf.protocol.unmarshalTimeSeriesData )
        val scoring = b.add( OutlierScoringModel.scoringGraph( planRouterRef = context.planRouter, config = conf ) )
        val logUnrecognized = b.add(
          OutlierScoringModel.logMetric( Logger( LoggerFactory getLogger "Unrecognized" ), conf.plans )
        )
        val ingressBroadcast = b.add( Broadcast[TimeSeries](outputPorts = 2, eagerCancel = false) )
        val egressBroadcast = b.add( Broadcast[Outliers](outputPorts = 2, eagerCancel = true) )
        val publishBuffer = b.add(
          Flow[Outliers].buffer( 10000, OverflowStrategy.backpressure ).watchFlow( Symbol("publish.buffer"))
        )
        val publish = b.add( publishOutliers( context.config.graphiteAddress ) )
        val tcpOut = b.add( Flow[Outliers].map{ _ => ByteString() } )

        val passArchivable = b.add( archiveFilter[TimeSeries]( conf ) )

        import AvroFileTrainingRepositoryInterpreter.LocalhostWritersContextProvider
        val interpreter = {
          if ( conf.hasPath("spotlight.training.archival") && conf.getBoolean("spotlight.training.archival") ) {
            new AvroFileTrainingRepositoryInterpreter()( trainingDispatcher(system) ) with LocalhostWritersContextProvider {
              override def config: Config = conf
            }
          } else {
            LogStatisticsTrainingRepositoryInterpreter( trainingLogger )( trainingDispatcher(system) )
          }
        }
        val train = b.add(
          TrainOutlierAnalysis.feedTrainingFlow[TimeSeries](
            interpreter = interpreter,
            maxPoints = conf.getInt( "spotlight.training.batch.max-points" ),
            batchingWindow = FiniteDuration( conf.getDuration("spotlight.training.batch.window", NANOSECONDS), NANOSECONDS )
          )
        )

        val termTraining = b.add( Sink.ignore )
        val termUnrecognized = b.add( Sink.ignore )

//framing,intakeBuffer,scoring.planned,plan.router
        StreamMonitor.set(
          'framing,
          'intakeBuffer,
          Symbol("plan.buffer"),
//          Symbol( OutlierPlanDetectionRouter.WatchPoints.PlanRouter.name + ".buffer" ),
          OutlierPlanDetectionRouter.WatchPoints.PlanRouter,
          Symbol( "publish.buffer" )
        )

                                                 ingressBroadcast ~> passArchivable ~> train ~> termTraining
        framing ~> intakeBuffer ~> timeSeries ~> ingressBroadcast ~> scoring.in
                                                                     scoring.out0 ~> egressBroadcast ~> tcpOut
                                                                                     egressBroadcast ~> publishBuffer ~> publish
                                                                     scoring.out1 ~> logUnrecognized ~> termUnrecognized

        FlowShape( framing.in, tcpOut.out )
      }

      Flow.fromGraph( graph ).withAttributes( ActorAttributes.supervisionStrategy(workflowSupervision) )
    }
  }

  def archiveFilter[T <: TimeSeriesBase]( config: Configuration ): Flow[T, T, NotUsed] = {
    val archiveWhitelist: Set[Regex] = {
      import scala.collection.JavaConverters._
      if ( config hasPath "spotlight.training.whitelist" ) {
        config.getStringList( "spotlight.training.whitelist" ).asScala.toSet map { wl: String => new Regex( wl ) }
      } else {
        Set.empty[Regex]
      }
    }
    logger info s"""training archive whitelist: [${archiveWhitelist.mkString(",")}]"""

    val baseline = (ts: T) => { config.plans.exists{ _ appliesTo ts } }

    val isArchivable: T => Boolean = {
      if ( archiveWhitelist.nonEmpty ) baseline
      else (ts: T) => { archiveWhitelist.exists( _.findFirstIn( ts.topic.name ).isDefined ) || baseline( ts ) }
    }

    Flow[T].filter{ isArchivable }
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

    Sink actorSubscriber[Outliers] props
  }

  def startPlanWatcher( config: Configuration, listeners: Set[ActorRef] )( implicit system: ActorSystem ): Unit = {
//    logger info s"Outlier plan origin: [${config.planOrigin}]"
//    import java.nio.file.{ StandardWatchEventKinds => Events }
//    import better.files.FileWatcher._
//
//    val Path = "@\\s+file:(.*):\\s+\\d+,".r
//
//    Path
//    .findAllMatchIn( config.planOrigin.toString )
//    .map { _ group 1 }
//    .foreach { filename =>
//      // note: attempting to watch a shared file wrt VirtualBox will not work (https://www.virtualbox.org/ticket/9069)
//      // so dev testing of watching should be done by running the Java locally
//      logger info s"watching for changes in ${filename}"
//      val configWatcher = File( filename ).newWatcher( true )
//      configWatcher ! on( Events.ENTRY_MODIFY ) {
//        case _ => {
//          logger info s"config file watcher sending reload command due to change in ${config.planOrigin.description}"
//          listeners foreach { _ ! OutlierDetection.ReloadPlans }
//        }
//      }
//    }
  }

  lazy val workflowFailuresMeter: Meter = metrics meter "workflow.failures"

  val workflowSupervision: Supervision.Decider = {
    case ex => {
      logger.error( "Error caught by Supervisor:", ex )
      workflowFailuresMeter.mark( )
      Supervision.Restart
    }
  }

  def startMetricsReporter( config: Configuration ): Unit = {
    if ( config hasPath "spotlight.metrics" ) {
      logger info s"""starting metric reporting with config: [${config getConfig "spotlight.metrics"}]"""
      val reporter = Reporter.startReporter( config getConfig "spotlight.metrics" )
      logger info s"metric reporter: [${reporter}]"
    } else {
      logger warn """metric report configuration missing at "spotlight.metrics""""
    }
  }

  def startDetection( config: Configuration, reloader: () => V[Configuration] )( implicit system: ActorSystem ): ActorRef = {
    system.actorOf(
      OutlierDetectionBootstrap.props(
        new OutlierDetectionBootstrap( ) with OneForOneStrategyFactory with OutlierDetectionBootstrap.ConfigurationProvider {
          override def sourceAddress: InetSocketAddress = config.sourceAddress
          override def maxFrameLength: Int = config.maxFrameLength
          override def protocol: GraphiteSerializationProtocol = config.protocol
          override def windowDuration: FiniteDuration = config.windowDuration
          override def detectionBudget: FiniteDuration = config.detectionBudget
          override def bufferSize: Int = config.workflowBufferSize
          override def maxInDetectionCpuFactor: Double = config.maxInDetectionCpuFactor
          override def configuration: Config = config
        }
      ),
      "detection-supervisor"
    )
  }

  private val trainingLogger: Logger = Logger( LoggerFactory getLogger "Training" )

  private def trainingDispatcher( system: ActorSystem ): ExecutionContext = system.dispatchers lookup "logger-dispatcher"
}
