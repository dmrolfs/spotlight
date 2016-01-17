package lineup.app

import java.net.InetSocketAddress
import lineup.model.timeseries.TimeSeries
import lineup.publish.GraphitePublisher
import peds.akka.stream.StreamMonitor

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import akka.actor.{ ActorKilledException, ActorInitializationException, ActorRef, ActorSystem }
import akka.stream.scaladsl._
import akka.stream._
import akka.util.{ ByteString, Timeout }

import scalaz.{ Sink => _, _ }, Scalaz._
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{ Logger, StrictLogging }
import com.typesafe.config.Config
import kamon.Kamon
import better.files.File
import nl.grons.metrics.scala.{ MetricName, Meter }

import peds.akka.supervision.IsolatedLifeCycleSupervisor.{ WaitForStart, ChildStarted }
import peds.commons.log.Trace
import peds.akka.supervision.OneForOneStrategyFactory
import peds.akka.metrics.{ Reporter, Instrumented }
import peds.commons.Valid
import lineup.analysis.outlier.OutlierDetection
import lineup.stream.OutlierDetectionWorkflow.{ GetPublisher, GetPublishRateLimiter, GetOutlierDetector }
import lineup.model.outlier._
import lineup.protocol.GraphiteSerializationProtocol
import lineup.stream.{ TrainOutlierAnalysis, Configuration, GraphiteModel, OutlierDetectionWorkflow }


/**
  * Created by rolfsd on 1/12/16.
  */
object GraphiteLineup extends Instrumented with StrictLogging{
  override lazy val metricBaseName: MetricName = {
    import peds.commons.util._
    MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  }

  override protected val logger: Logger = Logger( LoggerFactory.getLogger("GraphiteLineup") )

  val trace = Trace[GraphiteLineup.type]


  def main( args: Array[String] ): Unit = {
    Configuration( args ).disjunction match {
      case -\/( exs ) => {
        logger error s"""Failed to start: ${exs.toList.map {_.getMessage}.mkString( "\n", "\n", "\n" )}"""
        System exit -1
      }

      case \/-( config ) => execute( config, Configuration.reloader(args)()() )
    }
  }

  val PlanConfigPath = "lineup.detection-plans"

  def execute( config: Configuration, reloader: () => Valid[Configuration] ): Unit = {
    Kamon.start( config )
    logger info config.usage

    startMetricsReporter( config )

    implicit val system = ActorSystem( "Lineup" )
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings( system ) withSupervisionStrategy workflowSupervision
    )

    val workflow = startWorkflow( config, reloader )

    streamGraphiteOutliers( workflow, config, reloader ) onComplete {
      case Success(_) => {
        system.terminate()
        Kamon.shutdown()
      }

      case Failure( e ) => {
        logger.error( "Graphite Model bootstrap failure:", e )
        system.terminate()
        Kamon.shutdown()
      }
    }
  }

  def streamGraphiteOutliers(
    workflow: ActorRef,
    config: Configuration,
    reloader: () => Valid[Configuration]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Future[Unit] = {
    implicit val dispatcher = system.dispatcher

    import akka.pattern.ask

    implicit val timeout = Timeout( 30.seconds )
    val actors = for {
      _ <- workflow ? WaitForStart
      ChildStarted( d ) <- ( workflow ? GetOutlierDetector ).mapTo[ChildStarted]
      ChildStarted( l ) <- ( workflow ? GetPublishRateLimiter ).mapTo[ChildStarted]
      ChildStarted( p ) <- ( workflow ? GetPublisher ).mapTo[ChildStarted]
    } yield (d, l, p)

    val (detector, limiter, publisher) = Await.result( actors, 1.seconds )

    startPlanWatcher( config, Set(detector) )

    val tcp = Tcp().bind( config.sourceAddress.getHostName, config.sourceAddress.getPort )
    tcp runForeach { connection =>
      detectionWorkflow( detector, limiter, publisher, reloader ) match {
        case \/-( model ) => connection.handleWith( model )
        case -\/( exs ) => exs foreach { ex => logger error s"failed to handle connection: ${ex.getMessage}" }
      }
    }
  }

  def detectionWorkflow(
    detector: ActorRef,
    limiter: ActorRef,
    publisher: ActorRef,
    reloadConfiguration: Configuration.Reload
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Valid[Flow[ByteString, ByteString, Unit]] = {
    for {
      config <- reloadConfiguration()
    } yield {
      logger.info(
        s"""
           |\nConnection made using the following configuration:
           |\tTCP-In Buffer Size   : ${config.tcpInboundBufferSize}
           |\tWorkflow Buffer Size : ${config.workflowBufferSize}
           |\tDetect Timeout       : ${config.detectionBudget.toCoarsest}
           |\tplans                : [${config.plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]
        """.stripMargin
      )

      val graph = GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        import StreamMonitor._

        //todo add support to watch FlowShapes

        val framing = b.add( config.protocol.framingFlow(config.maxFrameLength).watchSourced( 'framing ) )

        val intakeBuffer = b.add(
          Flow[ByteString]
          .buffer( config.tcpInboundBufferSize, OverflowStrategy.backpressure )
          .watchFlow( 'intakeBuffer )
        )

        val timeSeries = b.add( config.protocol.unmarshalTimeSeriesData.watchSourced( 'timeseries ) )
        val scoring = b.add( GraphiteModel.scoringGraph( detector, config ) )
        val logUnrecognized = b.add(
          Flow[TimeSeries].transform( () =>
            GraphiteModel.logMetric( Logger(LoggerFactory getLogger "Unrecognized"), config.plans )
          )
        )
        val broadcast = b.add( Broadcast[Outliers](outputPorts = 2, eagerCancel = false) )
        val publish = b.add( publishOutliers( limiter, publisher ).watchFlow( 'publish ) )
        val tcpOut = b.add( Flow[Outliers].map{ _ => ByteString() }.watchConsumed( 'tcpOut ) )
        val train = b.add( TrainOutlierAnalysis.feedOutlierTraining.watchConsumed( 'train ) )
        val termTraining = b.add( Sink.ignore )
        val termUnrecognized = b.add( Sink.ignore )

        StreamMonitor.set(
          'framing,
          'intakeBuffer,
          'timeseries,
          GraphiteModel.WatchPoints.ScoringPlanned,
          GraphiteModel.WatchPoints.ScoringBatch,
          GraphiteModel.WatchPoints.ScoringAnalysisBuffer,
          GraphiteModel.WatchPoints.ScoringDetect,
          'publish,
          'tcpOut,
          'train
        )

        framing ~> intakeBuffer ~> timeSeries ~> scoring.in
                                                 scoring.out0 ~> broadcast ~> publish ~> tcpOut
                                                                 broadcast ~> train ~> termTraining
                                                 scoring.out1 ~> logUnrecognized ~> termUnrecognized

        FlowShape( framing.in, tcpOut.out )
      }

      Flow.fromGraph( graph ).withAttributes( ActorAttributes.supervisionStrategy(workflowSupervision) )
    }
  }

  def publishOutliers(
    limiter: ActorRef,
    publisher: ActorRef
  )(
    implicit system: ActorSystem
  ): Flow[Outliers, Outliers, Unit] = {
    import scala.collection.immutable

    implicit val ec = system.dispatcher

    Flow[Outliers]
    .conflate( immutable.Seq( _ ) ) { _ :+ _ }
    .mapConcat( identity )
    .via( GraphitePublisher.publish( limiter, publisher, 2, 90.seconds ) )
  }

  def startPlanWatcher( config: Configuration, listeners: Set[ActorRef] )( implicit system: ActorSystem ): Unit = {
    logger info s"Outlier plan origin: [${config.planOrigin}]"
    import java.nio.file.{ StandardWatchEventKinds => Events }
    import better.files.FileWatcher._

    val Path = "@\\s+file:(.*):\\s+\\d+,".r

    Path
    .findAllMatchIn( config.planOrigin.toString )
    .map { _ group 1 }
//    .toList
    .foreach { filename =>
      // note: attempting to watch a shared file wrt VirtualBox will not work (https://www.virtualbox.org/ticket/9069)
      // so dev testing of watching should be done by running the Java locally
      logger info s"watching for changes in ${filename}"
      val configWatcher = File( filename ).newWatcher( true )
      configWatcher ! on( Events.ENTRY_MODIFY ) {
        case _ => {
          logger info s"config file watcher sending reload command due to change in ${config.planOrigin.description}"
          listeners foreach { _ ! OutlierDetection.ReloadPlans }
        }
      }
    }
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
    if ( config hasPath "lineup.metrics" ) {
      logger info s"""starting metric reporting with config: [${config getConfig "lineup.metrics"}]"""
      val reporter = Reporter.startReporter( config getConfig "lineup.metrics" )
      logger info s"metric reporter: [${reporter}]"
    } else {
      logger warn """metric report configuration missing at "lineup.metrics""""
    }
  }

  def startWorkflow( config: Configuration, reloader: () => Valid[Configuration] )( implicit system: ActorSystem ): ActorRef = {
    val loadPlans: () => Valid[Seq[OutlierPlan]] = () => { reloader() map { _.plans } }

    system.actorOf(
      OutlierDetectionWorkflow.props(
        new OutlierDetectionWorkflow() with OneForOneStrategyFactory with OutlierDetectionWorkflow.ConfigurationProvider {
          override def sourceAddress: InetSocketAddress = config.sourceAddress
          override def maxFrameLength: Int = config.maxFrameLength
          override def protocol: GraphiteSerializationProtocol = config.protocol
          override def windowDuration: FiniteDuration = config.windowDuration
          override def graphiteAddress: Option[InetSocketAddress] = config.graphiteAddress
          override def makePlans: () => Valid[Seq[OutlierPlan]] = loadPlans
          override def configuration: Config = config
        }
      ),
      "workflow-supervisor"
    )
  }


  //  def loggerDispatcher( system: ActorSystem ): ExecutionContext = system.dispatchers lookup "logger-dispatcher"
  //
  //  def log( logr: Logger, level: Symbol )( msg: => String )( implicit system: ActorSystem ): Future[Unit] = {
  //    Future {
  //      level match {
  //        case 'debug => logr debug msg
  //        case 'info => logr info msg
  //        case 'warn => logr warn msg
  //        case 'error => logr error msg
  //        case _ => logr error msg
  //      }
  //    }( loggerDispatcher(system) )
  //  }
}
