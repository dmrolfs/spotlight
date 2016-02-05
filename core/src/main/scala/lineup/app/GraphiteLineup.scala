package lineup.app

import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException
import akka.stream.scaladsl.Tcp.{ ServerBinding, IncomingConnection }
import org.apache.http.HttpEntityEnclosingRequest

import scala.concurrent.{ ExecutionContext, Await, Future }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.util.matching.Regex
import akka.actor.{ ActorRef, ActorSystem }
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
import peds.commons.V
import peds.akka.stream.StreamMonitor
import lineup.analysis.outlier.OutlierDetection
import lineup.model.outlier._
import lineup.model.timeseries.{ TimeSeries, TimeSeriesBase }
import lineup.protocol.GraphiteSerializationProtocol
import lineup.publish.GraphitePublisher
import lineup.stream.{ Configuration, OutlierScoringModel, OutlierDetectionWorkflow }
import lineup.stream.OutlierDetectionWorkflow.{ GetPublisher, GetPublishRateLimiter, GetOutlierDetector }
import lineup.train.{ AvroFileTrainingRepositoryInterpreter, LogStatisticsTrainingRepositoryInterpreter, TrainOutlierAnalysis }


/**
  * Created by rolfsd on 1/12/16.
  */
object GraphiteLineup extends Instrumented with StrictLogging {
  override lazy val metricBaseName: MetricName = {
    import peds.commons.util._
    MetricName( getClass.getPackage.getName, getClass.safeSimpleName )
  }

  override protected val logger: Logger = Logger( LoggerFactory.getLogger("GraphiteLineup") )
  val trace = Trace[GraphiteLineup.type]
  val PlanConfigPath = "lineup.detection-plans"


  case class BootstrapContext(
    config: Configuration,
    reloader: () => V[Configuration],
    detector: ActorRef,
    limiter: ActorRef,
    publisher: ActorRef
  )


  def main( args: Array[String] ): Unit = {
    Configuration( args ).disjunction match {
      case \/-( config ) => {
        implicit val system = ActorSystem( "Lineup" )
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
    val workflow = startWorkflow( config, reloader )

    import akka.pattern.ask

    implicit val timeout = Timeout( 30.seconds )
    val actors = for {
      _ <- workflow ? WaitForStart
      ChildStarted( d ) <- ( workflow ? GetOutlierDetector ).mapTo[ChildStarted]
      ChildStarted( l ) <- ( workflow ? GetPublishRateLimiter ).mapTo[ChildStarted]
      ChildStarted( p ) <- ( workflow ? GetPublisher ).mapTo[ChildStarted]
    } yield ( d, l, p )

    val bootstrapTimeout = akka.pattern.after( 1.second, system.scheduler ) {
      Future.failed( new TimeoutException( "failed to bootstrap detection workflow core actors" ) )
    }

    Future.firstCompletedOf( actors :: bootstrapTimeout :: Nil ) map {
      case (detector, limiter, publisher) => {
        startPlanWatcher( config, Set(detector) )
        BootstrapContext( config, reloader, detector, limiter, publisher )
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
  ): V[Flow[ByteString, ByteString, Unit]] = {
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

        val timeSeries = b.add( conf.protocol.unmarshalTimeSeriesData.watchSourced( 'timeseries ) )
        val scoring = b.add( OutlierScoringModel.scoringGraph( context.detector, conf ) )
        val logUnrecognized = b.add(
          Flow[TimeSeries].transform( () =>
            OutlierScoringModel.logMetric( Logger( LoggerFactory getLogger "Unrecognized" ), conf.plans )
          )
        )
        val broadcast = b.add( Broadcast[TimeSeries](outputPorts = 2, eagerCancel = false) )
        val publish = b.add( publishOutliers( context.limiter, context.publisher ).watchFlow( 'publish ) )
        val tcpOut = b.add( Flow[Outliers].map{ _ => ByteString() }.watchConsumed( 'tcpOut ) )

        val passArchivable = b.add( archiveFilter[TimeSeries]( conf ) )

        import AvroFileTrainingRepositoryInterpreter.LocalhostWritersContextProvider
        val interpreter = {
          if ( conf.hasPath("lineup.training.archival") && conf.getBoolean("lineup.training.archival") ) {
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
            maxPoints = conf.getInt( "lineup.training.batch.max-points" ),
            batchingWindow = FiniteDuration( conf.getDuration("lineup.training.batch.window", NANOSECONDS), NANOSECONDS )
          ).watchConsumed( 'train )
        )

        val termTraining = b.add( Sink.ignore )
        val termUnrecognized = b.add( Sink.ignore )

        StreamMonitor.set(
          'framing,
          'intakeBuffer,
          'timeseries,
          OutlierScoringModel.WatchPoints.ScoringPlanned,
          OutlierScoringModel.WatchPoints.ScoringBatch,
          OutlierScoringModel.WatchPoints.ScoringAnalysisBuffer,
          OutlierScoringModel.WatchPoints.ScoringDetect,
          'publish,
          'tcpOut,
          'train
        )

                                                 broadcast ~> passArchivable ~> train ~> termTraining
        framing ~> intakeBuffer ~> timeSeries ~> broadcast ~> scoring.in
                                                              scoring.out0 ~> publish ~> tcpOut
                                                              scoring.out1 ~> logUnrecognized ~> termUnrecognized

        FlowShape( framing.in, tcpOut.out )
      }

      Flow.fromGraph( graph ).withAttributes( ActorAttributes.supervisionStrategy(workflowSupervision) )
    }
  }

  def archiveFilter[T <: TimeSeriesBase]( config: Configuration ): Flow[T, T, Unit] = {
    val archiveWhitelist: Set[Regex] = {
      import scala.collection.JavaConverters._
      if ( config hasPath "lineup.training.whitelist" ) {
        config.getStringList( "lineup.training.whitelist" ).asScala.toSet map { wl: String => new Regex( wl ) }
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
if ( config.hasPath("lineup.metrics.csv.dir") ) File( config.getString("lineup.metrics.csv.dir") ).createIfNotExists( asDirectory = true ) //todo remove with next peds version
      val reporter = Reporter.startReporter( config getConfig "lineup.metrics" )
      logger info s"metric reporter: [${reporter}]"
    } else {
      logger warn """metric report configuration missing at "lineup.metrics""""
    }
  }

  def startWorkflow( config: Configuration, reloader: () => V[Configuration] )( implicit system: ActorSystem ): ActorRef = {
    val loadPlans: () => V[Seq[OutlierPlan]] = () => { reloader() map { _.plans } }

    system.actorOf(
      OutlierDetectionWorkflow.props(
        new OutlierDetectionWorkflow() with OneForOneStrategyFactory with OutlierDetectionWorkflow.ConfigurationProvider {
          override def sourceAddress: InetSocketAddress = config.sourceAddress
          override def maxFrameLength: Int = config.maxFrameLength
          override def protocol: GraphiteSerializationProtocol = config.protocol
          override def windowDuration: FiniteDuration = config.windowDuration
          override def graphiteAddress: Option[InetSocketAddress] = config.graphiteAddress
          override def makePlans: () => V[Seq[OutlierPlan]] = loadPlans
          override def configuration: Config = config
        }
      ),
      "workflow-supervisor"
    )
  }

  private val trainingLogger: Logger = Logger( LoggerFactory getLogger "Training" )

  private def trainingDispatcher( system: ActorSystem ): ExecutionContext = system.dispatchers lookup "logger-dispatcher"
}
