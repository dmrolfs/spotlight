package lineup.stream

import java.util.concurrent.atomic.AtomicInteger
import lineup.Valid
import lineup.app.Configuration
import lineup.protocol.GraphiteSerializationProtocol

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import akka.actor._
import akka.stream.scaladsl._
import akka.stream._
import akka.util.ByteString
import akka.stream.stage.{ SyncDirective, Context, PushStage }

import com.typesafe.scalalogging.{ StrictLogging, Logger }
import org.slf4j.LoggerFactory
import nl.grons.metrics.scala.Meter
import peds.akka.metrics.{ StreamMonitor, Instrumented }
import peds.commons.collection.BloomFilter

import lineup.analysis.outlier.OutlierDetection
import lineup.model.outlier._
import lineup.model.timeseries.TimeSeriesBase.Merging
import lineup.model.timeseries._
import lineup.publish.GraphitePublisher
import lineup.stream.OutlierDetectionWorkflow._


/**
 * Created by rolfsd on 10/21/15.
 */
object GraphiteModel extends Instrumented with StrictLogging {
  lazy val streamFailuresMeter: Meter = metrics meter "stream.failures"

  val defaultSupervision: Supervision.Decider = {
    case _: ActorInitializationException  => Supervision.stop

    case _: ActorKilledException => Supervision.stop

    case _ => {
      streamFailuresMeter.mark()
      Supervision.restart
    }
  }

  trait ConfigurationProvider {
    def tcpInboundBufferSize: Int
    def workflowBufferSize: Int
    def scoringBudget: FiniteDuration

    def plans: Valid[Seq[OutlierPlan]]
    def protocol: GraphiteSerializationProtocol
    def maxFrameLength: Int
    def windowDuration: FiniteDuration
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
      log( logger, 'info ){
        s"""
           |\nConnection made using the following configuration:
           |\tTCP-In Buffer Size   : ${config.tcpInboundBufferSize}
           |\tWorkflow Buffer Size : ${config.workflowBufferSize}
           |\tDetect Timeout       : ${config.detectionBudget.toCoarsest}
           |\tplans                : [${config.plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]
       """.stripMargin
      }

      Flow.fromGraph(
        detectionGraph(
          detector = detector,
          limiter = limiter,
          publisher = publisher,
          config = config
        ).withAttributes( ActorAttributes.supervisionStrategy(defaultSupervision) )
      )
    }
  }

  def detectionGraph(
    detector: ActorRef,
    limiter: ActorRef,
    publisher: ActorRef,
    config: Configuration
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Graph[FlowShape[ByteString, ByteString], Unit] = {
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val framing = b.add(
        StreamMonitor.source('framing).watch( config.protocol.framingFlow( config.maxFrameLength ) )
      )

      val timeSeries = b.add( StreamMonitor.source('timeseries).watch( config.protocol.loadTimeSeriesData ) )
      val planned = b.add( StreamMonitor.flow('planned).watch( filterPlanned(config.plans) ) )

      val batch = StreamMonitor.sink('batch).watch( batchSeries( config.windowDuration ) )

      val buf1 = b.add(
        StreamMonitor.flow('buffer1).watch(Flow[ByteString].buffer(config.tcpInboundBufferSize, OverflowStrategy.backpressure))
      )

      val buf2 = b.add(
        StreamMonitor.source('groups).watch(
          Flow[TimeSeries]
          .via(
            StreamMonitor.flow('buffer2).watch(
              Flow[TimeSeries].buffer( config.workflowBufferSize, OverflowStrategy.backpressure )
            )
          )
        )
      )

      val detectOutlier = StreamMonitor.flow('detect).watch(
        OutlierDetection.detectOutlier( detector, config.detectionBudget, Runtime.getRuntime.availableProcessors * 8 )
      )

      val broadcast = b.add( Broadcast[Outliers](outputPorts = 2, eagerCancel = false) )
      val publish = b.add( StreamMonitor.flow('publish).watch( publishOutliers(limiter, publisher) ) )
      val tcpOut = b.add( StreamMonitor.sink('tcpOut).watch( Flow[Outliers] map { _ => ByteString() } ) )
      val train = StreamMonitor.sink('train).watch( TrainOutlierAnalysis.feedOutlierTraining )
      val term = b.add( Sink.ignore )

      StreamMonitor.set(
        'framing,
        'buffer1,
        'timeseries,
        'planned,
        'batch,
        'groups,
        'buffer2,
        'detect,
        'publish,
        'tcpOut,
        'train
      )

      framing ~> buf1 ~> timeSeries ~> planned ~> batch ~> buf2 ~> detectOutlier ~> broadcast ~> publish ~> tcpOut
                                                                                    broadcast ~> train ~> term

      FlowShape( framing.in, tcpOut.out )
    }
  }

  def filterPlanned( plans: Seq[OutlierPlan] )( implicit system: ActorSystem ): Flow[TimeSeries, TimeSeries, Unit] = {
    def logMetric: PushStage[TimeSeries, TimeSeries] = new PushStage[TimeSeries, TimeSeries] {
      val count = new AtomicInteger( 0 )
      var bloom = BloomFilter[Topic]( maxFalsePosProbability = 0.001, 500000 )
      val metricLogger = Logger( LoggerFactory getLogger "Metrics" )

      override def onPush( elem: TimeSeries, ctx: Context[TimeSeries] ): SyncDirective = {
        if ( bloom has_? elem.topic ) ctx push elem
        else {
          bloom += elem.topic
          if ( !elem.topic.name.startsWith(OutlierMetricPrefix) ) {
            log( metricLogger, 'debug ){
              s"""[${count.incrementAndGet}] Plan for ${elem.topic}: ${plans.find{ _ appliesTo elem }.getOrElse{"NONE"}}"""
            }
          }

          ctx push elem
        }
      }
    }

    Flow[TimeSeries]
    .transform( () => logMetric )
    .filter { ts => !ts.topic.name.startsWith( OutlierMetricPrefix ) && plans.exists{ _ appliesTo ts } }
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


  /**
    * Limit downstream rate to one element every 'interval' by applying back-pressure on upstream.
    *
    * @param interval time interval to send one element downstream
    * @tparam A
    * @return
    */
  def rateLimiter[A]( interval: FiniteDuration ): Flow[A, A, Unit] = {
    case object Tick

    val graph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val rateLimiter = Source.tick( 0.second, interval, Tick )
      val zip = builder add Zip[A, Tick.type]()
      rateLimiter ~> zip.in1

      FlowShape( zip.in0, zip.out.map{_._1}.outlet )
    }

    // We need to limit input buffer to 1 to guarantee the rate limiting feature
    Flow.fromGraph( graph ).withAttributes( Attributes.inputBuffer(initial = 1, max = 64) )
  }

  def batchSeries(
    windowSize: FiniteDuration = 1.minute,
    parallelism: Int = 4
  )(
    implicit system: ActorSystem,
    tsMerging: Merging[TimeSeries],
    materializer: Materializer
  ): Flow[TimeSeries, TimeSeries, Unit] = {
    val numTopics = 1

    val n = if ( numTopics * windowSize.toMicros.toInt < 0 ) { numTopics * windowSize.toMicros.toInt } else { Int.MaxValue }
    logger info s"n = [${n}] for windowSize=[${windowSize.toCoarsest}]"
    Flow[TimeSeries]
    .groupedWithin( n, d = windowSize ) // max elems = 1 per micro; duration = windowSize
    .map {
      _.groupBy{ _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
  }


  def loggerDispatcher( system: ActorSystem ): ExecutionContext = system.dispatchers lookup "logger-dispatcher"

  def log( logr: Logger, level: Symbol )( msg: => String )( implicit system: ActorSystem ): Future[Unit] = {
    Future {
      level match {
        case 'debug => logr debug msg
        case 'info => logr info msg
        case 'warn => logr warn msg
        case 'error => logr error msg
        case _ => logr error msg
      }
    }( loggerDispatcher(system) )
  }
}
