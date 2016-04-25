package spotlight.publish

import java.net.{InetSocketAddress, Socket}
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.CircuitBreaker
import akka.stream.actor.{ActorSubscriberMessage, MaxInFlightRequestStrategy, RequestStrategy}
import akka.util.ByteString
import shapeless.syntax.typeable._
import com.codahale.metrics.{Metric, MetricFilter}
import org.joda.{time => joda}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory
import nl.grons.metrics.scala.{Meter, Timer}
import spotlight.model.timeseries.Topic
import spotlight.model.outlier.{OutlierPlan, Outliers}
import spotlight.protocol.PythonPickleProtocol


/**
  * Created by rolfsd on 12/29/15.
  */
object GraphitePublisher extends LazyLogging {
  def props( makePublisher: => GraphitePublisher with PublishProvider ): Props = Props( makePublisher )


  sealed trait GraphitePublisherProtocol
  case object Open extends GraphitePublisherProtocol
  case object Close extends GraphitePublisherProtocol
  case object Flush extends GraphitePublisherProtocol
  case object SendBatch extends GraphitePublisherProtocol
  case object ReplenishTokens extends  GraphitePublisherProtocol

  case object Opened extends GraphitePublisherProtocol
  case object Closed extends GraphitePublisherProtocol
  case object Flushed extends GraphitePublisherProtocol


  trait PublishProvider  {
    def maxOutstanding: Int
    def separation: FiniteDuration = 10.seconds
    def destinationAddress: InetSocketAddress
    def createSocket( address: InetSocketAddress ): Socket
    def batchSize: Int
    def minimumBatchInterval: FiniteDuration = 500.milliseconds
    def maximumBatchInterval: FiniteDuration = 5.seconds
    def publishingTopic( p: OutlierPlan, t: Topic ): Topic = s"${p.name}.${t}"
    def publishAlgorithmControlBoundaries( p: OutlierPlan, algorithm: Symbol ): Boolean = {
      val publishKey = algorithm.name + ".publish-controls"
      if ( p.algorithmConfig hasPath publishKey ) p.algorithmConfig getBoolean publishKey else false
    }
  }


  private[publish] sealed trait BreakerStatus
  case object BreakerOpen extends BreakerStatus { override def toString: String = "open" }
  case object BreakerPending extends BreakerStatus { override def toString: String = "pending" }
  case object BreakerClosed extends BreakerStatus { override def toString: String = "closed" }
}


class GraphitePublisher extends DenseOutlierPublisher { outer: GraphitePublisher.PublishProvider =>
  import GraphitePublisher._
  import OutlierPublisher._
  import context.dispatcher

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy( outer.maxOutstanding ) {
    override def inFlightInternally: Int = waitQueue.size
  }

  val pickler: PythonPickleProtocol = new PythonPickleProtocol
  var socket: Option[Socket] = None //todo: if socket fails supervisor should restart actor
  var waitQueue: immutable.Queue[TopicPoint] = immutable.Queue.empty[TopicPoint]
  override val fillSeparation: FiniteDuration = outer.separation


  lazy val circuitOpenMeter: Meter = metrics.meter( "circuit", "open" )
  lazy val circuitPendingMeter: Meter = metrics.meter( "circuit", "pending" )
  lazy val circuitClosedMeter: Meter = metrics.meter( "circuit", "closed" )
  lazy val circuitRequestsMeter: Meter = metrics.meter( "circuit", "requests" )
  lazy val bytesPublishedMeter: Meter = metrics.meter( "bytes-published" )
  lazy val publishingTimer: Timer = metrics.timer( "publishing" )
  var gaugeStatus: BreakerStatus = BreakerClosed

  val publishLogger: Logger = Logger( LoggerFactory getLogger "Publish" )

  val publishDispatcher = context.system.dispatchers.lookup( "publish-dispatcher" )


  var lastQueueClearingNanos: Long = System.nanoTime()
  def markClearing(): Unit = lastQueueClearingNanos = System.nanoTime()
  def sinceLastClearing(): FiniteDuration = FiniteDuration( System.nanoTime() - lastQueueClearingNanos, NANOSECONDS )

  final def batchInterval: FiniteDuration = {
    val calculated = FiniteDuration( ( publishingTimer.mean + 10 * publishingTimer.stdDev ).toLong, NANOSECONDS )
    calculated match {
      case c if c < minimumBatchInterval => minimumBatchInterval
      case c if maximumBatchInterval < c => maximumBatchInterval
      case c => c
    }
  }

  def initializeMetrics(): Unit = {
    stripLingeringMetrics()
    metrics.gauge( "waiting" ) { waitQueue.size }
    metrics.gauge( "circuit", "breaker" ) { gaugeStatus.toString }
  }

  def stripLingeringMetrics(): Unit = {
    metrics.registry.removeMatching(
      new MetricFilter {
        override def matches( name: String, metric: Metric ): Boolean = {
          name.contains( classOf[GraphitePublisher].getName ) &&
            ( name.contains( "waiting" ) || name.contains( "circuit.breaker" ) )
        }
      }
    )
  }

  val breaker: CircuitBreaker = {
    val reset = 2.minutes

    new CircuitBreaker(
      context.system.scheduler,
      maxFailures = 5,
      callTimeout = 10.seconds,
      resetTimeout = reset
    )( executor = context.system.dispatcher )
    .onOpen {
      circuitOpenMeter.mark()
      gaugeStatus = BreakerOpen
      val oldConnected = isConnected
      val socketClose = Try { socket foreach { _.close() } }
      socket = None
      val reopened = open()

      if ( reopened.isSuccess && isConnected ) {
        log.info(
          "graphite connection is down, closed[{}], reconnection [{}] " +
            "and circuit breaker will remain open for [{}] before trying again",
          socketClose,
          "succeeded",
          reset.toCoarsest
        )
      } else {
        log.error(
          "graphite connection is down, closed[{}], reconnection [{}] " +
            "and circuit breaker will remain open for [{}] before trying again",
          socketClose,
          "failed",
          reset.toCoarsest
        )
      }
    }
    .onHalfOpen {
      circuitPendingMeter.mark()
      gaugeStatus = BreakerPending
      log info "attempting to publish to graphite and close circuit breaker"
    }
    .onClose {
      circuitClosedMeter.mark()
      gaugeStatus = BreakerClosed
      log info "graphite connection established and circuit breaker is closed"
    }
  }

  var _nextScheduled: Option[Cancellable] = None
  def cancelNextSchedule(): Unit = {
    _nextScheduled foreach { _.cancel() }
    _nextScheduled = None
  }

  def resetNextScheduled(): Option[Cancellable] = {
    cancelNextSchedule()

    //todo this wont log -- what do i care to show?
    if ( log.isInfoEnabled && batchInterval > outer.maximumBatchInterval ) {
      log.info( "GraphitePublisher: batch interval = {}", batchInterval.toCoarsest )
    }

    _nextScheduled = Some( context.system.scheduler.scheduleOnce(batchInterval, self, SendBatch) )
    _nextScheduled
  }


  override def preStart(): Unit = {
    initializeMetrics()
    log.debug( "{} dispatcher: [{}]", self.path, context.dispatcher )
    log.info( "GraphitePublisher started with maxOutstanding:[{}]", outer.maxOutstanding )
    self ! Open
  }

  override def postStop(): Unit = {
    cancelNextSchedule()
    close()
    context.parent ! Closed
    context become around( closed )
  }


  override def receive: Receive = LoggingReceive{ around( closed ) }

  val closed: Receive = {
    case Open => {
      open()
      sender() ! Opened
      context become LoggingReceive { around( opened ) }
    }

    case Close => {
      close()
      sender() ! Closed
    }
  }

  val opened: Receive = {
    case ActorSubscriberMessage.OnNext( outliers ) => {
      outliers.cast[Outliers] match {
        case Some( os ) => {
          log.debug( "GraphitePublisher received OnNext. publishing outliers" )
          publish( os )
        }

        case None => log.warning( "GraphitePublisher received UNKNOWN OnNext with:[{}]", outliers )
      }
    }

    case ActorSubscriberMessage.OnComplete => {
      log debug "GraphitePublisher closing on completed stream"
      close()
    }

    case ActorSubscriberMessage.OnError( ex ) => {
      log.info( "GraphitePublisher closing on errored stream: [{}]", ex.getMessage )
      close()
    }

    case Publish( outliers ) => {
      log.debug( "GraphitePublisher received Publish([{}])", outliers )
      publish( outliers )
      sender() ! Published( outliers ) //todo send when all corresponding marks are sent to graphite
    }

    case Open => {
      log.debug( "GraphitePublisher received Open" )
      open()
      sender() ! Opened
    }

    case Close => {
      log.debug( "GraphitePublisher received Close" )
      close()
      sender() ! Closed
      context stop self
    }

    case Flush => {
      log.debug( "GraphitePublisher received Flush" )
      flush()
    }

    case SendBatch => {
      log.debug( "GraphitePublisher received SendBatch: next schedule in [{}]", batchInterval.toCoarsest )
      if ( sinceLastClearing() > batchInterval || waitQueue.size >= outer.batchSize ) batchAndSend()
      resetNextScheduled()
    }
  }

  override def publish( o: Outliers ): Unit = {
    val points = markPoints( o )
    waitQueue = points.foldLeft( waitQueue ){ _ enqueue _ }
    log.debug( "publish[{}]: added [{} / {}] to wait queue - now at [{}]", o.topic, o.anomalySize, o.size, waitQueue.size )
    if ( waitQueue.size > outer.maxOutstanding ) batchAndSend()
  }

  object ControlLabeling {
    type ControlLabel = Symbol => String
    val FloorLabel: ControlLabel = (algorithm: Symbol) => algorithm.name + ".floor."
    val ExpectedLabel: ControlLabel = (algorithm: Symbol) => algorithm.name + ".expected."
    val CeilingLabel: ControlLabel = (algorithm: Symbol) => algorithm.name + ".ceiling."
  }

  def flattenControlPoints( o: Outliers ): Seq[TopicPoint] = {
    import ControlLabeling._
    def algorithmControlTopic( algorithm: Symbol, labeling: ControlLabel ): Topic = {
      outer.publishingTopic( o.plan, labeling(algorithm) + o.topic )
    }

    o.algorithmControlBoundaries.toSeq flatMap {
      case (algorithm, controlBoundaries) if outer.publishAlgorithmControlBoundaries( o.plan, algorithm ) => {
        val floorTopic = algorithmControlTopic( algorithm, FloorLabel )
        val expectedTopic = algorithmControlTopic( algorithm, ExpectedLabel )
        val ceilingTopic = algorithmControlTopic( algorithm, CeilingLabel )
        controlBoundaries flatMap { c =>
          Seq( floorTopic, expectedTopic, ceilingTopic )
          .zipAll( Seq.empty[joda.DateTime], Topic(""), c.timestamp ) // tuple topics with c.timestamp
          .zip( Seq(c.floor, c.expected, c.ceiling) ) // add control values to tuple
          .map { case ((topic, ts), value) => value map { v => ( topic, ts, v ) } } // make tuple
          .flatten // trim out unprovided control values
        }
      }

      case (algorithm, controlBoundaries) => Seq.empty[TopicPoint]
    }
  }

  override def markPoints( o: Outliers ): Seq[TopicPoint] = {
    val dataPoints = super.markPoints( o ) map { case (t, ts, v) => ( outer.publishingTopic(o.plan, t), ts, v ) }
    log.debug( "GraphitePublisher: marked data points: [{}]", dataPoints.mkString(","))
    val controlPoints = flattenControlPoints( o )
    log.debug( "GraphitePublisher: control data points: [{}]", controlPoints.mkString(","))
    dataPoints ++ controlPoints
  }

  def isConnected: Boolean = socket map { s => s.isConnected && !s.isClosed } getOrElse { false }

  def open(): Try[Unit] = {
    if ( isConnected ) Try { () }
    else {
      val socketOpen = Try { createSocket( destinationAddress ) }
      socket = socketOpen.toOption
      socketOpen match {
        case Success(s) => {
          resetNextScheduled()
          log.info( "{} opened [{}] [{}] nextScheduled:[{}]", self.path, socket, socketOpen, _nextScheduled.map{!_.isCancelled} )
        }

        case Failure(ex) => {
          log.error( "open failed - could not connect with graphite server [{}]: [{}]", destinationAddress, ex.getMessage )
          cancelNextSchedule()
        }
      }

      socketOpen map { s => () }
    }
  }

  def close(): Unit = {
    val f = Try { flush() }
    val socketClose = Try { socket foreach { _.close() } }
    socket = None
    cancelNextSchedule()
    context become around( closed )
    log.info( "{} closed [{}] [{}]", self.path, socket, socketClose )
  }

  @tailrec final def flush(): Unit = {
    cancelNextSchedule()

    if ( waitQueue.isEmpty ) {
      log.info( "{} flushing complete", self.path )
      ()
    }
    else {
      val (toBePublished, remainingQueue) = waitQueue splitAt outer.batchSize
      waitQueue = remainingQueue

      publishLogger.info(
        "Flush: toBePublished:[{}]\tremainingQueue:[{}]\tbatchSize:[{}]",
        toBePublished.size.toString,
        remainingQueue.size.toString,
        outer.batchSize.toString
      )

      toBePublished foreach { tbp => publishLogger.info( "{}: timestamp:[{}] value:[{}]", tbp._1, tbp._2, tbp._3.toString) }

      //      log.debug( "flush: batch=[{}]", toBePublished.mkString(",") )
      val report = serialize( toBePublished:_* )
      breaker withSyncCircuitBreaker {
        circuitRequestsMeter.mark()
        sendToGraphite( report )
      }
      flush()
    }
  }

  def serialize( payload: TopicPoint* ): ByteString = pickler.pickleFlattenedTimeSeries( payload:_* )

  def batchAndSend(): Unit = {
    log.debug(
      "waitQueue:[{}] batch-size:[{}] toBePublished:[{}] remainingQueue:[{}]",
      waitQueue.size,
      outer.batchSize,
      waitQueue.take(outer.batchSize).size,
      waitQueue.drop(outer.batchSize).size
    )

    val (toBePublished, remainingQueue) = waitQueue splitAt batchSize
    waitQueue = remainingQueue

    if ( toBePublished.nonEmpty ) {
      publishLogger.info(
        "BatchAndSend: toBePublished:[{} / {}]\tremainingQueue:[{}]",
        toBePublished.size.toString,
        outer.batchSize.toString,
        remainingQueue.size.toString
      )

      val report = pickler.pickleFlattenedTimeSeries( toBePublished:_* )
      breaker withCircuitBreaker {
        Future {
          circuitRequestsMeter.mark()
          sendToGraphite( report )
          resetNextScheduled()
          self ! SendBatch
        }( publishDispatcher ) map { _ =>
          toBePublished foreach { tbp => publishLogger.info( "{}: timestamp:[{}] value:[{}]", tbp._1, tbp._2, tbp._3.toString) }
        } //todo send self message with report to republish upon failure?
      }
    }

    if ( waitQueue.isEmpty ) {
      publishLogger.info( "BatchAndSend: cleared no toBePublish. remainingQueue:[{}]", waitQueue.size.toString )
      markClearing()
    } else {
      resetNextScheduled()
    }
  }

  def sendToGraphite( pickle: ByteString ): Unit = {
    bytesPublishedMeter mark pickle.size

    publishingTimer time {
      socket foreach { s =>
        val out = s.getOutputStream
        out write pickle.toArray
        out.flush()
      }
    }
  }
}
