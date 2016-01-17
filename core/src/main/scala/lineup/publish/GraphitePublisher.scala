package lineup.publish

import java.net.{ InetSocketAddress, Socket }
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.{ CircuitBreaker, CircuitBreakerOpenException, ask }
import akka.stream.scaladsl.Flow
import akka.stream.{ ActorAttributes, Supervision }
import akka.util.{ ByteString, Timeout }

import com.typesafe.scalalogging.{ LazyLogging, Logger }
import nl.grons.metrics.scala.Meter
import org.slf4j.LoggerFactory
import peds.akka.stream.Limiter
import peds.commons.log.Trace
import lineup.model.outlier.Outliers
import lineup.protocol.PythonPickleProtocol


/**
  * Created by rolfsd on 12/29/15.
  */
object GraphitePublisher extends LazyLogging {
  import OutlierPublisher._

  def props( makePublisher: => GraphitePublisher with PublishProvider ): Props = Props( makePublisher )


  def publish(
    limiter: ActorRef,
    publisher: ActorRef,
    parallelism: Int,
    timeout: FiniteDuration
  )(
    implicit ec: ExecutionContext
  ): Flow[Outliers, Outliers, Unit] = {
    val decider: Supervision.Decider = {
      case ex: CircuitBreakerOpenException => {
        logger warn s"GRAPHITE CIRCUIT BREAKER OPENED: [${ex.getMessage}]"
        Supervision.Resume
      }

      case ex => {
        logger warn s"GraphitePublisher restarting after error: [${ex.getMessage}]"
        Supervision.Restart
      }
    }

    implicit val to = Timeout( timeout )

    Flow[Outliers]
    .conflate( immutable.Seq( _ ) ) { _ :+ _ }
    .mapConcat( identity )
    .via( Limiter.limitGlobal(limiter, 2.minutes) )
    .mapAsync( 1 ) { os =>
      val published = publisher ? Publish( os )
      published.mapTo[Published] map { _.outliers }
    }
    .withAttributes( ActorAttributes.supervisionStrategy(decider) )
  }


  sealed trait GraphitePublisherProtocol
  case object Open extends GraphitePublisherProtocol
  case object Close extends GraphitePublisherProtocol
  case object Flush extends GraphitePublisherProtocol
  case object SendBatch extends GraphitePublisherProtocol
  case object ReplenishTokens extends  GraphitePublisherProtocol

  case object Opened extends GraphitePublisherProtocol
  case object Closed extends GraphitePublisherProtocol
  case object Flushed extends GraphitePublisherProtocol


  trait PublishProvider {
    def separation: FiniteDuration = 10.seconds
    def destinationAddress: InetSocketAddress
    def createSocket( address: InetSocketAddress ): Socket
    def batchSize: Int = 100
    def batchInterval: FiniteDuration = 1.second
  }


  private[publish] sealed trait BreakerStatus
  case object BreakerOpen extends BreakerStatus { override def toString: String = "open" }
  case object BreakerPending extends BreakerStatus { override def toString: String = "pending" }
  case object BreakerClosed extends BreakerStatus { override def toString: String = "closed" }
}


class GraphitePublisher( outlierTopicPrefix: Option[String] ) extends DenseOutlierPublisher {
  outer: GraphitePublisher.PublishProvider =>

  import GraphitePublisher._
  import OutlierPublisher._
  import context.dispatcher

  val trace = Trace[GraphitePublisher]

  val pickler: PythonPickleProtocol = new PythonPickleProtocol
  val outlierLogger: Logger = Logger( LoggerFactory getLogger "Outliers" )
  var socket: Option[Socket] = None //todo: if socket fails supervisor should restart actor
  var waitQueue: immutable.Queue[MarkPoint] = immutable.Queue.empty[MarkPoint]
  override val fillSeparation: FiniteDuration = outer.separation


  lazy val circuitOpenMeter: Meter = metrics.meter( "circuit", "open" )
  lazy val circuitPendingMeter: Meter = metrics.meter( "circuit", "pending" )
  lazy val circuitRequestsMeter: Meter = metrics.meter( "circuit", "requests" )
  lazy val publishedMeter: Meter = metrics.meter( "published" )
  var gaugeStatus: BreakerStatus = BreakerClosed


  def initializeMetrics(): Unit = {
    metrics.gauge( "waiting" ) { waitQueue.size }
    metrics.gauge( "circuit", "breaker" ) { gaugeStatus.toString }
  }

  val breaker: CircuitBreaker = {
    new CircuitBreaker(
      context.system.scheduler,
      maxFailures = 5,
      callTimeout = 10.seconds,
      resetTimeout = 1.minute
    )( executor = context.system.dispatcher )
    .onOpen {
      gaugeStatus = BreakerOpen
      log warning  "graphite connection is down, and circuit breaker will remain open for 1 minute before trying again"
    }
    .onHalfOpen {
      gaugeStatus = BreakerPending
      log info "attempting to publish to graphite and close circuit breaker"
    }
    .onClose {
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
    _nextScheduled = Some( context.system.scheduler.scheduleOnce(outer.batchInterval, self, SendBatch) )
    _nextScheduled
  }


  override def preStart(): Unit = {
    initializeMetrics()
    log info s"${self.path} dispatcher: [${context.dispatcher}]"
    self ! Open
  }

  override def postStop(): Unit = {
    cancelNextSchedule()
    close()
    context.parent ! Closed
    context become around( closed )
  }


  override def receive: Receive = around( closed )

  val closed: Receive = LoggingReceive {
    case Open => {
      open()
      sender() ! Opened
      context become around( opened )
    }

    case Close => {
      close()
      sender() ! Closed
    }
  }

  val opened: Receive = LoggingReceive {
    case Publish( outliers ) => {
      publish( outliers )
      sender() ! Published( outliers ) //todo send when all corresponding marks are sent to graphite
    }

    case Open => {
      open()
      sender() ! Opened
    }

    case Close => {
      close()
      sender() ! Closed
      context become around( closed )
    }

    case Flush => flush()

    case SendBatch => {
      if ( waitQueue.nonEmpty ) batchAndSend()
      resetNextScheduled()
    }
  }

  override def publish( o: Outliers ): Unit = {
    val points = markPoints( o )
    waitQueue = points.foldLeft( waitQueue ){ _.enqueue( _ ) }
    log info s"publish[${o.topic}:${o.hasAnomalies}]: added [${o.anomalySize} / ${o.size}] to wait queue - now at [${waitQueue.size}]"
    outlierLogger info o.toString
  }

  override def markPoints( o: Outliers ): Seq[MarkPoint] = {
    def augmentTopic( mp: MarkPoint ): MarkPoint = {
      outlierTopicPrefix map { prefix =>
        val (n, ts, v) = mp
        ( prefix + n, ts, v )
      } getOrElse {
        mp
      }
    }

    super.markPoints( o ) map { augmentTopic }
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
          log info s"${self.path} opened [${socket}] [${socketOpen}] nextScheduled:[${_nextScheduled.map{!_.isCancelled}}]"
        }

        case Failure(ex) => {
          log error s"open failed - could not connect with graphite server: [${ex.getMessage}]"
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
    log info s"${self.path} closed [${socket}] [${socketClose}]"
  }

  @tailrec final def flush(): Unit = {
    cancelNextSchedule()

    if ( waitQueue.isEmpty ) {
      log info s"${self.path} flushing complete"
      ()
    }
    else {
      val (toBePublished, remainingQueue) = waitQueue splitAt outer.batchSize
      waitQueue = remainingQueue

      log info s"""flush: batch=[${toBePublished.mkString(",")}]"""
      val report = serialize( toBePublished:_* )
      breaker withSyncCircuitBreaker {
        circuitRequestsMeter.mark()
        sendToGraphite( report )
      }
      flush()
    }
  }

  def serialize( payload: MarkPoint* ): ByteString = pickler.pickleFlattenedTimeSeries( payload:_* )

  def batchAndSend(): Unit = {
    log debug s"waitQueue:[${waitQueue.size}] batch-size:[${outer.batchSize}] toBePublished:[${waitQueue.take(outer.batchSize).size}] remainingQueue:[${waitQueue.drop(outer.batchSize).size}]"
    val (toBePublished, remainingQueue) = waitQueue splitAt batchSize
    waitQueue = remainingQueue

    if ( toBePublished.nonEmpty ) {
      val report = pickler.pickleFlattenedTimeSeries( toBePublished:_* )
      breaker withCircuitBreaker {
        Future {
          circuitRequestsMeter.mark()
          sendToGraphite( report )
          resetNextScheduled()
          self ! SendBatch
        }
      }
    }
  }

  def sendToGraphite( pickle: ByteString ): Unit = {
    publishedMeter mark pickle.size

    socket foreach { s =>
      val out = s.getOutputStream
      out write pickle.toArray
      out.flush()
    }
  }
}
