package lineup.stream

import java.net.{ InetSocketAddress, Socket }
import scala.annotation.tailrec
import scala.concurrent.{ Future, ExecutionContext }
import scala.concurrent.duration._
import scala.collection.immutable
import scala.util.Try
import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.{ ask, CircuitBreakerOpenException, CircuitBreaker }
import akka.util.{ByteString, Timeout}
import akka.stream.{ ActorAttributes, Supervision }
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.{ Logger, LazyLogging }
import org.slf4j.LoggerFactory
import peds.akka.stream.Limiter
import peds.commons.log.Trace
import lineup.model.outlier.Outliers


/**
  * Created by rolfsd on 12/29/15.
  */
object GraphitePublisher extends LazyLogging {
  import OutlierPublisher._

  def props( address: InetSocketAddress, batchSize: Int = 100 ): Props = {
    Props(
      new GraphitePublisher( address, batchSize ) with SocketProvider {
        override def createSocket( address: InetSocketAddress ): Socket = new Socket( address.getAddress, address.getPort )
      }
    )
  }


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
    .via( Limiter.limitGlobal(limiter, 1.minute) )
    .mapAsync( 1 ) { os =>
      val published = publisher ? Publish( os )
      published.mapTo[Published] map { _.outliers }
    }.withAttributes( ActorAttributes.supervisionStrategy(decider) )
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


  trait SocketProvider {
    def createSocket( address: InetSocketAddress ): Socket
  }
}


class GraphitePublisher(
  address: InetSocketAddress,
  batchSize: Int = 100
) extends OutlierPublisher {
  outer: GraphitePublisher.SocketProvider =>
  import OutlierPublisher._
  import GraphitePublisher._
  import context.dispatcher

  val trace = Trace[GraphitePublisher]

  val pickler: PythonPickleProtocol = new PythonPickleProtocol
  val outlierLogger: Logger = Logger( LoggerFactory getLogger "Outliers" )
  var socket: Option[Socket] = None
  var waitQueue = immutable.Queue.empty[MarkPoint]

  val breaker: CircuitBreaker = {
    new CircuitBreaker(
      context.system.scheduler,
      maxFailures = 5,
      callTimeout = 10.seconds,
      resetTimeout = 1.minute
    )( executor = context.system.dispatcher )
    .onOpen( log warning  "graphite connection is down, and circuit breaker will remain open for 1 minute before trying again" )
    .onHalfOpen( log info "attempting to publish to graphite and close circuit breaker" )
    .onClose( log info "graphite connection established and circuit breaker is closed" )
  }

  var _nextScheduled: Option[Cancellable] = None
  def cancelNextSchedule(): Unit = {
    _nextScheduled foreach { _.cancel() }
    _nextScheduled = None
  }

  def resetNextScheduled(): Option[Cancellable] = {
    cancelNextSchedule()
    _nextScheduled = Some( context.system.scheduler.scheduleOnce(1.second, self, SendBatch) )
    _nextScheduled
  }


  override def preStart(): Unit = open()

  override def postStop(): Unit = {
    cancelNextSchedule()
    close()
  }


  override def receive: Receive = around( closed )

  val closed: Receive = LoggingReceive {
    case Open => open()

    case Close => { }
  }

  val opened: Receive = LoggingReceive {
    case Publish( outliers ) => {
      publish( outliers )
      sender() ! Published( outliers ) //todo send when all corresponding marks are sent to graphite
    }

    case Open => { }

    case Close => close()

    case Flush => flush()

    case SendBatch => if ( waitQueue.nonEmpty ) batchAndSend()
  }

  override def publish( o: Outliers ): Unit = trace.block( s"publish($o)" ) {
    waitQueue = markPoints( o ).foldLeft( waitQueue ){ _.enqueue( _ ) }
    outlierLogger info o.toString
  }


  def open(): Try[Unit] = trace.block( "open" ) {
    Try {
      if ( !isConnected ) {
        val socketOpen = Try { createSocket(address) }
        socket = socketOpen.toOption
        resetNextScheduled()
        sender() ! Opened
        context become around( opened )
        log info s"${self.path} opened [${socket}] [${socketOpen}]"
      }
    }
  }

  def close(): Unit = trace.block( "close" ) {
    val f = Try { trace.briefBlock("flushing") { flush() } }
    val socketClose = Try { socket foreach { _.close() } }
    socket = None
    cancelNextSchedule()
    sender() ! Closed
    context become around( closed )
    log info s"${self.path} closed [${socket}] [${socketClose}]"
  }

  def isConnected: Boolean = socket map { s => s.isConnected && !s.isClosed } getOrElse { false }

  @tailrec final def flush(): Unit = {
    cancelNextSchedule()

    if ( waitQueue.isEmpty ) {
      log info s"${self.path} flushing complete"
      ()
    }
    else {
      val (toBePublished, remainingQueue) = waitQueue splitAt batchSize
      waitQueue = remainingQueue

      log debug s"""flush: batch=[${toBePublished.mkString(",")}]"""
      val report = serialize( toBePublished:_* )
      breaker withSyncCircuitBreaker { sendToGraphite( report ) }
      flush()
    }
  }

  def serialize( payload: MarkPoint* ): ByteString = pickler.pickleFlattenedTimeSeries( payload:_* )

  def batchAndSend(): Unit = trace.block( "batchAndSend" ) {
    val (toBePublished, remainingQueue) = waitQueue splitAt batchSize
    waitQueue = remainingQueue

    if ( toBePublished.nonEmpty ) {
      val report = pickler.pickleFlattenedTimeSeries( toBePublished:_* )
      breaker withCircuitBreaker {
        Future {
          sendToGraphite( report )
          resetNextScheduled()
        }
      }
    }
  }

  def sendToGraphite( pickle: ByteString ): Unit = trace.block( "sendToGraphite" ) {
    socket foreach { s =>
      val out = s.getOutputStream
      out write pickle.toArray
      out.flush()
    }
  }
}
