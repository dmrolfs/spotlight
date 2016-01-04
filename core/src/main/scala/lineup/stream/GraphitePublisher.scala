package lineup.stream

import java.io.OutputStream
import java.net.{InetSocketAddress, Socket, InetAddress}
import org.joda.time.DateTime
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.Limiter
import peds.commons.log.Trace

import scala.annotation.tailrec
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.{ ask, CircuitBreakerOpenException, CircuitBreaker }
import akka.util.{ByteString, Timeout}
import akka.stream.{ ActorAttributes, Supervision }
import akka.stream.scaladsl.Flow
import resource._
import com.typesafe.scalalogging.{ Logger, LazyLogging }
import org.joda.{ time => joda }
import org.slf4j.LoggerFactory
import lineup.model.outlier.{ NoOutliers, SeriesOutliers, Outliers }
import lineup.model.timeseries._
import lineup.stream.PythonPickleProtocol._
import scala.collection.immutable
import scala.util.Try
import scalaz.\/

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
//  var out: Option[OutputStream] = None

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

    case SendBatch => {
      import akka.pattern.pipe
      batchAndSend pipeTo self
      resetNextScheduled()
    }
  }

  override def publish( o: Outliers ): Unit = trace.block( s"publish($o)" ) {
    waitQueue = markPoints( o ).foldLeft( waitQueue ){ _.enqueue( _ ) }
    outlierLogger info o.toString
  }


  override def markPoints(o: Outliers): Seq[(String, DateTime, Double)] = trace.block(s"markPoints($o)") { super.markPoints( o ) }

  override def mark(o: Outliers): Seq[DataPoint] = trace.block( s"mark($o)" ) { super.mark( o ) }

  def cancelNextSchedule(): Unit = trace.block( "cancelNextSchedule" ) {
    _nextScheduled foreach { _.cancel() }
    _nextScheduled = None
  }

  def resetNextScheduled(): Option[Cancellable] = trace.block( s"resetNextSchedule" ) {
    cancelNextSchedule()
    _nextScheduled = Some( context.system.scheduler.scheduleOnce(1.second, self, SendBatch) )
    _nextScheduled
  }

  def batchAndSend: Future[SendBatch.type] = trace.block( "batchAndSend" ) {
    val (toBePublished, remainingQueue) = waitQueue splitAt batchSize
    waitQueue = remainingQueue

    val report = pickler.pickleFlattenedTimeSeries( toBePublished:_* )
    breaker withCircuitBreaker {
      Future {
        sendToGraphite( report )
        SendBatch
      }
    }
  }

  def sendToGraphite( pickle: ByteString ): Unit = trace.block( "sendToGraphite" ) {
    require( pickle == pickler.pickle( ("foo", 100L, "0.0") ) )
    socket foreach { s =>
      val out = s.getOutputStream
      out write pickle.toArray
      out.flush()
    }
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

      log info s"""flush: batch=[${toBePublished.mkString(",")}]"""
//WORK HERE
      val report = pickler.pickleFlattenedTimeSeries( toBePublished:_* )
//      val report = pickler.pickle( ("name", 100L, "value") )
      log info s"""batch pickle: ${report.decodeString("UTF-8")}"""
      breaker withSyncCircuitBreaker { sendToGraphite( report ) }
      flush()
    }
  }

}
