package spotlight.publish

import java.net.{InetSocketAddress, Socket}

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.{CircuitBreaker, CircuitBreakerOpenException, ask}
import akka.stream.scaladsl.Flow
import akka.stream.{ActorAttributes, Supervision}
import akka.util.{ByteString, Timeout}
import org.joda.{time => joda}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{Meter, Timer}
import peds.akka.stream.Limiter
import peds.commons.log.Trace
import spotlight.model.timeseries.Topic
import spotlight.model.outlier.{OutlierPlan, Outliers}
import spotlight.protocol.PythonPickleProtocol


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
        logger.warn( "GraphitePublisher: Graphite circuit breaker opened: [{}]", ex.getMessage )
        Supervision.Resume
      }

      case ex => {
        logger.warn( "GraphitePublisher: restarting after error: [{}]", ex.getMessage )
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


class GraphitePublisher extends DenseOutlierPublisher {
  outer: GraphitePublisher.PublishProvider =>

  import GraphitePublisher._
  import OutlierPublisher._
  import context.dispatcher

  val trace = Trace[GraphitePublisher]

  val pickler: PythonPickleProtocol = new PythonPickleProtocol
  var socket: Option[Socket] = None //todo: if socket fails supervisor should restart actor
  var waitQueue: immutable.Queue[TopicPoint] = immutable.Queue.empty[TopicPoint]
  override val fillSeparation: FiniteDuration = outer.separation


  lazy val circuitOpenMeter: Meter = metrics.meter( "circuit", "open" )
  lazy val circuitPendingMeter: Meter = metrics.meter( "circuit", "pending" )
  lazy val circuitRequestsMeter: Meter = metrics.meter( "circuit", "requests" )
  lazy val publishedMeter: Meter = metrics.meter( "published" )
  lazy val publishTimer: Timer = metrics.timer( "publishing" )
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
    log.info( "{} dispatcher: [{}]", self.path, context.dispatcher )
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
//    log.error( "PUBLISH OUTLIER-CONTROLS: [{}]", o.algorithmControlBoundaries.mkString(","))
    val points = markPoints( o )
//    log.error( "PUBLISH-POINTS: [{}]", points.mkString("\n", ",", "\n") )
    waitQueue = points.foldLeft( waitQueue ){ _ enqueue _ }
    log.debug( "publish[{}]: added [{} / {}] to wait queue - now at [{}]", o.topic, o.anomalySize, o.size, waitQueue.size )
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
    log.error( "GraphitePublisher: marked data points: [{}]", dataPoints.mkString(","))
    val controlPoints = flattenControlPoints( o )
    log.error( "GraphitePublisher: control data points: [{}]", controlPoints.mkString(","))
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

      log.info( "flush: batch=[{}]", toBePublished.mkString(",") )
      val report = serialize( toBePublished:_* )
      breaker withSyncCircuitBreaker {
        circuitRequestsMeter.mark()
        publishTimer time { sendToGraphite( report ) }
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