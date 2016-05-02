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
  case class Recover( points: Seq[OutlierPublisher.TopicPoint] ) extends GraphitePublisherProtocol


  trait PublishProvider  {
    def maxOutstanding: Int
    def overflowFactor: Double = 0.75
    def batchFairnessDivisor: Int = 100
    def separation: FiniteDuration = 10.seconds
    def destinationAddress: InetSocketAddress
    def createSocket( address: InetSocketAddress ): Socket
    def batchSize: Int
    def batchInterval: FiniteDuration = 5.seconds
    def publishingTopic( p: OutlierPlan, t: Topic ): Topic = s"${p.name}.${t}"
    def publishAlgorithmControlBoundaries( p: OutlierPlan, algorithm: Symbol ): Boolean = {
      val publishKey = algorithm.name + ".publish-controls"
      if ( p.algorithmConfig hasPath publishKey ) p.algorithmConfig getBoolean publishKey
      else {
        //todo dmr remove after determining prob with control publishing
        logger.warn(
          "Plan[{}] algorithm[{}] will not publishing controls; algo-config:[{}]",
          p.name,
          algorithm.name,
          p.algorithmConfig
        )
        false
      }
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

  val debugLogger = Logger( LoggerFactory getLogger "Debug" )

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
  lazy val publishRequestMeter: Meter = metrics.meter( "request", "publish" )
  lazy val sendBatchRequestMeter: Meter = metrics.meter( "request", "sendBatch" )
  var gaugeStatus: BreakerStatus = BreakerClosed

  val publishLogger: Logger = Logger( LoggerFactory getLogger "Publish" )

  val publishDispatcher = context.system.dispatchers.lookup( "publish-dispatcher" )

  var lastQueueClearingNanos: Long = System.nanoTime()
  def markClearing(): Unit = lastQueueClearingNanos = System.nanoTime()
  def sinceLastClearing(): FiniteDuration = FiniteDuration( System.nanoTime() - lastQueueClearingNanos, NANOSECONDS )


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

    //todo currently uses GraphitePublisher's dispatcher for state transitions; maybe used publishDispatcher executor instead?
    CircuitBreaker(
      context.system.scheduler,
      maxFailures = 5,
      callTimeout = 10.seconds,
      resetTimeout = reset
    )
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
      ()
    }
    .onHalfOpen {
      circuitPendingMeter.mark()
      gaugeStatus = BreakerPending
      log info "attempting to publish to graphite and close circuit breaker"
      ()
    }
    .onClose {
      circuitClosedMeter.mark()
      gaugeStatus = BreakerClosed
      log info "graphite connection established and circuit breaker is closed"
      ()
    }
  }

  var _scheduled: Option[Cancellable] = None
  def scheduleBatches(): Option[Cancellable] = {
    _scheduled foreach { _.cancel() }
    _scheduled = Option( context.system.scheduler.schedule( outer.batchInterval, outer.batchInterval, self, SendBatch ) )
    _scheduled
  }

  def cancelScheduledBatches(): Unit = {
    _scheduled foreach { _.cancel() }
    _scheduled = None
  }


  override def preStart(): Unit = {
    initializeMetrics()
    log.debug( "{} dispatcher: [{}]", self.path, context.dispatcher )
    log.info( "GraphitePublisher started with maxOutstanding:[{}]", outer.maxOutstanding )
    self ! Open
  }

  override def postStop(): Unit = {
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

    case Recover( points ) => {
      log.info( "GraphitePublisher recovered [{}] points delayed by open publish circuit", points.size.toString )
      waitQueue = points ++: waitQueue
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
      debugLogger.info(
        "GraphitePublisher received SendBatch: next schedule in [{}]; last-clearing?[{}] wait-queue?[{}]",
        batchInterval.toCoarsest,
        (sinceLastClearing() > batchInterval).toString,
        (waitQueue.size >= outer.batchSize).toString
      )
      batchAndSend()
    }
  }

  override def publish( o: Outliers ): Unit = {
    publishRequestMeter.mark()
    val points = markPoints( o )
    waitQueue = points.foldLeft( waitQueue ){ _ enqueue _ }
    log.debug( "publish[{}]: added [{} / {}] to wait queue - now at [{}]", o.topic, o.anomalySize, o.size, waitQueue.size )

    if ( publishRequestMeter.count % outer.batchFairnessDivisor == 0 ) {
      debugLogger.info( "GraphitePublisher SendBatch out of fairness" )
      self ! SendBatch
    }

    val overflow = outer.maxOutstanding * outer.overflowFactor
    if ( waitQueue.size > overflow ) {
      debugLogger.info( "GraphitePublisher flushing overflow" )
      flush()
    }
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
    val result = {
      if ( isConnected ) Try { () }
      else {
        val socketOpen = Try { createSocket( destinationAddress ) }
        socket = socketOpen.toOption
        socketOpen match {
          case Success(s) => log.info( "{} opened [{}] [{}]", self.path, socket, socketOpen )
          case Failure(ex) => {
            log.error( "open failed - could not connect with graphite server [{}]: [{}]", destinationAddress, ex.getMessage )
          }
        }

        socketOpen map { s => () }
      }
    }

    result foreach { _ => scheduleBatches() }
    result
  }

  def close(): Unit = {
    val f = Try { flush() }
    val socketClose = Try { socket foreach { _.close() } }
    socket = None
    cancelScheduledBatches()
    context become around( closed )
    log.info( "{} closed [{}] [{}]", self.path, socket, socketClose )
  }

  @tailrec final def flush(): Unit = {
    if ( waitQueue.isEmpty ) {
      log.info( "{} flushing complete", self.path )
      ()
    }
    else {
      val (toBePublished, remainingQueue) = waitQueue splitAt outer.batchSize
      waitQueue = remainingQueue

      reportDuplicates( toBePublished )

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
    sendBatchRequestMeter.mark()

    if ( gaugeStatus == BreakerClosed ) {
      val waiting = waitQueue.size
      val (toBePublished, remainingQueue) = waitQueue splitAt batchSize
      waitQueue = remainingQueue

      if ( toBePublished.nonEmpty ) {
        publishLogger.info(
          "BatchAndSend: from waiting:[{}] toBePublished:[{}]\tremainingQueue:[{}]",
          waiting.toString,
          s"${toBePublished.size} / ${outer.batchSize}",
          remainingQueue.size.toString
        )

        reportDuplicates( toBePublished )

        val report = pickler.pickleFlattenedTimeSeries( toBePublished:_* )

        breaker withCircuitBreaker {
          val body = Future {
            circuitRequestsMeter.mark()
            sendToGraphite( report )
            self ! SendBatch
          }( publishDispatcher ) map { _ =>
            toBePublished foreach { tbp => publishLogger.info( "{}: timestamp:[{}] value:[{}]", tbp._1, tbp._2, tbp._3.toString) }
          }

          body onFailure {
            case ex => {
              log.warning( "GraphitePublisher recovering [{}] points due to open publish circuit", toBePublished.size.toString )
              self ! Recover( toBePublished )
            }
          }

          body
        }
      }

      if ( waitQueue.isEmpty ) {
        publishLogger.info( "BatchAndSend: cleared no toBePublish. remainingQueue:[{}]", waitQueue.size.toString )
        markClearing()
      }
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


//  val seenMax: Int = 1000000
//  var seen: BloomFilter[(Topic, joda.DateTime)] = BloomFilter[(Topic, joda.DateTime)]( maxFalsePosProbability = 0.01, seenMax )
  def reportDuplicates( toBePublished: Iterable[(Topic, joda.DateTime, Double)] ): Unit = {
//    toBePublished foreach { tbp =>
//      if ( seen.size == seenMax ) log.warning( "[GraphitePublisher] SEEN filled" )
//      if ( seen.size < seenMax ) {
//        val (t,dt, v) = tbp
//        if ( seen has_? (t, dt) ) log.warning( "[GraphitePublisher] Possible  duplicate: [{}]", tbp)
//        else seen += (t, dt)
//      }
//    }
  }
}
