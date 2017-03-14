package spotlight.publish

import java.net.{ InetSocketAddress, Socket }

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.{ Failure, Success, Try }
import akka.actor._
import akka.agent.Agent
import akka.dispatch.MessageDispatcher
import akka.event.LoggingReceive
import akka.pattern.{ CircuitBreaker, pipe }
import akka.stream.actor.{ ActorSubscriberMessage, MaxInFlightRequestStrategy, RequestStrategy }
import akka.util.ByteString
import shapeless.syntax.typeable._
import com.codahale.metrics.{ Metric, MetricFilter }
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.joda.{ time ⇒ joda }
import com.typesafe.scalalogging.{ LazyLogging, Logger }
import org.slf4j.LoggerFactory
import nl.grons.metrics.scala.{ Meter, Timer }
import spotlight.model.timeseries.Topic
import spotlight.model.outlier.{ AnalysisPlan, Outliers }
import spotlight.protocol.PythonPickleProtocol

/** Created by rolfsd on 12/29/15.
  */
object GraphitePublisher extends LazyLogging {
  def props( makePublisher: ⇒ GraphitePublisher with PublishProvider ): Props = Props( makePublisher )

  val DispatcherPath: String = "spotlight.dispatchers.publishing-dispatcher"

  sealed trait GraphitePublisherProtocol
  case object Open extends GraphitePublisherProtocol
  case object Close extends GraphitePublisherProtocol
  case object Flush extends GraphitePublisherProtocol
  case object SendBatch extends GraphitePublisherProtocol
  case object ReplenishTokens extends GraphitePublisherProtocol

  case object Opened extends GraphitePublisherProtocol
  case object Closed extends GraphitePublisherProtocol
  case class Flushed( performed: Boolean ) extends GraphitePublisherProtocol
  case class Recover( points: Seq[OutlierPublisher.TopicPoint] ) extends GraphitePublisherProtocol

  trait PublishProvider {
    def maxOutstanding: Int
    def overflowFactor: Double = 0.75
    def batchFairnessDivisor: Int = 100
    def separation: FiniteDuration = 10.seconds
    def destinationAddress: InetSocketAddress
    def createSocket( address: InetSocketAddress ): Socket
    def batchSize: Int
    def batchInterval: FiniteDuration = 5.seconds
    def publishingTopic( p: AnalysisPlan, t: Topic ): Topic = s"${p.name}.${t}"
    def publishThresholdBoundaries( p: AnalysisPlan, algorithm: String ): Boolean = {
      val PublishPath = "publish-threshold"
      val algorithmConfig = p.algorithms.getOrElse( algorithm, ConfigFactory.empty )
      algorithmConfig.as[Option[Boolean]]( PublishPath ) getOrElse false
    }
  }

  private[publish] sealed trait BreakerStatus
  case object BreakerOpen extends BreakerStatus { override def toString: String = "open" }
  case object BreakerPending extends BreakerStatus { override def toString: String = "pending" }
  case object BreakerClosed extends BreakerStatus { override def toString: String = "closed" }
}

class GraphitePublisher extends DenseOutlierPublisher { outer: GraphitePublisher.PublishProvider ⇒
  import GraphitePublisher._
  import OutlierPublisher._
  import context.dispatcher

  val debugLogger = Logger( LoggerFactory getLogger "Debug" )

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy( outer.maxOutstanding ) {
    override def inFlightInternally: Int = waitQueue.size
  }

  val pickler: PythonPickleProtocol = new PythonPickleProtocol
  var socket: Option[Socket] = None
  var waitQueue: immutable.Queue[TopicPoint] = immutable.Queue.empty[TopicPoint]
  override val fillSeparation: FiniteDuration = outer.separation

  lazy val circuitOpenMeter: Meter = metrics.meter( "circuit", "open" )
  lazy val circuitPendingMeter: Meter = metrics.meter( "circuit", "pending" )
  lazy val circuitClosedMeter: Meter = metrics.meter( "circuit", "closed" )
  lazy val circuitRequestsMeter: Meter = metrics.meter( "circuit", "requests" )
  lazy val bytesPublishedMeter: Meter = metrics.meter( "bytes", "published" )
  lazy val bytesRecoveredMeter: Meter = metrics.meter( "bytes", "recovered" )
  lazy val publishingTimer: Timer = metrics.timer( "publishing" )
  lazy val publishRequestMeter: Meter = metrics.meter( "request", "publish" )
  lazy val sendBatchRequestMeter: Meter = metrics.meter( "request", "sendBatch" )

  val publishDispatcher: MessageDispatcher = context.system.dispatchers lookup DispatcherPath

  val circuitStatus: Agent[BreakerStatus] = Agent[BreakerStatus]( BreakerClosed )( publishDispatcher )

  val publishLogger: Logger = Logger( LoggerFactory getLogger "Publish" )

  var lastQueueClearingMillis: Long = System.currentTimeMillis()
  def markClearing(): Unit = lastQueueClearingMillis = System.currentTimeMillis()
  def sinceLastClearing(): FiniteDuration = FiniteDuration( System.currentTimeMillis() - lastQueueClearingMillis, MILLISECONDS )

  def initializeMetrics(): Unit = {
    stripLingeringMetrics()
    metrics.gauge( "waiting" ) { waitQueue.size }
    metrics.gauge( "circuit", "breaker" ) { circuitStatus().toString }
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

  val BreakerReset: FiniteDuration = 30.seconds

  val breaker: CircuitBreaker = {
    //todo currently uses GraphitePublisher's dispatcher for state transitions; maybe used publishDispatcher executor instead?
    //todo would need to protect socket in Agent?
    CircuitBreaker(
      context.system.scheduler,
      maxFailures = 5,
      callTimeout = 10.seconds,
      resetTimeout = BreakerReset
    )
      .onOpen { notifyOnOpenCircuit() }
      .onHalfOpen { notifyOnPendingCircuit() }
      .onClose { notifyOnClosedCircuit() }
  }

  def notifyOnOpenCircuit(): Unit = {
    outer.circuitStatus send { s ⇒
      outer.circuitOpenMeter.mark()
      val newStatus = BreakerOpen
      log.info( "setting graphite circuit breaker from [{}] to [{}]", s, newStatus )
      newStatus
    }
    val oldConnected = isConnected
    val socketClose = {
      Try {
        socket foreach { _.close() }
      } recoverWith {
        case ex ⇒ {
          log.info( "closing socket upon open circuit caused error: [{}]", ex.getMessage )
          Failure( ex )
        }
      }
    }

    outer.socket = None
    val reopened = open()

    if ( reopened.isSuccess && isConnected ) {
      log.info(
        "graphite connection is down, closed[{}], reconnection succeeded " +
          "and circuit breaker will remain open for [{}] before trying again",
        socketClose,
        BreakerReset.toCoarsest
      )
    } else {
      log.error(
        "graphite connection is down, closed[{}], reconnection failed " +
          "and circuit breaker will remain open for [{}] before trying again",
        socketClose,
        BreakerReset.toCoarsest
      )
    }
  }

  def notifyOnPendingCircuit(): Unit = {
    log info "attempting to publish to graphite and close circuit breaker"
    outer.circuitStatus alter { s ⇒
      outer.circuitPendingMeter.mark()
      val newStatus = BreakerPending
      log.info( "setting graphite circuit breaker from [{}] to [{}]", s, newStatus )
      newStatus
    } map { s ⇒
      if ( !isConnected ) {
        log.info( "[{}] is not connected attempting to reopen socket to Graphite", s )
        val reopened = open()
        if ( reopened.isSuccess && isConnected ) {
          log.info( "[{}] graphite socket reopened", s )
        } else {
          log.warning( "[{}] failed to reopen graphite socket", s )
        }
      }
    }
  }

  def notifyOnClosedCircuit(): Unit = {
    outer.circuitStatus send { s ⇒
      outer.circuitClosedMeter.mark()
      val newStatus = BreakerClosed
      log.info( "setting graphite circuit breaker from [{}] to [{}]", s, newStatus )
      newStatus
    }
    log info "graphite connection established and circuit breaker is closed"
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

  override def receive: Receive = LoggingReceive { around( closed ) }

  val closed: Receive = {
    case Open ⇒ {
      open()
      sender() ! Opened
      context become LoggingReceive { around( opened ) }
    }

    case Close ⇒ {
      close()
      sender() ! Closed
    }
  }

  val opened: Receive = {
    case ActorSubscriberMessage.OnNext( outliers ) ⇒ {
      outliers.cast[Outliers] match {
        case Some( os ) ⇒ {
          log.debug( "GraphitePublisher received OnNext. publishing outliers" )
          publish( os )
        }

        case None ⇒ log.warning( "GraphitePublisher received UNKNOWN OnNext with:[{}]", outliers )
      }
    }

    case ActorSubscriberMessage.OnComplete ⇒ {
      log debug "GraphitePublisher closing on completed stream"
      close()
    }

    case ActorSubscriberMessage.OnError( ex ) ⇒ {
      log.error( ex, "GraphitePublisher closing on errored stream: [{}]", ex.getMessage )
      close()
    }

    case Recover( points ) ⇒ {
      bytesRecoveredMeter mark points.size
      waitQueue = points ++: waitQueue

      circuitStatus.future() map { status ⇒
        log.info(
          "GraphitePublisher recovered [{}] points delayed by circuit failure [{}]",
          points.size.toString,
          status
        )
      }
    }

    case Publish( outliers ) ⇒ {
      log.debug( "GraphitePublisher received Publish([{}])", outliers )
      publish( outliers )
      sender() ! Published( outliers ) //todo send when all corresponding marks are sent to graphite
    }

    case Open ⇒ {
      log.debug( "GraphitePublisher received Open" )
      open()
      sender() ! Opened
    }

    case Close ⇒ {
      log.debug( "GraphitePublisher received Close" )
      close()
      sender() ! Closed
      context stop self
    }

    case Flush ⇒ {
      log.debug( "GraphitePublisher received Flush" )
      val source = sender()
      flush( Some( source ) )
    }

    case SendBatch ⇒ {
      //      debugLogger.info(
      //        "GraphitePublisher received SendBatch: next schedule in [{}]; last-clearing?[{}] wait-queue?[{}]",
      //        batchInterval.toCoarsest,
      //        (sinceLastClearing() > batchInterval).toString,
      //        (waitQueue.size >= outer.batchSize).toString
      //      )
      batchAndSend()
    }
  }

  override def publish( o: Outliers ): Unit = {
    publishRequestMeter.mark()
    val points = markPoints( o )
    waitQueue = points.foldLeft( waitQueue ) { _ enqueue _ }
    log.debug( "publish[{}]: added [{} / {}] to wait queue - now at [{}]", o.topic, o.anomalySize, o.size, waitQueue.size )
    fairSharing()
  }

  def fairSharing(): Unit = {
    if ( publishRequestMeter.count % outer.batchFairnessDivisor == 0 ) {
      //      debugLogger.info( "GraphitePublisher SendBatch out of fairness" )
      self ! SendBatch
    }

    val overflow = outer.maxOutstanding * outer.overflowFactor
    if ( waitQueue.size > overflow ) {
      debugLogger.info( "GraphitePublisher flushing overflow" )
      flush()
    }
  }

  object ControlLabeling {
    type ControlLabel = String ⇒ String
    val FloorLabel: ControlLabel = ( algorithm: String ) ⇒ algorithm + ".floor."
    val ExpectedLabel: ControlLabel = ( algorithm: String ) ⇒ algorithm + ".expected."
    val CeilingLabel: ControlLabel = ( algorithm: String ) ⇒ algorithm + ".ceiling."
  }

  def flattenThresholds( o: Outliers ): Seq[TopicPoint] = {
    import ControlLabeling._
    def thresholdTopic( algorithm: String, labeling: ControlLabel ): Topic = {
      outer.publishingTopic( o.plan, labeling( algorithm ) + o.topic )
    }

    o.thresholdBoundaries.toSeq flatMap {
      case ( algorithm, thresholds ) if outer.publishThresholdBoundaries( o.plan, algorithm ) ⇒ {
        val floorTopic = thresholdTopic( algorithm, FloorLabel )
        val expectedTopic = thresholdTopic( algorithm, ExpectedLabel )
        val ceilingTopic = thresholdTopic( algorithm, CeilingLabel )
        thresholds flatMap { c ⇒
          Seq( floorTopic, expectedTopic, ceilingTopic )
            .zipAll( Seq.empty[joda.DateTime], Topic( "" ), c.timestamp ) // tuple topics with c.timestamp
            .zip( Seq( c.floor, c.expected, c.ceiling ) ) // add threshold values to tuple
            .map { case ( ( topic, ts ), value ) ⇒ value map { v ⇒ ( topic, ts, v ) } } // make tuple
            .flatten // trim out unprovided threshold values
        }
      }

      case ( algorithm, controlBoundaries ) ⇒ Seq.empty[TopicPoint]
    }
  }

  override def markPoints( o: Outliers ): Seq[TopicPoint] = {
    val dataPoints = super.markPoints( o ) map { case ( t, ts, v ) ⇒ ( outer.publishingTopic( o.plan, t ), ts, v ) }
    log.debug( "GraphitePublisher: marked data points: [{}]", dataPoints.mkString( "," ) )
    val thresholds = flattenThresholds( o )
    log.debug( "GraphitePublisher: threshold data points: [{}]", thresholds.mkString( "," ) )
    dataPoints ++ thresholds
  }

  def isConnected: Boolean = socket map { s ⇒ s.isConnected && !s.isClosed } getOrElse { false }

  def open(): Try[Unit] = {
    val result = {
      if ( isConnected ) Try { () }
      else {
        val socketOpen = Try { createSocket( destinationAddress ) }
        socket = socketOpen.toOption
        socketOpen match {
          case Success( s ) ⇒ log.info( "{} opened [{}] [{}]", self.path, socket, socketOpen )
          case Failure( ex ) ⇒ {
            log.error( "open failed - could not connect with graphite server [{}]: [{}]", destinationAddress, ex.getMessage )
          }
        }

        socketOpen map { s ⇒ () }
      }
    }

    result foreach { _ ⇒ scheduleBatches() }
    result
  }

  def close(): Unit = {
    val ctx = context
    val flushStatus = Await.ready( flush(), 2.minutes ) //todo make atMost configurable
    val socketClose = Try { socket foreach { _.close() } }
    socket = None
    cancelScheduledBatches()
    ctx become LoggingReceive { around( closed ) }
    log.info( "{} flushed:[{}] closed socket:[{}] = [{}]", flushStatus.value, self.path, socket, socketClose )
  }

  final def flush( source: Option[ActorRef] = None ): Future[Boolean] = {
    @tailrec def loop( status: BreakerStatus, count: Int = 1 ): Boolean = {
      def execute(): Unit = {
        val ( toBePublished, remainingQueue ) = waitQueue splitAt outer.batchSize
        waitQueue = remainingQueue

        reportDuplicates( toBePublished )

        publishLogger.info(
          "Flush[{}]: toBePublished:[{}]\tremainingQueue:[{}]\tbatchSize:[{}]",
          count.toString,
          toBePublished.size.toString,
          remainingQueue.size.toString,
          outer.batchSize.toString
        )

        toBePublished foreach { tbp ⇒
          publishLogger.info( "[{}] {}: timestamp:[{}] value:[{}]", count.toString, tbp._1, tbp._2, tbp._3.toString )
        }

        log.debug( "flush: batch=[{}]", toBePublished.mkString( "," ) )
        val report = serialize( toBePublished: _* )
        breaker withSyncCircuitBreaker {
          circuitRequestsMeter.mark()
          sendToGraphite( report )
        }
      }

      if ( waitQueue.isEmpty ) {
        log.info( "{} flushing complete after [{}] loops", self.path, count )
        true
      } else {
        status match {
          case BreakerClosed ⇒ {
            execute()
            loop( status, count + 1 )
          }

          case BreakerPending ⇒ {
            log.info( "attempting to flush to graphite under circuit[{}]", BreakerPending )
            execute()
            loop( status, count + 1 )
          }

          case s ⇒ {
            debugLogger.info( "GraphitePublisher[{}] NOT flushing since circuit status = [{}]", count.toString, circuitStatus() )
            false
          }
        }
      }
    }

    val flushStatus = circuitStatus.future() map { status ⇒ loop( status ) }
    val flushed = flushStatus map { Flushed }
    source foreach { flushed pipeTo _ }
    flushStatus
  }

  def serialize( payload: TopicPoint* ): ByteString = pickler.pickleFlattenedTimeSeries( payload: _* )

  def batchAndSend(): Unit = {
    sendBatchRequestMeter.mark()

    def execute(): Unit = {
      val waiting = waitQueue.size
      val ( toBePublished, remainingQueue ) = waitQueue splitAt batchSize
      waitQueue = remainingQueue

      if ( toBePublished.nonEmpty ) {
        publishLogger.info(
          "BatchAndSend: from waiting:[{}] toBePublished:[{}]\tremainingQueue:[{}]",
          waiting.toString,
          s"${toBePublished.size} / ${outer.batchSize}",
          remainingQueue.size.toString
        )

        reportDuplicates( toBePublished )

        val report = pickler.pickleFlattenedTimeSeries( toBePublished: _* )

        breaker
          .withCircuitBreaker {
            Future {
              circuitRequestsMeter.mark()
              sendToGraphite( report )
              self ! SendBatch
            }( publishDispatcher ) map { _ ⇒
              toBePublished foreach { tbp ⇒ publishLogger.info( "{}: timestamp:[{}] value:[{}]", tbp._1, tbp._2, tbp._3.toString ) }
            }
          }
          .recover {
            case ex ⇒ {
              self ! Recover( toBePublished )

              circuitStatus.future() map { status ⇒
                log.info(
                  "GraphitePublisher recovering [{}] points due to circuit failure [{}]. ex message:[{}]",
                  toBePublished.size.toString,
                  status,
                  ex.getMessage
                )
              }
            }
          }
      }

      if ( waitQueue.isEmpty ) {
        publishLogger.info( "BatchAndSend: cleared no toBePublish. remainingQueue:[{}]", waitQueue.size.toString )
        markClearing()
      }
    }

    circuitStatus() match {
      case BreakerClosed ⇒ execute()

      case BreakerPending ⇒ {
        log.info( "attempting to send to graphite under circuit[{}]", BreakerPending )
        execute()
      }

      case status ⇒ debugLogger.info( "GraphitePublisher NOT SENDING due to circuit status = [{}]", status )
    }
  }

  def sendToGraphite( pickle: ByteString ): Unit = {
    bytesPublishedMeter mark pickle.size

    publishingTimer time {
      socket foreach { s ⇒
        val out = s.getOutputStream
        out write pickle.toArray
        out.flush()
      }
    }
  }

  //  val seenMax: Int = 1000000
  //  var seen: BloomFilter[(Topic, joda.DateTime)] = BloomFilter[(Topic, joda.DateTime)]( maxFalsePosProbability = 0.01, seenMax )
  def reportDuplicates( toBePublished: Iterable[( Topic, joda.DateTime, Double )] ): Unit = {
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
