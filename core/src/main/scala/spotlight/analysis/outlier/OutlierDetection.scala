package spotlight.analysis.outlier

import java.net.URI

import scala.concurrent.ExecutionContext
import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, Props}
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.pattern.AskTimeoutException
import akka.stream.Supervision
import akka.stream.actor.ActorSubscriberMessage

import scalaz.{-\/, \/, \/-}
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{Logger, StrictLogging}
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.commons.TryV
import peds.commons.identifier.ShortUUID
import peds.commons.log.Trace
import spotlight.model.timeseries._
import spotlight.model.outlier.{CorrelatedData, OutlierPlan, Outliers}


object OutlierDetection extends Instrumented with StrictLogging {
  def props( routerRef: ActorRef, scope: String ): Props = Props( new Default(routerRef, scope) )
  val DispatcherPath: String = "spotlight.dispatchers.outlier-detection-dispatcher"

  def name( scope: String ): String = "OutlierDetector-" + scope

  private class Default( routerRef: ActorRef, _scope: String ) extends OutlierDetection( _scope ) with ConfigurationProvider {
    override def router: ActorRef = routerRef
  }


  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierDetection] )

  lazy val timeoutMeter: Meter = metrics meter "timeouts"
  val totalOutstanding: Agent[Int] = Agent( 0 )( scala.concurrent.ExecutionContext.global )
  metrics.gauge( "outstanding" ){ totalOutstanding.get() }

  def incrementTotalOutstanding(): Unit = totalOutstanding send { _ + 1 }
  def decrementTotalOutstanding(): Unit = totalOutstanding send { _ - 1 }

  val decider: Supervision.Decider = {
    case ex: AskTimeoutException => {
      logger.error( "Detection stage timed out", ex )
      timeoutMeter.mark()
      Supervision.Resume
    }

    case _ => {
      Supervision.Stop
    }
  }


  object WatchPoints {
    val DetectionFlow: Symbol = Symbol( "detection" )
  }

  sealed trait DetectionProtocol
  case class DetectionResult( outliers: Outliers, correlationIds: Set[WorkId] ) extends DetectionProtocol
  case class UnrecognizedTopic( topic: Topic ) extends DetectionProtocol
  case class DetectionTimedOut( topic: Topic, plan: OutlierPlan ) extends DetectionProtocol


  val extractOutlierDetectionTopic: OutlierPlan.ExtractTopic = {
    case e @ Envelope( payload, _ ) if extractOutlierDetectionTopic isDefinedAt payload => extractOutlierDetectionTopic( payload )
    case m: OutlierDetectionMessage => Some(m.topic)
    case CorrelatedData( ts: TimeSeriesBase, _, _ ) => Some( ts.topic )
    case ts: TimeSeriesBase => Some(ts.topic)
    case t: Topic => Some(t)
  }

  trait ConfigurationProvider {
    def router: ActorRef
  }

  case class DetectionRequest private[OutlierDetection](
    subscriber: ActorRef,
    correlationIds: Set[WorkId],
    startNanos: Long = System.nanoTime()
  )

  private[OutlierDetection] object DetectionRequest {
    private val trace = Trace[DetectionResult.type]
    def from( m: OutlierDetectionMessage )( implicit wid: WorkId ): DetectionRequest = trace.block( "from" ) {
      logger.debug( "req = [{}]", m )
      logger.debug( "req.correlationIds = [{}]", m.correlationIds )
      logger.debug( "workId:[{}]", wid )
      val wids = if ( m.correlationIds.nonEmpty ) m.correlationIds else Set( wid )
      logger.debug( "request correlationIds = [{}]", wids )
      DetectionRequest( m.subscriber, wids )
    }
  }
}


class OutlierDetection( scope: String )
extends Actor
with EnvelopingActor
with InstrumentedActor
with ActorLogging {
  outer: OutlierDetection.ConfigurationProvider =>

  import OutlierDetection._

  val trace = Trace[OutlierDetection]

  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierDetection] )
  val detectionTimer: Timer = metrics timer "detect"

  type AggregatorSubscribers = Map[ActorRef, DetectionRequest]
  var outstanding: AggregatorSubscribers = Map.empty[ActorRef, DetectionRequest]
  def addToOutstanding( aggregator: ActorRef, request: DetectionRequest ): Unit = {
    outstanding += ( aggregator -> request )
    incrementTotalOutstanding()
  }

  def removeFromOutstanding( aggregator: ActorRef ): Unit = {
    outstanding -= aggregator
    decrementTotalOutstanding()
  }

  override def receive: Receive = LoggingReceive { around( detection() ) }

  def detection( isWaitingToComplete: Boolean = false ): Receive = {
    case m: OutlierDetectionMessage => {
      log.debug( "OutlierDetection received detection request[{}]: [{}:{}] subscriber=[{}]", workId, m.topic, m.plan, m.subscriber.path.name )
      dispatch( m, m.plan )( context.dispatcher ) match {
        case \/-( aggregator ) => {
          addToOutstanding( aggregator, DetectionRequest from m )
          stopIfFullyComplete( isWaitingToComplete )
        }
        case -\/( ex ) => log.error( ex, "OutlierDetector failed to create aggregator for topic:[{}]", m.topic )
      }
    }

    case result: Outliers if outstanding.contains( sender() ) => {
      log.debug( "OutlierDetection[{}} results: [{}:{}]", workId, result.topic, result.plan )
      val aggregator = sender()
      val request = outstanding( aggregator )
      log.debug( "OutlierDetection sending results for [{}] to subscriber:[{}]", workId, request.subscriber )
      request.subscriber !+ DetectionResult( result, request.correlationIds )
      removeFromOutstanding( aggregator )
      detectionTimer.update( System.nanoTime() - request.startNanos, scala.concurrent.duration.NANOSECONDS )
      updateScore( result )
      stopIfFullyComplete( isWaitingToComplete )
    }

    case timedOut @ OutlierQuorumAggregator.AnalysisTimedOut( topic, plan ) => {
      log.error( "quorum was not reached in time: [{}]", timedOut )
      val aggregator = sender()

      outstanding
      .get( aggregator )
      .foreach { request =>
        request.subscriber !+ timedOut
        removeFromOutstanding( aggregator )
      }

      stopIfFullyComplete( isWaitingToComplete )
    }

    case ActorSubscriberMessage.OnComplete => {
      log.info( "OutlierDetection[{}]: notified that stream is completed. waiting to complete", self.path )
      context become LoggingReceive { around( detection( isWaitingToComplete = true ) ) }
    }
  }


  def dispatch( m: OutlierDetectionMessage, p: OutlierPlan )( implicit ec: ExecutionContext ): TryV[ActorRef] = {
    log.debug( "OutlierDetection [{}] dispatching: [{}][{}:{}]", workId, m.topic, m.plan, m.plan.id )

    for {
      aggregator <- \/ fromTryCatchNonFatal {
        context.actorOf( OutlierQuorumAggregator.props(p, m.source), aggregatorNameForMessage(m, p) )
      }
    } yield {
      val history = updateHistory( m.source, p )
//      p.algorithms foreach { algo => router !+ DetectUsing(algo, aggregator, m, history, p.algorithmConfig) }
      p.algorithms foreach { algo => router.sendEnvelope( DetectUsing(algo, m, history, p.algorithmConfig) )( aggregator )}
      aggregator
    }
  }


  //todo store/hydrate
  var _history: Map[OutlierPlan.Scope, HistoricalStatistics] = Map.empty[OutlierPlan.Scope, HistoricalStatistics]
  def updateHistory( data: TimeSeriesBase, plan: OutlierPlan ): HistoricalStatistics = {
    val key = OutlierPlan.Scope( plan, data.topic )

    val initialHistory = _history get key getOrElse { HistoricalStatistics( 2, false ) }
    val sentHistory = data.points.foldLeft( initialHistory ) { (h, dp) => h :+ dp }
    val updatedHistory = sentHistory recordLastPoints data.points
    _history += key -> updatedHistory


    log.debug( "HISTORY for [{}]: [{}]", data.topic, sentHistory )
    sentHistory
  }

  var _score: Map[(OutlierPlan, Topic), (Long, Long)] = Map.empty[(OutlierPlan, Topic), (Long, Long)]
  val outlierLogger: Logger = Logger( LoggerFactory getLogger "Outliers" )

  def updateScore( os: Outliers ): Unit = {
    val key = (os.plan, os.topic)
    val s = _score.get( key ) map { case (o, t) =>
      ( o + os.anomalySize.toLong, t + os.size.toLong )
    } getOrElse {
      ( os.anomalySize.toLong, os.size.toLong )
    }
    _score += key -> s
    outlierLogger.debug(
      "\t\toutlier-rate[{}] = [{}%] [{} (count, points)] [threshold-data:[{}]]",
      s"${os.plan.name}:${os.topic}",
      (s._1.toDouble / s._2.toDouble).toString,
      (s._1.toString, s._2.toString),
      os.thresholdBoundaries
    )
  }

  private def stopIfFullyComplete( isWaitingToComplete: Boolean ): Unit = {
    if ( isWaitingToComplete && outstanding.isEmpty ) {
      log.info( "OutlierDetection[{}] finished outstanding work. stopping for completion", self.path )
      context stop self
    }
  }

  val fullExtractTopic: OutlierPlan.ExtractTopic = OutlierDetection.extractOutlierDetectionTopic orElse { case _ => None }

  def aggregatorNameForMessage( m: OutlierDetectionMessage, p: OutlierPlan ): String = {
    val extractedTopic = fullExtractTopic( m ) getOrElse {
      log.warning( "OutlierDetection failed to extract ID from message:[{}] and plan:[{}]", m, p )
      "__UNKNOWN_ID__"
    }

    val uuid = ShortUUID()
    val name = s"""quorum-${p.name}-${extractedTopic.toString}-${uuid.toString}"""

    if ( ActorPath isValidPathElement name ) name
    else {
      val blunted = new URI( "akka.tcp", name, null ).getSchemeSpecificPart.replaceAll( """[\.\[\];/?:@&=+$,]""", "_" )
      log.debug(
        "OutlierDetection attempting to dispatch to invalid aggregator\n" +
        " + (plan-name, extractedTopic, uuid):[{}]\n" +
        " + attempted name:{}\n" +
        " + blunted name:{}", 
        (p.name, extractedTopic.toString, uuid.toString),
        name,
        blunted
      )
      blunted
    }
  }
}
