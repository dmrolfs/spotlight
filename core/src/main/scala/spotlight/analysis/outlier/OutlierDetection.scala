package spotlight.analysis.outlier

import java.net.URI

import scala.concurrent.ExecutionContext
import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, Props}
import akka.event.LoggingReceive
import akka.pattern.AskTimeoutException
import akka.stream.Supervision
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import com.codahale.metrics.{Metric, MetricFilter}
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{Logger, StrictLogging}
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.commons.identifier.ShortUUID
import peds.commons.log.Trace
import spotlight.model.timeseries._
import spotlight.model.outlier.{OutlierPlan, Outliers}


object OutlierDetection extends StrictLogging with Instrumented {
  def props( routerRef: ActorRef ): Props = Props( new Default(routerRef) )

  private class Default( routerRef: ActorRef ) extends OutlierDetection with ConfigurationProvider {
    override def router: ActorRef = routerRef
  }

  lazy val timeoutMeter: Meter = metrics meter "timeouts"

  val decider: Supervision.Decider = {
    case ex: AskTimeoutException => {
      logger error s"Detection stage timed out on [${ex.getMessage}]"
      timeoutMeter.mark()
      Supervision.Resume
    }

    case _ => Supervision.Stop
  }


  object WatchPoints {
    val DetectionFlow: Symbol = Symbol( "detection" )
  }

  sealed trait DetectionProtocol
  case class DetectionResult( outliers: Outliers ) extends DetectionProtocol
  case class UnrecognizedTopic( topic: Topic ) extends DetectionProtocol
  case class DetectionTimedOut( topic: Topic, plan: OutlierPlan ) extends DetectionProtocol


  val extractOutlierDetectionTopic: OutlierPlan.ExtractTopic = {
    case m: OutlierDetectionMessage => Some(m.topic)
    case ts: TimeSeriesBase => Some(ts.topic)
    case t: Topic => Some(t)
  }

  trait ConfigurationProvider {
    def router: ActorRef
  }


  case class DetectionSubscriber( ref: ActorRef, startNanos: Long = System.nanoTime( ) )
}


class OutlierDetection
extends Actor
with InstrumentedActor
with ActorLogging {
  outer: OutlierDetection.ConfigurationProvider =>

  import OutlierDetection._

  val trace = Trace[OutlierDetection]

  override def preStart(): Unit = {
    initializeMetrics()
    log.info( "{} dispatcher: [{}]", self.path, context.dispatcher )
  }

  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierDetection] )
  val detectionTimer: Timer = metrics timer "detect"

  def initializeMetrics(): Unit = {
    stripLingeringMetrics()
    metrics.gauge( "outstanding" ) { outstanding.size }
  }

  def stripLingeringMetrics(): Unit = {
    metrics.registry.removeMatching(
      new MetricFilter {
        override def matches( name: String, metric: Metric ): Boolean = {
          name.contains( classOf[OutlierDetection].getName ) && name.contains( "outstanding" )
        }
      }
    )
  }

  type AggregatorSubscribers = Map[ActorRef, DetectionSubscriber]
  var outstanding: AggregatorSubscribers = Map.empty[ActorRef, DetectionSubscriber]

  override def receive: Receive = LoggingReceive { around( detection() ) }

  def detection( isWaitingToComplete: Boolean = false ): Receive = {
    case m: OutlierDetectionMessage =>  {
      log.debug( "OutlierDetection request: [{}:{}]", m.topic, m.plan )
      val requester = m.subscriber
      val aggregator = dispatch( m, m.plan )( context.dispatcher )
      outstanding += aggregator -> DetectionSubscriber( ref = requester )
    }

    case OnComplete => {
      log.info( "OutlierDetection[{}][{}]: notified that stream is completed. waiting to complete", self.path, this.## )
      context become LoggingReceive { around( detection(isWaitingToComplete = true) ) }
    }

    case result: Outliers if outstanding.contains( sender() ) => {
      log.debug( "OutlierDetection results: [{}:{}]", result.topic, result.plan )
      val aggregator = sender()
      val subscriber = outstanding( aggregator )
      log.debug( "OutlierDetecion sending results to subscriber:[{}]", subscriber.ref )
      subscriber.ref ! DetectionResult( result )
      outstanding -= aggregator
      detectionTimer.update( System.nanoTime() - subscriber.startNanos, scala.concurrent.duration.NANOSECONDS )
      updateScore( result )

      if ( isWaitingToComplete ) stopIfFullyComplete()
    }

    case to @ OutlierQuorumAggregator.AnalysisTimedOut( topic, plan ) => {
      log.error( "quorum was not reached in time: [{}]", to )
      outstanding -= sender()
      if ( isWaitingToComplete ) stopIfFullyComplete()
    }
  }

  private def stopIfFullyComplete(): Unit = {
    if ( outstanding.isEmpty ) {
      log.info( "OutlierDetection[{}][{}] finished outstanding work. stopping for completion", self.path, this.## )
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
//      val blunted = name
      val blunted = new URI( "akka.tcp", name, null ).getSchemeSpecificPart.replaceAll( """[\.\[\];/?:@&=+$,]""", "_" )
      log.warning( "OutlierDetection attempting to dispatch to invalid aggregator" )
      log.warning(
        " + (plan-name, extractedTopic, uuid):[{}]  name:{}  blunting to:{}", (p.name, extractedTopic.toString, uuid.toString)
      )
      log.warning( " + attempted name:{}", name )
      log.warning( " + blunted name:{}", blunted )
      blunted
    }
  }

  def dispatch( m: OutlierDetectionMessage, p: OutlierPlan )( implicit ec: ExecutionContext ): ActorRef = {
    log.debug( "OutlierDetection disptaching: [{}][{}:{}]", m.topic, m.plan, m.plan.id )
    val aggregator = context.actorOf( OutlierQuorumAggregator.props(p, m.source), aggregatorNameForMessage(m, p) )
    val history = updateHistory( m.source, p )

    p.algorithms foreach { a =>
      val detect = {
        DetectUsing(
          algorithm = a,
          aggregator = aggregator,
          payload = m,
          history = history,
          properties = p.algorithmConfig
        )
      }

      router ! detect
    }

    aggregator
  }


  //todo store/hydrate
  var _history: Map[HistoryKey, HistoricalStatistics] = Map.empty[HistoryKey, HistoricalStatistics]
  def updateHistory( data: TimeSeriesBase, plan: OutlierPlan ): HistoricalStatistics = {
    val key = HistoryKey( plan, data.topic )

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
}
