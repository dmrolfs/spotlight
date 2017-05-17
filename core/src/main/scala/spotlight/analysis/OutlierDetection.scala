package spotlight.analysis

import java.net.URI
import akka.actor.{ Actor, ActorPath, ActorRef, Props }
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.pattern.AskTimeoutException
import akka.stream.Supervision
import akka.stream.actor.ActorSubscriberMessage
import cats.syntax.either._
import nl.grons.metrics.scala.{ Meter, MetricName, Timer }
import com.persist.logging._
import omnibus.akka.envelope._
import omnibus.akka.metrics.{ Instrumented, InstrumentedActor }
import omnibus.commons.ErrorOr
import omnibus.commons.identifier.ShortUUID
import spotlight.EC
import spotlight.model.timeseries._
import spotlight.model.outlier.{ AnalysisPlan, CorrelatedData, Outliers }

object OutlierDetection extends Instrumented with ClassLogging {
  sealed trait DetectionProtocol
  case class DetectionResult( outliers: Outliers, correlationIds: Set[WorkId] ) extends DetectionProtocol
  case class UnrecognizedTopic( topic: Topic ) extends DetectionProtocol
  case class DetectionTimedOut( source: TimeSeriesBase, plan: AnalysisPlan ) extends DetectionProtocol

  def props( routerRef: ActorRef ): Props = Props( new Default( routerRef ) )

  def name( suffix: String ): String = "detector" //"detector:" + suffix //todo suffix is redundant given it's inclusion in foreman naming

  val DispatcherPath: String = "spotlight.dispatchers.outlier-detection-dispatcher"

  private class Default( override val router: ActorRef ) extends OutlierDetection with ConfigurationProvider

  trait ConfigurationProvider {
    def router: ActorRef
  }

  val decider: Supervision.Decider = {
    case ex: AskTimeoutException ⇒ {
      log.error( "Detection stage timed out", ex )
      timeoutMeter.mark()
      Supervision.Resume
    }

    case _ ⇒ {
      Supervision.Stop
    }
  }

  val extractOutlierDetectionTopic: AnalysisPlan.ExtractTopic = {
    case e @ Envelope( payload, _ ) if extractOutlierDetectionTopic isDefinedAt payload ⇒ extractOutlierDetectionTopic( payload )
    case m: OutlierDetectionMessage ⇒ Some( m.topic )
    case CorrelatedData( ts: TimeSeriesBase, _, _ ) ⇒ Some( ts.topic )
    case ts: TimeSeriesBase ⇒ Some( ts.topic )
    case t: Topic ⇒ Some( t )
  }

  case class DetectionRequest private[OutlierDetection] (
    subscriber: ActorRef,
    correlationIds: Set[WorkId],
    startMillis: Long = System.currentTimeMillis()
  )

  private[OutlierDetection] object DetectionRequest {
    def from( m: OutlierDetectionMessage, subscriber: ActorRef )( implicit wid: WorkId ): DetectionRequest = {
      val wids = if ( m.correlationIds.nonEmpty ) m.correlationIds else Set( wid )
      DetectionRequest( subscriber, wids )
    }
  }

  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierDetection] )

  lazy val timeoutMeter: Meter = metrics meter "timeouts"
  val totalOutstanding: Agent[Int] = Agent( 0 )( scala.concurrent.ExecutionContext.global )
  metrics.gauge( "outstanding" ) { totalOutstanding.get() }

  def incrementTotalOutstanding(): Unit = totalOutstanding send { _ + 1 }
  def decrementTotalOutstanding(): Unit = totalOutstanding send { _ - 1 }

  object WatchPoints {
    val DetectionFlow: Symbol = Symbol( "detection" )
  }
}

class OutlierDetection extends Actor with EnvelopingActor with InstrumentedActor with ActorLogging {
  outer: OutlierDetection.ConfigurationProvider ⇒

  import OutlierDetection._

  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierDetection] )
  val detectionTimer: Timer = metrics timer "detect"

  type AggregatorSubscribers = Map[ActorRef, DetectionRequest]
  var outstanding: AggregatorSubscribers = Map.empty[ActorRef, DetectionRequest]
  def addToOutstanding( aggregator: ActorRef, request: DetectionRequest ): Unit = {
    outstanding += ( aggregator → request )
    incrementTotalOutstanding()
  }

  def removeFromOutstanding( aggregator: ActorRef ): Unit = {
    outstanding -= aggregator
    decrementTotalOutstanding()
  }

  override def receive: Receive = LoggingReceive { around( detection() ) }

  def detection( isWaitingToComplete: Boolean = false ): Receive = {
    case m: OutlierDetectionMessage ⇒ {
      log.debug( Map( "@msg" → "received detection request", "topic" → m.topic.toString, "plan" → m.plan.name ), id = requestId )
      dispatch( m, m.plan )( context.dispatcher ) match {
        case Right( aggregator ) ⇒ {
          addToOutstanding( aggregator, DetectionRequest.from( m, sender() ) )
          stopIfFullyComplete( isWaitingToComplete )
        }
        case Left( ex ) ⇒ {
          log.error( Map( "@msg" → "failed to create aggregator", "topic" → m.topic.toString ), ex = ex, id = requestId )
        }
      }
    }

    case result: Outliers if outstanding.contains( sender() ) ⇒ {
      val aggregator = sender()
      val request = outstanding( aggregator )
      log.debug(
        msg = Map(
          "@msg" → "received result",
          "plan" → result.plan.name,
          "topic" → result.topic.toString,
          "subscriber" → request.subscriber.path
        ),
        id = requestId
      )
      request.subscriber !+ DetectionResult( result, request.correlationIds )
      removeFromOutstanding( aggregator )
      detectionTimer.update( System.currentTimeMillis() - request.startMillis, scala.concurrent.duration.MILLISECONDS )
      //todo: leave off until det. desried means to regulate:      updateScore( result )
      stopIfFullyComplete( isWaitingToComplete )
    }

    case timedOut: DetectionTimedOut ⇒ {
      log.error( Map( "@msg" → "quorum not reached in time", "notice" → timedOut.toString ), id = requestId )
      val aggregator = sender()

      outstanding
        .get( aggregator )
        .foreach { request ⇒
          request.subscriber !+ timedOut
          removeFromOutstanding( aggregator )
        }

      stopIfFullyComplete( isWaitingToComplete )
    }

    case ActorSubscriberMessage.OnComplete ⇒ {
      log.info( Map( "@msg" → "notified that stream has completed, but waiting for outstanding work to complete" ), id = requestId )
      context become LoggingReceive { around( detection( isWaitingToComplete = true ) ) }
    }
  }

  def dispatch[_: EC]( m: OutlierDetectionMessage, p: AnalysisPlan ): ErrorOr[ActorRef] = {
    log.debug( Map( "@msg" → "dispatching", "topic" → m.topic.toString, "plan" → m.plan.name ), id = requestId )

    for {
      aggregator ← Either catchNonFatal {
        context.actorOf( OutlierQuorumAggregator.props( p, m.source ), aggregatorNameForMessage( m, p ) )
      }
    } yield {
      val history = updateHistory( m.source, p )
      p.algorithms foreach {
        case ( algo, algoConfig ) ⇒
          router.sendEnvelope( DetectUsing( m.plan.id, algo, m, history, algoConfig ) )( aggregator )
      }
      aggregator
    }
  }

  //todo store/hydrate
  var _history: Map[AnalysisPlan.Scope, HistoricalStatistics] = Map.empty[AnalysisPlan.Scope, HistoricalStatistics]
  def updateHistory( data: TimeSeriesBase, plan: AnalysisPlan ): HistoricalStatistics = {
    val key = AnalysisPlan.Scope( plan, data.topic )

    val initialHistory = _history get key getOrElse { HistoricalStatistics( 2, false ) }
    val sentHistory = data.points.foldLeft( initialHistory ) { ( h, dp ) ⇒ h :+ dp }
    val updatedHistory = sentHistory recordLastPoints data.points
    _history += key → updatedHistory

    log.debug( Map( "@msg" → "HISTORY", "topic" → data.topic.toString, "sent" → sentHistory.toString ), id = requestId )
    sentHistory
  }

  var _score: Map[( AnalysisPlan, Topic ), ( Long, Long )] = Map.empty[( AnalysisPlan, Topic ), ( Long, Long )]

  def updateScore( os: Outliers ): Unit = {
    val key = ( os.plan, os.topic )
    val s = _score.get( key ) map {
      case ( o, t ) ⇒
        ( o + os.anomalySize.toLong, t + os.size.toLong )
    } getOrElse {
      ( os.anomalySize.toLong, os.size.toLong )
    }
    _score += key → s
    log.alternative(
      category = "outlier",
      m = Map(
        "plan" → os.plan.name,
        "topic" → os.topic.toString,
        "rate" → ( s._1.toDouble / s._2.toDouble ),
        "count" → s._1,
        "points" → s._2
      ),
      id = requestId
    )
  }

  private def stopIfFullyComplete( isWaitingToComplete: Boolean ): Unit = {
    if ( isWaitingToComplete && outstanding.isEmpty ) {
      log.info( "finished outstanding work. stopping for stream completion", id = requestId )
      context stop self
    }
  }

  val fullExtractTopic: AnalysisPlan.ExtractTopic = OutlierDetection.extractOutlierDetectionTopic orElse { case _ ⇒ None }

  def aggregatorNameForMessage( m: OutlierDetectionMessage, p: AnalysisPlan ): String = {
    val extractedTopic = fullExtractTopic( m ) getOrElse {
      log.warn( Map( "@msg" → "failed to extract ID from message", "message" → m, "plan" → p.name ), id = requestId )
      "__UNKNOWN_ID__"
    }

    val uuid = ShortUUID()
    val name = s"""quorum:${p.name}@${extractedTopic.toString}:${uuid.toString}"""

    if ( ActorPath isValidPathElement name ) name
    else {
      val blunted = new URI( "akka.tcp", name, null ).getSchemeSpecificPart.replaceAll( """[\.\[\];/?:@&=+$,]""", "_" )
      log.debug(
        msg = Map(
          "@msg" → "attempting to dispatch to invalidly named aggregator",
          "plan" → p.name,
          "topic" → extractedTopic.toString,
          "uuid" → uuid.toString,
          "attempted aggregator name" → name,
          "blunted name" → blunted.toString
        ),
        id = requestId
      )
      blunted
    }
  }
}
