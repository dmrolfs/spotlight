package spotlight.analysis.outlier

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.event.LoggingReceive
import akka.pattern.AskTimeoutException
import akka.stream.actor._
import akka.stream.{FlowShape, Materializer, Supervision}
import akka.stream.scaladsl._
import com.codahale.metrics.{Metric, MetricFilter}

import scalaz.\/-
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{Logger, StrictLogging}
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.akka.stream.StreamMonitor
import peds.commons.identifier.ShortUUID
import peds.commons.log.Trace
import spotlight.model.timeseries._
import spotlight.model.outlier.{OutlierPlan, Outliers}


object OutlierDetection extends StrictLogging with Instrumented {
  def props( routerRef: ActorRef ): Props = {
    Props(
      new OutlierDetection with ConfigurationProvider {
        override def router: ActorRef = routerRef
      }
    )
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

  def detectionFlow(
    detector: ActorRef,
    maxInDetectionFactor: Double,
    maxAllowedWait: FiniteDuration,
    plans: Seq[OutlierPlan]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[TimeSeriesBase, Outliers, Unit] = {
    Flow[TimeSeriesBase]
    .map { ts: TimeSeriesBase =>
      plans.to[immutable.Seq]
      .collect { case p if p appliesTo ts => OutlierDetectionMessage( ts, plan = p ).disjunction }
      .collect { case \/-( m ) => m }
    }
    .mapConcat[OutlierDetectionMessage] { identity }
    .via {
      ProcessorAdapter.processorFlow[OutlierDetectionMessage, DetectionResult]( maxInDetectionFactor ) {
        case m => detector
      }
    }
    .map { _.outliers }
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

//  lazy val maxInFlight: Int = {
//    val availableProcessors = Runtime.getRuntime.availableProcessors()
//    val result = math.floor( outer.maxInFlightCpuFactor * availableProcessors ).toInt
//    log.info(
//      "OutlierDetector: setting request strategy max in flight=[{}] based on CPU[{}] and configured factor[{}]",
//      result.toString,
//      availableProcessors,
//      outer.maxInFlightCpuFactor
//    )
//    result
//  }
//
//  override protected def requestStrategy: RequestStrategy = {
//    new MaxInFlightRequestStrategy( max = outer.maxInFlight ) {
//      override def inFlightInternally: Int = outstanding.size
//    }
//  }

  override def receive: Receive = LoggingReceive { around( detection ) }

//  val subscriber: Receive = {
//    case next @ ActorSubscriberMessage.OnNext( m: OutlierDetectionMessage ) if outer.streamConductor.isDefined => {
//      log.debug( "OutlierDetection request: [{}:{}]", m.topic, m.plan )
//      val aggregator = dispatch( m, m.plan )( context.dispatcher )
//      outstanding += aggregator -> DetectionSubscriber( ref = outer.streamConductor.get )
//    }
//
//    case ActorSubscriberMessage.OnComplete if outer.streamConductor.isDefined => {
//      outer.streamConductor.get ! ActorSubscriberMessage.OnComplete
//    }
////
////    case m: OnError => ???
//  }

  val detection: Receive = {
    case result: Outliers if outstanding.contains( sender() ) => {
      log.debug( "OutlierDetection results: [{}:{}]", result.topic, result.plan )
      val aggregator = sender()
      val subscriber = outstanding( aggregator )
      subscriber.ref ! DetectionResult( result )
      outstanding -= aggregator
      detectionTimer.update( System.nanoTime() - subscriber.startNanos, scala.concurrent.duration.NANOSECONDS )
      updateScore( result )
    }

    case m: OutlierDetectionMessage =>  {
      log.debug( "OutlierDetection request: [{}:{}]", m.topic, m.plan )
      val requester = sender()
      val aggregator = dispatch( m, m.plan )( context.dispatcher )
      outstanding += aggregator -> DetectionSubscriber( ref = requester )
    }

    case to @ OutlierQuorumAggregator.AnalysisTimedOut( topic, plan ) => {
      log.error( "quorum was not reached in time: [{}]", to )
      outstanding -= sender()
    }
  }

  val fullExtractId: OutlierPlan.ExtractTopic = OutlierDetection.extractOutlierDetectionTopic orElse { case _ => None }

  def dispatch( m: OutlierDetectionMessage, p: OutlierPlan )( implicit ec: ExecutionContext ): ActorRef = {
    log.debug( "OutlierDetection disptaching: [{}][{}:{}]", m.topic, m.plan, m.plan.id )
    val aggregatorName = s"quorum-${p.name}-${fullExtractId(m) getOrElse "!NULL-ID!"}-${ShortUUID()}"
    val aggregator = context.actorOf( OutlierQuorumAggregator.props( p, m.source ), aggregatorName )
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
    val sentHistory = data.points.foldLeft( initialHistory ) { (h, dp) => h :+ dp.getPoint }
    val updatedHistory = sentHistory recordLastPoints data.points.map{_.getPoint }
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
      "\t\toutlier-rate[{}]:[{}] = [{}%] in [{}] points",
      os.plan.name,
      os.topic,
      (s._1.toDouble / s._2.toDouble).toString,
      s._2.toString
    )
  }
}
