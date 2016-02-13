package lineup.analysis.outlier

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.actor.{ Actor, ActorRef, ActorLogging, Props }
import akka.event.LoggingReceive
import akka.pattern.AskTimeoutException
import akka.stream.{ ActorAttributes, Supervision }
import akka.stream.scaladsl.Flow
import scala.util.Success
import scalaz.{ Success => SuccessZ }
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{ Logger, StrictLogging }
import nl.grons.metrics.scala.Meter
import peds.akka.metrics.{ Instrumented, InstrumentedActor }
import peds.commons.identifier.ShortUUID
import peds.commons.log.Trace
import lineup.model.timeseries.{ Topic, TimeSeriesBase, TimeSeries, TimeSeriesCohort }
import lineup.model.outlier.{ Outliers, OutlierPlan }


object OutlierDetection extends StrictLogging with Instrumented {
  def props( makeDetector: => OutlierDetection ): Props = Props{ makeDetector }

  lazy val timeoutMeter: Meter = metrics meter "timeouts"

  val decider: Supervision.Decider = {
    case ex: AskTimeoutException => {
      logger error s"Detection stage timed out on [${ex.getMessage}]"
      timeoutMeter.mark()
      Supervision.Resume
    }

    case _ => Supervision.Stop
  }

  def detectOutlier(
    detector: ActorRef,
    maxAllowedWait: FiniteDuration,
    plans: immutable.Seq[OutlierPlan],
    parallelism: Int
  )(
    implicit ec: ExecutionContext
  ): Flow[TimeSeriesBase, Outliers, Unit] = {
    import akka.pattern.ask
    import akka.util.Timeout
    implicit val triggerTimeout = Timeout( maxAllowedWait )

  //todo: refactor this to a FanOutShape with recognized and unrecognized outs.
    Flow[TimeSeriesBase]
    .map { ts: TimeSeriesBase =>
      plans
      .collect { case p if p appliesTo ts => OutlierDetectionMessage( ts, p ) }
      .collect { case SuccessZ( m ) => m }
    }
    .mapConcat[OutlierDetectionMessage] { identity }
    .mapAsyncUnordered( parallelism ) { request: OutlierDetectionMessage =>
      val result = detector ? request
      result
      .mapTo[DetectionProtocol]
      .andThen {
        case Success( UnrecognizedTopic( topic ) ) => logger info s"detector did not recognize topic [${topic}]"
      }
    }.withAttributes( ActorAttributes.supervisionStrategy(decider) )
    .collect {
      case DetectionResult( result ) => result
    }
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
}


class OutlierDetection extends Actor with InstrumentedActor with ActorLogging {
  outer: OutlierDetection.ConfigurationProvider =>

  import OutlierDetection._

  val trace = Trace[OutlierDetection]

  override def preStart(): Unit = {
    initializeMetrics()
    log.info( "{} dispatcher: [{}]", self.path, context.dispatcher )
  }

  def initializeMetrics(): Unit = {
    metrics.gauge( "outstanding" ) { outstanding.size }
  }

  type AggregatorSubscribers = Map[ActorRef, ActorRef]
  var outstanding: AggregatorSubscribers = Map.empty[ActorRef, ActorRef]

  override def receive: Receive = around( detection )

  val detection: Receive = LoggingReceive {
    case result: Outliers if outstanding.contains( sender() ) => {
      val aggregator = sender()
      outstanding( aggregator ) ! DetectionResult( result )
      outstanding -= aggregator
      updateScore( result )
    }

    case m: OutlierDetectionMessage => trace.block( s"RECEIVE: [$m]" ) {
      val requester = sender()
      val aggregator = dispatch( m, m.plan )( context.dispatcher )
      outstanding += ( aggregator -> requester )
    }

    case to @ OutlierQuorumAggregator.AnalysisTimedOut( topic, plan ) => {
      log.error( "quorum was not reached in time: [{}]", to )
      outstanding( sender() ) ! DetectionTimedOut( topic, plan )
      outstanding -= sender()
    }
  }

  val fullExtractId: OutlierPlan.ExtractTopic = OutlierDetection.extractOutlierDetectionTopic orElse { case _ => None }

  def dispatch( m: OutlierDetectionMessage, p: OutlierPlan )( implicit ec: ExecutionContext ): ActorRef = trace.block( s"dispatch(${m.topic}, ${p.name}:${p.id})" ) {
    val aggregatorName = s"quorum-${p.name}-${fullExtractId(m) getOrElse "!NULL-ID!"}-${ShortUUID()}"
    val aggregator = context.actorOf( OutlierQuorumAggregator.props( p, m.source ), aggregatorName )
    val history = updateHistory( m.source, p )

    p.algorithms foreach { a =>
      val detect = {
        DetectUsing(
          algorithm = a,
          aggregator = aggregator,
          payload = m,
          history = Some(history),
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

    val initHistory = _history get key getOrElse { HistoricalStatistics( 2, false ) }
    val updatedHistory = data.points.foldLeft( initHistory ) { (h, dp) => h add dp.getPoint }
    _history += key -> updatedHistory

    log.debug( "HISTORY for [{}]: [{}]", data.topic, updatedHistory )
    updatedHistory
  }

  var _score: Map[Topic, (Long, Long)] = Map.empty[Topic, (Long, Long)]
  val outlierLogger: Logger = Logger( LoggerFactory getLogger "Outliers" )

  def updateScore( os: Outliers ): Unit = {
    val s = _score.get( os.topic ) map { case (o, t) =>
      ( o + os.anomalySize.toLong, t + os.size.toLong )
    } getOrElse {
      ( os.anomalySize.toLong, os.size.toLong )
    }
    _score += os.topic -> s
    outlierLogger.debug(
      "[{}]:[{}] outlier rate:[{}%] in [{}] points",
      os.plan.name,
      os.topic,
      (s._1.toDouble / s._2.toDouble).toString,
      s._2.toString
    )
  }
}
