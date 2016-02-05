package lineup.analysis.outlier

import lineup.analysis.outlier.OutlierDetection.DetectionResult

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.actor.{ Actor, ActorRef, ActorLogging, Props }
import akka.event.LoggingReceive
import akka.pattern.AskTimeoutException
import akka.stream.{ ActorAttributes, Supervision }
import akka.stream.scaladsl.Flow
import scala.util.Success
import scalaz.{ \/-, -\/ }
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{ Logger, StrictLogging }
import nl.grons.metrics.scala.Meter
import peds.akka.metrics.{ Instrumented, InstrumentedActor }
import peds.commons.identifier.ShortUUID
import peds.commons.V
import lineup.model.timeseries.{ Topic, TimeSeriesBase, TimeSeries, TimeSeriesCohort }
import lineup.model.outlier.{ Outliers, OutlierPlan }


object OutlierDetection extends StrictLogging with Instrumented {
  def props( makeDetector: => OutlierDetection ): Props = Props{ makeDetector }

  lazy val timeoutMeter: Meter = metrics meter "timeouts"

  def detectOutlier(
    detector: ActorRef,
    maxAllowedWait: FiniteDuration,
    parallelism: Int
  )(
    implicit ec: ExecutionContext
  ): Flow[TimeSeriesBase, Outliers, Unit] = {
    import akka.pattern.ask
    import akka.util.Timeout

    val decider: Supervision.Decider = {
      case ex: AskTimeoutException => {
        logger error s"Detection stage timed out on [${ex.getMessage}]"
        timeoutMeter.mark()
        Supervision.Resume
      }

      case _ => Supervision.Stop
    }

  //todo: refactor this to a FanOutShape with recognized and unrecognized outs.
    Flow[TimeSeriesBase]
    .mapAsyncUnordered( parallelism ) { ts: TimeSeriesBase =>
      implicit val triggerTimeout = Timeout( maxAllowedWait )
      val result = detector ? OutlierDetectionMessage( ts )
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
  case object ReloadPlans extends DetectionProtocol
  case class DetectionResult( outliers: Outliers ) extends DetectionProtocol
  case class UnrecognizedTopic( topic: Topic ) extends DetectionProtocol


  val extractOutlierDetectionTopic: OutlierPlan.ExtractTopic = {
    case m: OutlierDetectionMessage => Some(m.topic)
    case ts: TimeSeriesBase => Some(ts.topic)
    case t: Topic => Some(t)
  }

  trait ConfigurationProvider {
    def router: ActorRef
    def planFor( m: OutlierDetectionMessage ): Option[OutlierPlan]
    def refreshInterval: FiniteDuration
    def definedAt( m: OutlierDetectionMessage ): Boolean = planFor( m ).isDefined
    def invalidateCaches(): Unit
  }


  object PlanConfigurationProvider {
    type Creator = () => V[Seq[OutlierPlan]]
  }

  trait PlanConfigurationProvider extends OutlierDetection.ConfigurationProvider {
    def makePlans: PlanConfigurationProvider.Creator

    def makePlansWithFallback( default: Seq[OutlierPlan] ): Seq[OutlierPlan] = {
      makePlans() match {
        case \/-(ps) => ps
        case -\/(exs) => {
          exs foreach { ex => logger.warn( "Detector failed to load policy detection plans: {}", ex ) }
          default
        }
      }
    }

    var plans: Seq[OutlierPlan] = makePlansWithFallback( Seq.empty[OutlierPlan] )
    override def planFor( m: OutlierDetectionMessage ): Option[OutlierPlan] = plans find { _ appliesTo m }

    override def invalidateCaches(): Unit = {
      plans = makePlansWithFallback( plans )
      logger.debug(
        """Detector detection plans: [{}]""",
        plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")
      )
    }
  }
}


class OutlierDetection extends Actor with InstrumentedActor with ActorLogging {
  outer: OutlierDetection.PlanConfigurationProvider =>

  val scheduleEC = context.system.dispatchers.lookup( "schedule-dispatcher" )

  val reloader = context.system.scheduler.schedule(
    outer.refreshInterval,
    outer.refreshInterval,
    self,
    OutlierDetection.ReloadPlans
  )( scheduleEC )

  override def preStart(): Unit = {
    initializeMetrics()
    log.info( "{} dispatcher: [{}]", self.path, context.dispatcher )
  }

  override def postStop(): Unit = reloader.cancel()

  def initializeMetrics(): Unit = {
    metrics.gauge( "outstanding" ) { outstanding.size }
  }

  type AggregatorSubscribers = Map[ActorRef, ActorRef]
  var outstanding: AggregatorSubscribers = Map.empty[ActorRef, ActorRef]

  override def receive: Receive = around( detection() )

  def detection( inRetry: Boolean = false ): Receive = LoggingReceive {
    case result: Outliers if outstanding.contains( sender() ) => {
      val aggregator = sender()
      outstanding( aggregator ) ! DetectionResult( result )
      outstanding -= aggregator
      updateScore( result )
      context become around( detection(inRetry) )
    }

    case m: OutlierDetectionMessage if outer.definedAt( m ) => {
      log.debug( "plan for topic [{}]: [{}]", m.topic, outer.planFor(m) )
      val requester = sender()
      val aggregator = dispatch( m, outer.planFor(m).get )( context.dispatcher )
      outstanding += ( aggregator -> requester )
      context become around( detection(inRetry) )
    }

    case m: OutlierDetectionMessage if inRetry == false && outer.definedAt( m ) == false => {
      log.debug( "unrecognized topic [{}] retrying after reload", m.topic )
      // try reloading invalidating caches and retry on first miss only
      outer.invalidateCaches()
      self forward m
      context become around( detection(inRetry = true) )
    }

    case m: OutlierDetectionMessage => {
      log.debug( "unrecognized topic:[{}] from sender:[{}]", m.topic, sender() )
      sender() ! OutlierDetection.UnrecognizedTopic( m.topic )
    }

    case to: OutlierQuorumAggregator.AnalysisTimedOut => {
      log.error( "quorum was not reached in time: [{}]", to )
      outstanding -= sender()
      context become around( detection(inRetry) )
    }

    case OutlierDetection.ReloadPlans => {
      // invalidate cache and reset retry if triggered
      outer.invalidateCaches()
      context become around( detection(inRetry = false) )
    }
  }

  val fullExtractId: OutlierPlan.ExtractTopic = OutlierDetection.extractOutlierDetectionTopic orElse { case _ => None }

  def dispatch( m: OutlierDetectionMessage, p: OutlierPlan )( implicit ec: ExecutionContext ): ActorRef = {
    val aggregatorName = s"quorum-${p.name}-${fullExtractId(m) getOrElse "!NULL-ID!"}-${ShortUUID()}"
    val aggregator = context.actorOf( OutlierQuorumAggregator.props( p, m.source ), aggregatorName )
    val history = updateHistory( m.source )

    p.algorithms foreach { a =>
      val detect = DetectUsing(
        algorithm = a,
        aggregator = aggregator,
        payload = m,
        plan = p,
        history = Some(history),
        properties = p.algorithmConfig
      )

      router ! detect
    }

    aggregator
  }


  //todo store/hydrate
  var _history: Map[Topic, HistoricalStatistics] = Map.empty[Topic, HistoricalStatistics]
  def updateHistory( data: TimeSeriesBase ): HistoricalStatistics = {
    val result = _history get data.topic getOrElse { HistoricalStatistics( 2, false ) }

    for {
      dp <- data match {
        case s: TimeSeries => s.points
        case c: TimeSeriesCohort => c.data flatMap { _.points }
      }
    } {
      result add dp.getPoint
      _history += data.topic -> result
    }

    log.debug( "HISTORY for [{}]: [{}]", data.topic, result )
    result
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
