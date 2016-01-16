package lineup.analysis.outlier

import scala.concurrent.duration._
import akka.actor.{ Actor, ActorRef, ActorLogging, Props }
import akka.event.LoggingReceive
import akka.pattern.AskTimeoutException
import akka.stream.{ ActorAttributes, Supervision }
import akka.stream.scaladsl.Flow
import scalaz.{ \/-, -\/ }
import com.typesafe.scalalogging.StrictLogging
import nl.grons.metrics.scala.Meter
import peds.akka.metrics.{ Instrumented, InstrumentedActor }
import peds.commons.identifier.ShortUUID
import peds.commons.Valid
import lineup.model.timeseries.{ Topic, TimeSeriesBase }
import lineup.model.outlier.{ Outliers, OutlierPlan }


object OutlierDetection extends StrictLogging with Instrumented {
  def props( makeDetector: => OutlierDetection ): Props = Props( makeDetector )

  lazy val timeoutMeter: Meter = metrics meter "timeouts"

  def detectOutlier(
    detector: ActorRef,
    maxAllowedWait: FiniteDuration,
    parallelism: Int
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
      result.mapTo[Outliers]
    }.withAttributes( ActorAttributes.supervisionStrategy(decider) )
    .filter {
      case e: UnrecognizedTopic => false
      case e => true
    }
  }


  sealed trait DetectionProtocol
  case object ReloadPlans extends DetectionProtocol

  //todo: refactor with FanOutShape with recognized and unrecognized outs.
  case class UnrecognizedTopic( override val topic: Topic ) extends DetectionProtocol with Outliers {
    override type Source = TimeSeriesBase
    override def hasAnomalies: Boolean = false
    override def source: Source = null
    override def size: Int = 0
    override def anomalySize: Int = 0
    override def algorithms: Set[Symbol] = Set.empty[Symbol]
  }


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
    type Creator = () => Valid[Seq[OutlierPlan]]
  }

  trait PlanConfigurationProvider extends OutlierDetection.ConfigurationProvider {
    def makePlans: PlanConfigurationProvider.Creator

    def makePlansWithFallback( default: Seq[OutlierPlan] ): Seq[OutlierPlan] = {
      makePlans() match {
        case \/-(ps) => ps
        case -\/(exs) => {
          exs foreach { ex => logger warn s"Detector failed to load policy detection plans: $ex" }
          default
        }
      }
    }

    var plans: Seq[OutlierPlan] = makePlansWithFallback( Seq.empty[OutlierPlan] )
    override def planFor( m: OutlierDetectionMessage ): Option[OutlierPlan] = plans find { _ appliesTo m }

    override def invalidateCaches(): Unit = {
      plans = makePlansWithFallback( plans )
      logger debug s"""Detector detection plans: [${plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]"""
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
    log info s"${self.path} dispatcher: [${context.dispatcher}]"
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
      outstanding( aggregator ) ! result
      outstanding -= aggregator
      context become around( detection(inRetry) )
    }

    case m: OutlierDetectionMessage if outer.definedAt( m ) => {
      log debug s"plan for topic [${m.topic}]: [${outer.planFor(m)}]"
      val requester = sender()
      val aggregator = dispatch( m, outer.planFor(m).get )
      outstanding += ( aggregator -> requester )
      context become around( detection(inRetry) )
    }

    case m: OutlierDetectionMessage if inRetry == false && outer.definedAt( m ) == false => {
      log debug s"unrecognized topic [${m.topic}] retrying after reload"
      // try reloading invalidating caches and retry on first miss only
      outer.invalidateCaches()
      self forward m
      context become around( detection(inRetry = true) )
    }

    case m: OutlierDetectionMessage => {
      log debug s"unrecognized topic:[${m.topic}] from sender:[${sender()}]"
      sender() ! OutlierDetection.UnrecognizedTopic( m.topic )
    }

    case to: OutlierQuorumAggregator.AnalysisTimedOut => {
      log error s"quorum was not reached in time: [$to]"
      outstanding -= sender()
      context become around( detection(inRetry) )
    }

    case OutlierDetection.ReloadPlans => {
      // invalidate cache and reset retry if triggered
      outer.invalidateCaches()
      context become around( detection(inRetry = false) )
    }
  }


  def dispatch( m: OutlierDetectionMessage, p: OutlierPlan ): ActorRef = {
    val aggregatorName = s"quorum-${p.name}-${fullExtractId(m) getOrElse "!NULL-ID!"}-${ShortUUID()}"
    val aggregator = context.actorOf( OutlierQuorumAggregator.props( p, m.source ), aggregatorName )

    p.algorithms foreach { a =>
      //todo stream enveloping: router !+ DetectUsing( algorithm = a, destination = aggregator, payload = m, p.algorithmProperties )
      router ! DetectUsing( algorithm = a, aggregator = aggregator, payload = m, p.algorithmConfig )
    }

    aggregator
  }

  val fullExtractId: OutlierPlan.ExtractTopic = OutlierDetection.extractOutlierDetectionTopic orElse { case _ => None }
}
