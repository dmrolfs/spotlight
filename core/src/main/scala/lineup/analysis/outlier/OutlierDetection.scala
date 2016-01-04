package lineup.analysis.outlier

import akka.pattern.AskTimeoutException
import akka.stream.{ActorAttributes, Supervision}

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }
import akka.actor.{Actor, ActorRef, ActorLogging, Props}
import akka.event.LoggingReceive
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.StrictLogging
import peds.akka.metrics.InstrumentedActor
import peds.commons.identifier.ShortUUID
import lineup.model.timeseries.{ Topic, TimeSeriesBase }
import lineup.model.outlier.{ Outliers, OutlierPlan }


object OutlierDetection extends StrictLogging {
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



  def props( router: ActorRef )( makeDetector: ActorRef => OutlierDetection ): Props = Props( makeDetector(router) )

  sealed trait DetectionProtocol
  case object ReloadPlans extends DetectionProtocol

  //todo: refactor with FanOutShape with recognized and unrecognized outs.
  case class UnrecognizedTopic( override val topic: Topic ) extends DetectionProtocol with Outliers {
    override type Source = TimeSeriesBase
    override def hasAnomalies: Boolean = false
    override def source: Source = null
    override def algorithms: Set[Symbol] = Set.empty[Symbol]

  }


  val extractOutlierDetectionTopic: OutlierPlan.ExtractTopic = {
    case m: OutlierDetectionMessage => Some(m.topic)
    case ts: TimeSeriesBase => Some(ts.topic)
    case t: Topic => Some(t)
  }

  trait PlanPolicy {
    def planFor( m: OutlierDetectionMessage ): Option[OutlierPlan]
    def refreshInterval: FiniteDuration
    def definedAt( m: OutlierDetectionMessage ): Boolean = planFor( m ).isDefined
    def invalidateCaches(): Unit
  }


  trait ConfigPlanPolicy extends OutlierDetection.PlanPolicy {
    def getPlans: () => Try[Seq[OutlierPlan]]

    def getPlansWithFallback( default: Seq[OutlierPlan] ): Seq[OutlierPlan] = {
      getPlans() match {
        case Success(ps) => ps
        case Failure(ex) => {
          logger warn s"Detector failed to load policy detection plans: $ex"
          default
        }
      }
    }

    var plans: Seq[OutlierPlan] = getPlansWithFallback( Seq.empty[OutlierPlan] )
    override def planFor( m: OutlierDetectionMessage ): Option[OutlierPlan] = plans find { _ appliesTo m }

    override def invalidateCaches(): Unit = {
      plans = getPlansWithFallback( plans )
      logger debug s"""Detector detection plans: [${plans.zipWithIndex.map{ case (p,i) => f"${i}%2d: ${p}"}.mkString("\n","\n","\n")}]"""
    }
  }
}


class OutlierDetection( router: ActorRef ) extends Actor with InstrumentedActor with ActorLogging {
  outer: OutlierDetection.PlanPolicy =>

  val scheduleEC = context.system.dispatchers.lookup( "schedule-dispatcher" )

  val reloader = context.system.scheduler.schedule(
    outer.refreshInterval,
    outer.refreshInterval,
    self,
    OutlierDetection.ReloadPlans
  )( scheduleEC )

  override def postStop(): Unit = reloader.cancel()

  type AggregatorSubscribers = Map[ActorRef, ActorRef]
  override def receive: Receive = around( detection(Map.empty[ActorRef, ActorRef]) )

  def detection(outstanding: AggregatorSubscribers, inRetry: Boolean = false ): Receive = LoggingReceive {
    case result: Outliers if outstanding.contains( sender() ) => {
      val aggregator = sender()
      outstanding( aggregator ) ! result
      context become around( detection(outstanding - aggregator, inRetry) )
    }

    case m: OutlierDetectionMessage if outer.definedAt( m ) => {
      log debug s"plan for topic [${m.topic}]: [${outer.planFor(m)}]"
      val requester = sender()
      val aggregator = dispatch( m, outer.planFor(m).get )
      val newOutstanding = outstanding + (aggregator -> requester)
//      log debug s"new-outstanding[${newOutstanding.contains(aggregator)}] = $newOutstanding"
      context become around( detection(newOutstanding, inRetry) )
    }

    case m: OutlierDetectionMessage if inRetry == false && outer.definedAt( m ) == false => {
      log debug s"unrecognized topic [${m.topic}] retrying after reload"
      // try reloading invalidating caches and retry on first miss only
      outer.invalidateCaches()
      self forward m
      context become around( detection(outstanding, inRetry = true) )
    }

    case m: OutlierDetectionMessage => {
      log debug s"unrecognized topic:[${m.topic}] from sender:[${sender()}]"
      sender() ! OutlierDetection.UnrecognizedTopic( m.topic )
    }

    case to: OutlierQuorumAggregator.AnalysisTimedOut => {
      log error s"quorum was not reached in time: [$to]"
      val updated = outstanding - sender()
      context become around( detection(updated, inRetry) )
    }

    case OutlierDetection.ReloadPlans => {
      // invalidate cache and reset retry if triggered
      outer.invalidateCaches()
      context become around( detection(outstanding, inRetry = false) )
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
