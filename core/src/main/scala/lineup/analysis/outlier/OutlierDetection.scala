package lineup.analysis.outlier

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }
import akka.actor.{ ActorRef, ActorLogging, Props }
import akka.event.LoggingReceive
import akka.stream.scaladsl.Flow
import peds.akka.envelope._
import peds.commons.identifier.ShortUUID
import peds.commons.log.Trace
import lineup.model.timeseries.{ Topic, TimeSeriesBase }
import lineup.model.outlier.{ Outliers, OutlierPlan }
import lineup.analysis.outlier.OutlierDetection.ReloadPlans



object OutlierDetection extends LazyLogging {
  def detectOutlier(
    detector: ActorRef,
    maxAllowedWait: FiniteDuration,
    parallelism: Int
  ): Flow[TimeSeriesBase, Outliers, Unit] = {
    import akka.pattern.ask
    import akka.util.Timeout

    Flow[TimeSeriesBase].mapAsync( parallelism ) { ts: TimeSeriesBase =>
      //todo: the actor introduces a bottleneck, so might want to assign a dedicated dispatcher
      implicit val triggerTimeout = Timeout( maxAllowedWait )
      val result = detector ? OutlierDetectionMessage( ts )
      result.mapTo[Outliers]
    }
  }

  def props( router: ActorRef )( makeDetector: ActorRef => OutlierDetection ): Props = Props( makeDetector(router) )

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

  object ReloadPlans


  trait ConfigPlanPolicy extends OutlierDetection.PlanPolicy {
    def getPlans: () => Try[Seq[OutlierPlan]]

    def getPlansWithFallback( default: Seq[OutlierPlan] ): Seq[OutlierPlan] = {
      getPlans() match {
        case Success(ps) => ps
        case Failure(ex) => {
          logger warn s"failed to load detection plans in policy: $ex"
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


class OutlierDetection( router: ActorRef ) extends EnvelopingActor with ActorLogging { outer: OutlierDetection.PlanPolicy =>

  override val trace = Trace[OutlierDetection]

  val scheduleEC = context.system.dispatchers.lookup( "schdedule-dispatcher" )

  val reloader = context.system.scheduler.schedule(
    outer.refreshInterval,
    outer.refreshInterval,
    self,
    OutlierDetection.ReloadPlans
  )( scheduleEC )

  override def postStop(): Unit = reloader.cancel()

  type AggregatorSubscribers = Map[ActorRef, ActorRef]
  override def receive: Receive = around( detection( Map.empty[ActorRef, ActorRef] ) )

  def detection(outstanding: AggregatorSubscribers, inRetry: Boolean = false ): Receive = LoggingReceive {
    case m: OutlierDetectionMessage if outer.definedAt( m ) => {
      log debug s"plan for topic [${m.topic}]: [${outer.planFor(m)}]"
      val requester = sender()
      val aggregator = dispatch( m, outer.planFor(m).get )
      val newOutstanding = outstanding + (aggregator -> requester)
//      log debug s"new-outstanding[${newOutstanding.contains(aggregator)}] = $newOutstanding"
      context become around( detection(newOutstanding, inRetry) )
    }

    case m: OutlierDetectionMessage if inRetry == false && outer.definedAt( m ) == false => {
      log info s"unrecognized topic [${m.topic}] retrying after reload"
      // try reloading invalidating caches and retry on first miss only
      outer.invalidateCaches()
      self ! m
      context become around( detection(outstanding, inRetry = true) )
    }

    case result: Outliers if outstanding.contains( sender() ) => {
      val aggregator = sender()
      outstanding( aggregator ) ! result
      context become around( detection(outstanding - aggregator, inRetry) )
    }

    case ReloadPlans => {
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
