package lineup.analysis.outlier

import scala.concurrent.duration.FiniteDuration
import akka.actor.{ ActorRef, ActorLogging, Props }
import akka.event.LoggingReceive
import akka.stream.scaladsl.Flow
import peds.akka.envelope._
import peds.commons.identifier.ShortUUID
import peds.commons.log.Trace
import lineup.model.timeseries.{Topic, TimeSeriesBase}
import lineup.model.outlier.{ Outliers, OutlierPlan }


object OutlierDetection {
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

  def props(
    router: ActorRef,
    plans: Map[Topic, OutlierPlan],
    default: Option[OutlierPlan] = None,
    extractId: ExtractId = topicExtractId
  ): Props = {
    Props( new OutlierDetection( router, plans, default, extractId ) )
  }

  type ExtractId = PartialFunction[Any, Topic]
  val topicExtractId: ExtractId = { case m: OutlierDetectionMessage => m.topic }
}

class OutlierDetection(
  router: ActorRef,
  plans: Map[Topic, OutlierPlan],
  default: Option[OutlierPlan] = None,
  extractId: OutlierDetection.ExtractId = OutlierDetection.topicExtractId
) extends EnvelopingActor with ActorLogging {
  override val trace = Trace[OutlierDetection]

  val fullExtractId: OutlierDetection.ExtractId = extractId orElse { case _ => "!null-id!" }

  type AggregatorRequesters = Map[ActorRef, ActorRef]
  override def receive: Receive = around( detection( Map.empty[ActorRef, ActorRef] ) )

  def detection( outstanding: AggregatorRequesters ): Receive = LoggingReceive {
    case m: OutlierDetectionMessage if default.isDefined && extractId.isDefinedAt(m) => {
      val requester = sender()
      val aggregator = dispatch( m, plans.getOrElse(extractId(m), default.get) )
      context become around(detection(outstanding + (aggregator -> requester)))
    }

    case m: OutlierDetectionMessage if default.isDefined => {
      val requester = sender()
      val aggregator = dispatch( m, default.get )
      context become around(detection(outstanding + (aggregator -> requester)))
    }

    case result: Outliers if outstanding.contains( sender() ) => outstanding( sender() ) ! result
  }

  def dispatch( m: OutlierDetectionMessage, p: OutlierPlan ): ActorRef = {
    val aggregatorName = s"quorum-${p.name}-${fullExtractId(m)}-${ShortUUID()}"
    val aggregator = context.actorOf( OutlierQuorumAggregator.props( p, m.source ), aggregatorName )

    p.algorithms foreach { a =>
      //todo stream enveloping: router !+ DetectUsing( algorithm = a, destination = aggregator, payload = m, p.algorithmProperties )
      router ! DetectUsing( algorithm = a, aggregator = aggregator, payload = m, p.algorithmProperties )
    }

    aggregator
  }
}
