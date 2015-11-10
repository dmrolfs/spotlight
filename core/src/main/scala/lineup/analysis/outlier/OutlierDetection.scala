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

  def props( router: ActorRef, plans: Seq[OutlierPlan] ): Props = Props( new OutlierDetection( router, plans) )

  val extractOutlierDetectionTopic: OutlierPlan.ExtractTopic = {
    case m: OutlierDetectionMessage => Some(m.topic)
    case ts: TimeSeriesBase => Some(ts.topic)
    case t: Topic => Some(t)
  }
}


class OutlierDetection( router: ActorRef, plans: Seq[OutlierPlan] ) extends EnvelopingActor with ActorLogging {
  override val trace = Trace[OutlierDetection]

  type AggregatorSubscribers = Map[ActorRef, ActorRef]
  override def receive: Receive = around( detection( Map.empty[ActorRef, ActorRef] ) )

  def detection( outstanding: AggregatorSubscribers ): Receive = LoggingReceive {
    case m: OutlierDetectionMessage if planDefinedAt( m ) => {
      val requester = sender()
      val aggregator = dispatch( m, findPlanFor(m).get )
      val newOutstanding = outstanding + (aggregator -> requester)
      trace( s"new-outstanding[${newOutstanding.contains(aggregator)}] = $newOutstanding" )
      context become around( detection(newOutstanding) )
    }

    case result: Outliers if outstanding.contains( sender() ) => {
      val aggregator = sender()
      outstanding( aggregator ) ! result
      context become around( detection(outstanding - aggregator) )
    }

//    case result: Outliers => {
//      log error s"OUTLIER RESULT MISFIRE?\nresult:[$result]\noutstanding:[$outstanding]\naggregator:[${sender()}]"
//    }
  }


  override def unhandled(message: Any): Unit = trace.block( s"unhandled" ) {
    trace( s"sender = ${sender().path}" )
    trace( s"message = $message" )
    super.unhandled(message)
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

  def planDefinedAt( msg: OutlierDetectionMessage ): Boolean = trace.block( s"planDefinedAt($msg)" ) { findPlanFor( msg ).isDefined }

  def findPlanFor( msg: OutlierDetectionMessage ): Option[OutlierPlan] = trace.block( s"findPlanFor(${msg.topic})" ) { plans find { _ appliesTo msg } }

  val fullExtractId: OutlierPlan.ExtractTopic = OutlierDetection.extractOutlierDetectionTopic orElse { case _ => None }
}
