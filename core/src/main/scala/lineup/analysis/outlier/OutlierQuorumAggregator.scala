package lineup.analysis.outlier

import scala.concurrent.duration.FiniteDuration
import akka.actor.{ Cancellable, Actor, ActorLogging, Props }
import akka.event.LoggingReceive
import peds.akka.metrics.InstrumentedActor
import peds.commons.log.Trace
import lineup.model.outlier._
import lineup.model.timeseries.{ Topic, TimeSeriesBase }


object OutlierQuorumAggregator {
  val trace = Trace[OutlierQuorumAggregator.type]

  def props( plan: OutlierPlan, source: TimeSeriesBase ): Props = Props( new OutlierQuorumAggregator(plan, source) )

  case class AnalysisTimedOut( topic: Topic, plan: OutlierPlan )
}

/**
 * Created by rolfsd on 9/28/15.
 */
class OutlierQuorumAggregator( plan: OutlierPlan, source: TimeSeriesBase ) extends Actor with InstrumentedActor with ActorLogging {
  import OutlierQuorumAggregator._

  val attemptTimeout: FiniteDuration = plan.timeout / 3L

  implicit val ec = context.system.dispatcher

  var pendingWhistle: Option[Cancellable] = None
  scheduleWhistle()

  def scheduleWhistle( duration: FiniteDuration = attemptTimeout ): Unit = {
    cancelWhistle()
    pendingWhistle = Some( context.system.scheduler.scheduleOnce(duration, self, AnalysisTimedOut(source.topic, plan)) )
  }

  def cancelWhistle(): Unit = {
    pendingWhistle foreach { _.cancel() }
    pendingWhistle = None
  }

  override def postStop(): Unit = cancelWhistle()

  type AnalysisFulfillment = Map[Symbol, Outliers]
  var fulfilled: AnalysisFulfillment = Map()

  override def receive: Receive = around( quorum() )

  def quorum( retries: Int = 3 ): Receive = LoggingReceive {
    case m: Outliers => {
      val source = sender()
      fulfilled ++= m.algorithms map { _ -> m }
      process( m )
    }

    //todo: this whole retry approach is wrong; should simply increase timeout
    case _: AnalysisTimedOut if retries > 0 => {
      val retriesLeft = retries - 1

      log warning s"quorum not reached for topic:[${source.topic}] tries-left:[$retriesLeft] " +
                  s"""received:[${fulfilled.keys.mkString(",")}] plan:[${plan.summary}]"""

      scheduleWhistle()
      context become around( quorum(retriesLeft) )
    }

    case timeout: AnalysisTimedOut => {
      context.parent ! timeout
      context.stop( self )
    }
  }

  def process( m: Outliers ): Unit = {
    if ( plan isQuorum fulfilled ) {
      import akka.pattern.pipe
      plan.reduce( fulfilled, source ) pipeTo context.parent
      context stop self
    }
  }
}
