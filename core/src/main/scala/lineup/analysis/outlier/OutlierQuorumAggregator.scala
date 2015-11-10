package lineup.analysis.outlier

import akka.actor.{ ActorLogging, Props }
import akka.event.LoggingReceive
import lineup.model.timeseries.TimeSeriesBase
import peds.akka.envelope._
import peds.commons.log.Trace
import lineup.model.outlier._


object OutlierQuorumAggregator {
  val trace = Trace[OutlierQuorumAggregator.type]

  def props( plan: OutlierPlan, source: TimeSeriesBase ): Props = Props( new OutlierQuorumAggregator(plan, source) )

  case object AnalysisTimedOut
}

/**
 * Created by rolfsd on 9/28/15.
 */
class OutlierQuorumAggregator( plan: OutlierPlan, source: TimeSeriesBase ) extends EnvelopingActor with ActorLogging {
  import OutlierQuorumAggregator._

  implicit val ec = context.system.dispatcher

  val pendingWhistle = context.system.scheduler.scheduleOnce( plan.timeout, self, AnalysisTimedOut )
  override def postStop(): Unit = pendingWhistle.cancel()

  override val trace = Trace[OutlierQuorumAggregator]

  type AnalysisFulfillment = Map[Symbol, Outliers]
  var fulfilled: AnalysisFulfillment = Map()

  override def receive: Receive = around( quorum )

  val quorum: Receive = LoggingReceive {
    case m: Outliers => {
      val source = sender()
      fulfilled ++= m.algorithms map { _ -> m }
      process( m )
    }

    case AnalysisTimedOut => {
//todo stream enveloping: context.parent !+ AnalysisTimedOut
      context.parent ! AnalysisTimedOut
      context.stop( self )
    }
  }

  def process( m: Outliers ): Unit = trace.block( s"process($m)" ) {
    if ( plan isQuorum fulfilled ) {
      import akka.pattern.pipe
      //todo stream enveloping: context.parent !+ result
      plan.reduce( fulfilled, source ) pipeTo context.parent
      context stop self
    }
  }
}
