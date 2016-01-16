package lineup.analysis.outlier

import scala.concurrent.duration.FiniteDuration
import akka.actor.{ Cancellable, Actor, ActorLogging, Props }
import akka.event.LoggingReceive
import peds.akka.metrics.InstrumentedActor
import peds.commons.log.Trace
import nl.grons.metrics.scala.{MetricName, Meter}
import lineup.analysis.outlier.OutlierQuorumAggregator.ConfigurationProvider
import lineup.model.outlier._
import lineup.model.timeseries.{ Topic, TimeSeriesBase }


object OutlierQuorumAggregator {
  val trace = Trace[OutlierQuorumAggregator.type]

  def props( plan: OutlierPlan, source: TimeSeriesBase ): Props = {
    Props(
      new OutlierQuorumAggregator(plan, source) with ConfigurationProvider {
        override val warningsBeforeTimeout: Int = 3
      }
    )
  }

  case class AnalysisTimedOut( topic: Topic, plan: OutlierPlan )


  trait ConfigurationProvider {
    def warningsBeforeTimeout: Int
    def attemptBudget( planBudget: FiniteDuration ): FiniteDuration = {
      if ( warningsBeforeTimeout == 0 ) planBudget
      else planBudget / warningsBeforeTimeout.toLong
    }
  }
}

/**
 * Created by rolfsd on 9/28/15.
 */
class OutlierQuorumAggregator( plan: OutlierPlan, source: TimeSeriesBase )
extends Actor with InstrumentedActor with ActorLogging { outer: ConfigurationProvider =>
  import OutlierQuorumAggregator._

  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierQuorumAggregator] )
  lazy val conclusionsMeter: Meter = metrics meter "quorum.conclusions"
  lazy val warningsMeter: Meter = metrics meter "quorum.warnings"
  lazy val timeoutsMeter: Meter = metrics meter "quorum.timeout"

  implicit val ec = context.system.dispatcher

  var pendingWhistle: Option[Cancellable] = None
  scheduleWhistle()

  def scheduleWhistle( duration: FiniteDuration = attemptBudget(plan.timeout) ): Unit = {
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

  def quorum( retries: Int = warningsBeforeTimeout ): Receive = LoggingReceive {
    case m: Outliers => {
      val source = sender()
      fulfilled ++= m.algorithms map { _ -> m }
      process( m )
    }

    //todo: this whole retry approach is wrong; should simply increase timeout
    case _: AnalysisTimedOut if retries > 0 => {
      val retriesLeft = retries - 1

      warningsMeter.mark()
      log warning s"quorum not reached for topic:[${source.topic}] tries-left:[$retriesLeft] " +
                  s"""received:[${fulfilled.keys.mkString(",")}] plan:[${plan.summary}]"""

      scheduleWhistle()
      context become around( quorum(retriesLeft) )
    }

    case timeout: AnalysisTimedOut => {
      timeoutsMeter.mark()
      context.parent ! timeout
      context.stop( self )
    }
  }

  def process( m: Outliers ): Unit = {
    if ( plan isQuorum fulfilled ) {
      conclusionsMeter.mark()
      import akka.pattern.pipe
      plan.reduce( fulfilled, source ) pipeTo context.parent
      context stop self
    }
  }
}
