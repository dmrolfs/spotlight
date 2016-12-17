package spotlight.analysis.outlier

import scala.concurrent.duration.{Duration, FiniteDuration}
import akka.actor.{Actor, ActorLogging, Cancellable, Props}
import akka.event.LoggingReceive

import scalaz.{-\/, \/-}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor
import peds.commons.log.Trace
import spotlight.analysis.outlier.OutlierQuorumAggregator.ConfigurationProvider
import spotlight.model.outlier._
import spotlight.model.timeseries.{TimeSeriesBase, Topic}


object OutlierQuorumAggregator {
  val trace = Trace[OutlierQuorumAggregator.type]

  def props( plan: OutlierPlan, source: TimeSeriesBase ): Props = Props( new DefaultOutlierQuorumAggregator(plan, source) )

  private class DefaultOutlierQuorumAggregator( plan: OutlierPlan, source: TimeSeriesBase )
  extends OutlierQuorumAggregator( plan, source )
  with ConfigurationProvider {
    override val warningsBeforeTimeout: Int = 3
  }

  case class AnalysisTimedOut( topic: Topic, plan: OutlierPlan )


  trait ConfigurationProvider {
    def warningsBeforeTimeout: Int
    def attemptBudget( planBudget: Duration ): Duration = {
      planBudget match {
        case b if b.isFinite() && warningsBeforeTimeout == 0 => b
        case b if b.isFinite() => b / warningsBeforeTimeout
        case b => b
      }
    }
  }
}

/**
 * Created by rolfsd on 9/28/15.
 */
class OutlierQuorumAggregator( plan: OutlierPlan, source: TimeSeriesBase )
extends EnvelopingActor with InstrumentedActor with ActorLogging { outer: ConfigurationProvider =>
  import OutlierQuorumAggregator._

  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierQuorumAggregator] )
  lazy val conclusionsMeter: Meter = metrics meter "quorum.conclusions"
  lazy val warningsMeter: Meter = metrics meter "quorum.warnings"
  lazy val timeoutsMeter: Meter = metrics meter "quorum.timeout"

  lazy val quorumTimer: Timer = metrics.timer( "quorum.plan", plan.name )
  val originNanos: Long = System.nanoTime()

  implicit val ec = context.system.dispatcher

  var pendingWhistle: Option[Cancellable] = None
  scheduleWhistle( attemptBudget(plan.timeout) )

  def scheduleWhistle( duration: Duration ): Unit = {
    cancelWhistle()
    if ( duration.isFinite() ) {
      val budget = FiniteDuration( duration._1, duration._2 )
      pendingWhistle = Some( context.system.scheduler.scheduleOnce(budget, self, AnalysisTimedOut(source.topic, plan)) )
    }
  }

  def cancelWhistle(): Unit = {
    pendingWhistle foreach { _.cancel() }
    pendingWhistle = None
  }

  override def postStop(): Unit = cancelWhistle()

  var _fulfilled: OutlierAlgorithmResults = Map()

  override def receive: Receive = LoggingReceive{ around( quorum() ) }

  def quorum( retries: Int = warningsBeforeTimeout ): Receive = {
    case m: Outliers => {
      val source = sender()
      _fulfilled ++= m.algorithms map { _ -> m }
      log.debug( "Quorum received [{}] from [{}] fulfilled:[{}] of total:[{}]", m.getClass.getSimpleName, source, _fulfilled.size, plan.algorithms.size )
      if ( _fulfilled.size == plan.algorithms.size ) publishAndStop( _fulfilled )
    }

    case unknown: UnrecognizedPayload => {
      warningsMeter.mark()
      log.warning( "plan[{}] aggregator is dropping unrecognized response [{}]", plan.name, unknown )
    }

    //todo: this whole retry approach ROI doesn't pencil; should simply increase timeout
    case _: AnalysisTimedOut if retries > 0 => {
      val retriesLeft = retries - 1

      warningsMeter.mark()

      if ( !plan.isQuorum(_fulfilled) ) {
        log.debug(
          "may not reach quorum for topic:[{}] tries-left:[{}] received:[{}] of planned:[{}]",
          source.topic,
          retriesLeft,
          _fulfilled.keys.mkString(","),
          plan.summary
        )
      }

      scheduleWhistle( attemptBudget(plan.timeout) )
      context become LoggingReceive { around( quorum(retriesLeft) ) }
    }

    case timeout: AnalysisTimedOut => {
      timeoutsMeter.mark()
      if ( plan isQuorum _fulfilled ) {
        publishAndStop( _fulfilled )
      } else {
        log.info(
          "Analysis timed out and quorum was not reached for plan-topic:[{}] interval:[{}] received:[{}] of planned:[{}]",
          plan.name + ":" + source.topic,
          source.interval,
          _fulfilled.keys.mkString(","),
          plan.summary
        )

        context.parent ! timeout
        context stop self
      }
    }
  }


  def publishAndStop( fulfilled: OutlierAlgorithmResults ): Unit = {
    quorumTimer.update( System.nanoTime() - originNanos, scala.concurrent.duration.NANOSECONDS )
    conclusionsMeter.mark()

    plan.reduce( fulfilled, source, plan ) match {
      case \/-( o ) => {
        context.parent !+ o
        logTally( o, fulfilled )
      }

      case -\/( exs ) => exs.foreach{ ex => log.error( "failed to create Outliers for plan [{}] due to: {}", plan, ex ) }
    }

    context stop self
  }

  val outlierLogger: Logger = Logger( LoggerFactory getLogger "Outliers" )

  def logTally( result: Outliers, fulfilled: OutlierAlgorithmResults ): Unit = {
    val tally = fulfilled map { case (a, o) => (a, o.anomalySize) }
    outlierLogger.debug(
      "\t\talgorithm-tally[{}]:[{}] = final:[{}] algorithms:[{}]",
      result.plan.name,
      result.topic,
      result.anomalySize.toString,
      tally.mkString( "[", ",", "]" )
    )
  }
}
