package spotlight.analysis.outlier

import scala.annotation.tailrec
import scala.concurrent.duration._
import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.event.LoggingReceive
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage, ActorSubscriberMessage}
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.metrics.InstrumentedActor
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.model.outlier.Outliers


/**
  * Created by rolfsd on 3/30/16.
  */
object DetectionPublisher {
  def props: Props = Props( new DetectionPublisher with StreamConfigurationProvider )


  trait StreamConfigurationProvider {
    def conductorMaxNrOfRetries: Int = 5
    def conductorRetriesWithinTimeRange: Duration = 2.minutes
  }
}

class DetectionPublisher
extends Actor
with ActorPublisher[Outliers]
with InstrumentedActor
with ActorLogging { outer: DetectionPublisher.StreamConfigurationProvider =>

  var buffer: Vector[Outliers] = Vector.empty[Outliers]

  override lazy val metricBaseName: MetricName = MetricName( classOf[DetectionPublisher] )
  val conductorFailuresMeter: Meter = metrics.meter( "conductor.failures" )

  override def receive: Receive = LoggingReceive { around( publish ) }


  override val subscriptionTimeout: Duration = {
    log.debug( "SUBSCRIPTION_TIMEOUT called" )
    5.seconds
  }

  val publish: Receive = {
    case DetectionResult( outliers ) => {
      if ( buffer.isEmpty && totalDemand > 0 ) {
        log.debug(
          "DetectionPublisher: there is demand [{}:{}] so short-circuiting buffer[{}] to onNext outliers: [{}]",
          totalDemand,
          isActive,
          buffer.size,
          outliers
        )
        onNext( outliers )
      } else {
        buffer :+= outliers
        log.debug( "DetectionPublisher: buffered [new-size={}] result: [{}]", buffer.size, outliers )
        deliverBuffer()
      }
    }

    case ActorSubscriberMessage.OnComplete => {
      deliverBuffer()
      onComplete()
    }

    case _: ActorPublisherMessage.Request => {
      log.debug( "DetectionPublisher: downstream request received: totalDemand=[{}]", totalDemand )
      deliverBuffer()
    }

    case ActorPublisherMessage.Cancel => {
      log.info( "cancelling detection analysis - leaving buffered:[{}]", buffer.size )
      context stop self
    }
  }


  @tailrec final def deliverBuffer(): Unit = {
    if ( isActive && totalDemand > 0 ) {
      if ( totalDemand <= Int.MaxValue ) {
        log.debug( "DetectionPublisher: delivering {} of {} demand", totalDemand.toInt, totalDemand.toInt )
        val (use, keep) = buffer splitAt totalDemand.toInt
        use foreach onNext
        buffer = keep
      } else {
        log.debug( "DetectionPublisher: delivering {} of {} demand", Int.MaxValue, totalDemand.toInt )
        val (use, keep) = buffer splitAt Int.MaxValue
        use foreach onNext
        buffer = keep
        deliverBuffer()
      }
    }
  }

  override final val supervisorStrategy: SupervisorStrategy = {
    OneForOneStrategy(
      maxNrOfRetries = outer.conductorMaxNrOfRetries,
      withinTimeRange = outer.conductorRetriesWithinTimeRange
    ) {
      case _: ActorInitializationException => Stop

      case _: ActorKilledException => Stop

      case _: Exception => {
        conductorFailuresMeter.mark( )
        Restart
      }

      case _ => Escalate
    }
  }
}
