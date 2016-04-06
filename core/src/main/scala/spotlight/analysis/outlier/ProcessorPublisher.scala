package spotlight.analysis.outlier

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.event.LoggingReceive
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage, ActorSubscriberMessage}
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.metrics.InstrumentedActor


/**
  * Created by rolfsd on 3/30/16.
  */
object ProcessorPublisher {
  def props[O: ClassTag]: Props = Props( new ProcessorPublisher[O] with StreamConfigurationProvider )


  trait StreamConfigurationProvider {
    def conductorMaxNrOfRetries: Int = 5
    def conductorRetriesWithinTimeRange: Duration = 2.minutes
  }
}

class ProcessorPublisher[O: ClassTag]
extends Actor
with ActorPublisher[O]
with InstrumentedActor
with ActorLogging { outer: ProcessorPublisher.StreamConfigurationProvider =>
  val oTag = implicitly[ClassTag[O]]
  log.debug( "Publisher oTag = {}", oTag )
  var buffer: Vector[O] = Vector.empty[O]


  override lazy val metricBaseName: MetricName = MetricName( classOf[ProcessorPublisher[O]] )
  val conductorFailuresMeter: Meter = metrics.meter( "conductor.failures" )

//  override def receive: Receive = LoggingReceive { around( publish ) }
  override def receive: Receive = LoggingReceive { publish }


  override val subscriptionTimeout: Duration = {
    log.debug( "SUBSCRIPTION_TIMEOUT called" )
    5.seconds
  }

  val publish: Receive = {
    case oTag( message ) if isActive => {
      log.debug( "Publisher received message: [{}][{}]", oTag.runtimeClass.getName, message )
      if ( buffer.isEmpty && totalDemand > 0 ) {
        log.debug(
          "ProcessorPublisher: there is demand [{}:{}] so short-circuiting buffer[{}] to onNext message: [{}]",
          totalDemand,
          isActive,
          buffer.size,
          message
        )
        onNext( message )
      } else {
        buffer :+= message
        log.debug( "ProcessorPublisher: buffered [new-size={}] result: [{}]", buffer.size, message )
        deliverBuffer()
      }
    }

    case ActorSubscriberMessage.OnComplete => {
      deliverBuffer()
      onComplete()
    }

    case ActorSubscriberMessage.OnError( cause ) => onError( cause )

    case _: ActorPublisherMessage.Request => {
      log.debug( "ProcessorPublisher: downstream request received: totalDemand=[{}]", totalDemand )
      deliverBuffer()
    }

    case ActorPublisherMessage.Cancel => {
      log.info( "cancelling detection analysis - leaving buffered:[{}]", buffer.size )
      context stop self
    }

//    case m => unhandled( m )
  }


  @tailrec final def deliverBuffer(): Unit = {
    if ( isActive && totalDemand > 0 ) {
      if ( totalDemand <= Int.MaxValue ) {
        log.debug( "ProcessorPublisher: delivering {} of {} demand", totalDemand.toInt, totalDemand.toInt )
        val (use, keep) = buffer splitAt totalDemand.toInt
        use foreach onNext
        buffer = keep
      } else {
        log.debug( "ProcessorPublisher: delivering {} of {} demand", Int.MaxValue, totalDemand.toInt )
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
