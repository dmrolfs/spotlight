package lineup.sandbox

import akka.actor.Actor.Receive
import akka.actor.{ActorLogging, Actor, Props}
import akka.event.LoggingReceive
import lineup.model.timeseries.TimeSeriesBase.Merging
import lineup.model.timeseries.{TimeSeriesCohort, TimeSeries, TimeSeriesBase}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag


object Limiter {
  def props( releasePeriod: FiniteDuration ): Props = Props( new Limiter( releasePeriod ) )
  case object ReleaseSeries
}
/**
 * Created by rolfsd on 10/21/15.
 */
class Limiter( releasePeriod: FiniteDuration ) extends Actor with ActorLogging {
  import Limiter._
  import akka.actor.Status

  implicit val ec = context.system.dispatcher

  val releaseTimer = context.system.scheduler.schedule(
    initialDelay = releasePeriod,
    interval = releasePeriod,
    receiver = self,
    message = ReleaseSeries
  )

  override def receive: Receive = quiescent

  val quiescent: Receive = LoggingReceive {
    case t: TimeSeries => context become merging( t )
    case t: TimeSeriesCohort => context become merging( t )
    case ReleaseSeries => { }
  }

  def merging[T <: TimeSeriesBase : Merging: ClassTag]( acc: T ): Receive = LoggingReceive {
    case t: T => implicitly[Merging[T]].merge( acc, t ) foreach { m => context become merging( m ) }

    case ReleaseSeries => {
     releaseWaiting( acc )
      context become quiescent
    }
  }

  def releaseWaiting( b: TimeSeriesBase ): Unit = b match {
    case s: TimeSeries => {

    }

    case c: TimeSeriesCohort => ???
  }
}
