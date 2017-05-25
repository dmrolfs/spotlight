import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import akka.actor.{ ActorContext, ActorRef, ActorSystem }
import akka.stream.Materializer
import akka.util.Timeout
import cats.Show
import org.joda.{ time ⇒ joda }
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import demesne.{ BoundedContext, DomainModel }
import org.joda.time.DateTime

/** Created by rolfsd on 1/17/17.
  */
package object spotlight {
  type EC[_] = ExecutionContext
  type BC[_] = BoundedContext
  type DM[_] = DomainModel
  type AS[_] = ActorSystem
  type AC[_] = ActorContext
  type T[_] = Timeout
  type M[_] = Materializer

  val MetricBasePath = "spotlight.metrics.basename"

  lazy val BaseMetricName: String = {
    val base = "spotlight"
    ConfigFactory.load().as[Option[String]]( MetricBasePath ) map { _ + "." + base } getOrElse { base }
  }

  object showImplicits {
    implicit val actorRefShow = new Show[ActorRef] {
      override def show( ref: ActorRef ): String = ref.toString
    }

    implicit val finiteDurationShow = new Show[FiniteDuration] {
      override def show( d: FiniteDuration ): String = d.toCoarsest.toString()
    }

    implicit val jodaDateTimeShow = new Show[joda.DateTime] {
      override def show( dt: DateTime ): String = dt.toString
    }
  }
}
