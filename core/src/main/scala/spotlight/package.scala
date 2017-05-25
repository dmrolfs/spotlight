import scala.concurrent.ExecutionContext
import akka.actor.{ ActorContext, ActorPath, ActorRef, ActorSystem }
import akka.stream.Materializer
import akka.util.Timeout
import cats.Show
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import demesne.{ BoundedContext, DomainModel }

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

  object Show {
    implicit val actorRefShow = new Show[ActorRef] {
      override def show( r: ActorRef ): String = actorPathShow show r.path
    }
    implicit val actorPathShow = new Show[ActorPath] {
      override def show( p: ActorPath ): String = p.toString
    }
  }
}
