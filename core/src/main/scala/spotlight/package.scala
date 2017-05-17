import scala.concurrent.ExecutionContext
import akka.actor.{ ActorContext, ActorSystem }
import akka.stream.Materializer
import akka.util.Timeout
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
}
