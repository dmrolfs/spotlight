import com.typesafe.config.ConfigFactory

import net.ceedubs.ficus.Ficus._

/** Created by rolfsd on 1/17/17.
  */
package object spotlight {
  val MetricBasePath = "spotlight.metrics.basename"

  lazy val BaseMetricName: String = {
    val base = "spotlight"
    ConfigFactory.load().as[Option[String]]( MetricBasePath ) map { _ + "." + base } getOrElse { base }
  }
}
