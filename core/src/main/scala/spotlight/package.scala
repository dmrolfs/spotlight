import com.typesafe.config.ConfigFactory

/** Created by rolfsd on 1/17/17.
  */
package object spotlight {
  val MetricBasePath = "spotlight.metrics.basename"

  lazy val BaseMetricName: String = {
    val base = "spotlight"
    val config = ConfigFactory.load()
    if ( config hasPath MetricBasePath ) {
      config.getString( MetricBasePath ) + "." + base
    } else {
      base
    }
  }
}
