package spotlight.analysis

import java.util.ServiceConfigurationError

import com.typesafe.config.Config
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced

/** Created by rolfsd on 10/8/15.
  */
package object algorithm {
  implicit val summaryStatisticsAdvancing = new Advancing[SummaryStatistics] {
    override def zero( configuration: Option[Config] ): SummaryStatistics = new SummaryStatistics()

    override def N( shape: SummaryStatistics ): Long = shape.getN

    override def advance( original: SummaryStatistics, advanced: Advanced ): SummaryStatistics = {
      val result = original.copy()
      result addValue advanced.point.value
      result
    }
  }

  trait OutlierAlgorithmError

  case class InsufficientAlgorithmError(
    algorithm: String,
    fqcn: String
  ) extends ServiceConfigurationError(
    s"Insufficient class identified [${fqcn}] for ${algorithm} algorithm. " +
      s"Algorithm implementations must extend from ${classOf[Algorithm[_]].getName}"
  ) with OutlierAlgorithmError

  case class InsufficientAlgorithmConfigurationError( algorithm: String, property: String )
    extends ServiceConfigurationError( s"Algorithm [${algorithm}] is not sufficiently configured for property [${property}]" )
    with OutlierAlgorithmError
}
