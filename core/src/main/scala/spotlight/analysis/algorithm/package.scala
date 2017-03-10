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

    override def copy( shape: SummaryStatistics ): SummaryStatistics = shape.copy()
  }

  /** Errors during algorithm scoring that are marked with [[NonFatalSkipScoringException]] will not stop further anaylsis and
    * the corresponding data point will be skipped and cannot be marked as an anomaly.
    */
  trait NonFatalSkipScoringException extends Throwable

  /** Many algorithm must see a certain number of points before they can determine anomalies. Points seen before that point are
    * skipped.
    * @param algorithm
    * @param size
    * @param required
    */
  case class InsufficientDataSize(
    algorithm: String,
    size: Long,
    required: Long
  ) extends IllegalArgumentException(
    s"${size} data points is insufficient to perform ${algorithm} test, which requires at least ${required} points"
  ) with NonFatalSkipScoringException

  /** Scoring that results in insignificant variance -- particularly after one one data point -- have threshold boundaries where
    * the floor and ceiling are equal or otherwise very very near the expected value. In order to avoid the potential false
    * positives that may occur for this scenario, corresponding data points are skipped until there is a data variance.
    * @param algorithm
    * @param N
    */
  case class InsufficientVariance( algorithm: String, N: Long ) extends NonFatalSkipScoringException {
    override def getMessage: String = s"insufficient variance may result in false anomalies"
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
