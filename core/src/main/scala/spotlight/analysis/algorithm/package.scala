package spotlight.analysis

import java.util.ServiceConfigurationError


/**
 * Created by rolfsd on 10/8/15.
 */
package object algorithm {
  trait OutlierAlgorithmError


  case class InsufficientAlgorithmModuleError(
    algorithm: Symbol,
    fqcn: String
  ) extends ServiceConfigurationError(
    s"Insufficient class identified [${fqcn}] for ${algorithm.name} algorithm. " +
    "Algorithm implementations must extend from spotlight.analysis.outlier.algorithm.AlgorithmModule"
  ) with OutlierAlgorithmError


  case class InsufficientAlgorithmConfigurationError( algorithm: Symbol, property: String )
  extends ServiceConfigurationError( s"Algorithm [$algorithm] is not sufficiently configured for property [$property]" )
  with OutlierAlgorithmError
}
