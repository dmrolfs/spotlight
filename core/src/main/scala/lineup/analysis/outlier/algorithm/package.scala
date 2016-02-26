package lineup.analysis.outlier

import java.util.ServiceConfigurationError


/**
 * Created by rolfsd on 10/8/15.
 */
package object algorithm {
  trait OutlierAlgorithmError

  case class InsufficientAlgorithmConfigurationError( algorithm: Symbol, property: String )
  extends ServiceConfigurationError( s"Algorithm [$algorithm] is not sufficiently configured for property [$property]" )
  with OutlierAlgorithmError
}
