package lineup.analysis.outlier

import java.util.ServiceConfigurationError
import com.typesafe.scalalogging.LazyLogging


/**
 * Created by rolfsd on 10/8/15.
 */
package object algorithm extends LazyLogging {
  trait OutlierAlgorithmError

  case class InsufficientAlgorithmConfigurationError( algorithm: Symbol, property: String )
  extends ServiceConfigurationError( s"Algorithm [$algorithm] is not sufficiently configured for property [$property]" )
  with OutlierAlgorithmError
}
