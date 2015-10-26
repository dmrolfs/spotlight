package lineup.analysis.outlier

import java.util.ServiceConfigurationError
import com.typesafe.scalalogging.LazyLogging
import peds.commons.log.Trace
import shapeless._
import shapeless.syntax.typeable._


/**
 * Created by rolfsd on 10/8/15.
 */
package object algorithm extends LazyLogging {
  val trace = Trace( "algorithm" )


  implicit class Properties( val underlying: Map[String, Any] ) extends AnyVal {
    def property[R](
      key: String,
      default: Option[R] = None
    )(
      implicit algorithm: Symbol, evTypeable: Typeable[R]
    ): R = trace.briefBlock( s"property($key, $default)($algorithm,...)" ) {
      val result = for {
        value <- underlying get key
        typed <- value.cast[R]
      } yield typed

      result getOrElse {
        default getOrElse {
          logger error s"no configuration for $algorithm property [$key]"
          throw InsufficientAlgorithmConfigurationError( algorithm, key )
        }
      }
    }
  }


  trait OutlierAlgorithmError

  case class InsufficientAlgorithmConfigurationError( algorithm: Symbol, property: String )
  extends ServiceConfigurationError( s"Algorithm [$algorithm] is not sufficiently configured for property [$property]" )
  with OutlierAlgorithmError
}
