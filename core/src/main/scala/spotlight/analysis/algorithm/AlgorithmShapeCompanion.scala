package spotlight.analysis.algorithm

import scalaz.\/
import com.typesafe.config.Config
import com.persist.logging._

/** AlgorithmShapeCompanion can be extends by shape companion object to access commonly useful operations.
  * Created by rolfsd on 3/10/17.
  */
abstract class AlgorithmShapeCompanion extends ClassLogging {
  def valueFrom[V]( configuration: Option[Config], path: String, default: ⇒ V )( fn: ( Config, String ) ⇒ V ): V = {
    val v = for {
      c ← configuration if c hasPath path
      value ← \/.fromTryCatchNonFatal( fn( c, path ) ).toOption
    } yield value

    v getOrElse default
  }

  def valueOptionFrom[V](
    configuration: Option[Config],
    path: String,
    default: Option[V] = None
  )(
    fn: ( Config, String ) ⇒ V
  ): Option[V] = {
    val v = for {
      c ← configuration if c hasPath path
      value ← \/.fromTryCatchNonFatal( fn( c, path ) ).toOption
    } yield value

    default map { d ⇒ Option( v getOrElse d ) } getOrElse v
  }
}
