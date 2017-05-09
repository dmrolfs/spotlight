package spotlight.analysis.algorithm

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import com.persist.logging._
import net.ceedubs.ficus.readers.ValueReader

/** AlgorithmShapeCompanion can be extends by shape companion object to access commonly useful operations.
  * Created by rolfsd on 3/10/17.
  */
abstract class AlgorithmShapeCompanion extends ClassLogging {
  def getFrom[T: ValueReader]( configuration: Option[Config], path: String ): Option[T] = {
    for {
      c ← configuration
      v ← c.as[Option[T]]( path )
    } yield v
  }

  def getFromOrElse[T: ValueReader]( configuration: Option[Config], path: String, default: ⇒ T ): T = {
    getFrom[T]( configuration, path ) getOrElse default
  }
}
