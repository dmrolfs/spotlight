package spotlight.analysis.algorithm

import java.io.Serializable
import scalaz.\/
import com.typesafe.config.Config

/** Created by rolfsd on 2/17/17.
  */
trait Advancing[S <: Serializable] {
  def zero( configuration: Option[Config] ): S
  def N( shape: S ): Long
  def advance( original: S, advanced: AlgorithmProtocol.Advanced ): S

  //todo: there's a functional way to achieve this :-)
  def valueFrom[V]( configuration: Option[Config], path: String )( fn: Config ⇒ V ): Option[V] = {
    for {
      c ← configuration if c hasPath path
      value ← \/.fromTryCatchNonFatal { fn( c ) }.toOption
    } yield value
  }
}
