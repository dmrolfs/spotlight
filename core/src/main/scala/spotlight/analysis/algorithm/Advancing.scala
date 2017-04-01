package spotlight.analysis.algorithm

import java.io.Serializable
import shapeless.the
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

/** Created by rolfsd on 2/17/17.
  */
trait Advancing[S <: Serializable] {
  def zero( configuration: Option[Config] ): S
  def N( shape: S ): Long
  def advance( original: S, advanced: AlgorithmProtocol.Advanced ): S
  def copy( shape: S ): S

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

object Advancing {
  object syntax {
    implicit class AdvancingOps[S <: Serializable: Advancing]( val shape: S ) {
      def N: Long = the[Advancing[S]].N( shape )
      def advance( advanced: AlgorithmProtocol.Advanced ): S = the[Advancing[S]].advance( shape, advanced )
      def copy: S = the[Advancing[S]].copy( shape )
    }
  }
}
