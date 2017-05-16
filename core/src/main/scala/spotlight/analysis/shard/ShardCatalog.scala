package spotlight.analysis.shard

import cats.data.Validated
import cats.syntax.either._
import omnibus.archetype.domain.model.core.Entity
import omnibus.commons._
import omnibus.commons.identifier.{ Identifying, ShortUUID }
import omnibus.commons.util._
import spotlight.analysis.algorithm.AlgorithmIdGenerator
import spotlight.model.outlier.AnalysisPlan

/** Created by rolfsd on 1/18/17.
  */
trait ShardCatalog extends Entity {
  override type ID = ShardCatalog.ID
  val idGenerator: AlgorithmIdGenerator
}

object ShardCatalog {
  def idFor[T <: ShardCatalog](
    plan: AnalysisPlan.Summary,
    algorithmLabel: String
  )(
    implicit
    identifying: Identifying.Aux[T, ShardCatalog.ID]
  ): ShardCatalog#TID = identifying.tag( ShardCatalog.ID( plan.id, algorithmLabel ) )

  final case class ID( planId: AnalysisPlan#ID, algorithmLabel: String ) {
    override def toString: String = algorithmLabel + ID.Delimeter + planId
  }

  object ID {
    val Delimeter = '@'
    def fromString( rep: String ): AllIssuesOr[ID] = {
      Validated.catchNonFatal {
        val Array( algo, pid ) = rep.split( Delimeter )
        ID( planId = ShortUUID.fromString( pid ), algorithmLabel = algo )
      }
        .toValidatedNel
    }
  }

  trait ShardCatalogIdentifying[T <: ShardCatalog] extends Identifying[T] {
    override type ID = ShardCatalog.ID
    override def tidOf( c: T ): TID = c.id
    override def nextTID: ErrorOr[TID] = new IllegalStateException( s"${getClass.safeSimpleName} does not support nextId" ).asLeft
    override def idFromString( idRep: String ): ID = ShardCatalog.ID.fromString( idRep ).unsafeGet
  }
}
