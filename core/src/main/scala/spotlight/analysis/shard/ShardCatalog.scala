package spotlight.analysis.shard

import scala.reflect._
import scalaz.{-\/, Validation, \/-}
import peds.archetype.domain.model.core.Entity
import peds.commons.{TryV, Valid}
import peds.commons.identifier.{Identifying, ShortUUID}
import peds.commons.util._
import spotlight.analysis.algorithm.AlgorithmModule
import spotlight.model.outlier.AnalysisPlan


/**
  * Created by rolfsd on 1/18/17.
  */
trait ShardCatalog extends Entity {
  override type ID = ShardCatalog.ID
  override val evID: ClassTag[ID] = classTag[ShardCatalog.ID]
  override val evTID: ClassTag[TID] = classTag[TID]
  val nextAlgorithmId: () => AlgorithmModule.TID
}

object ShardCatalog {
  def idFor[I]( plan: AnalysisPlan.Summary, algorithmLabel: String )( implicit identifying: Identifying[I] ): ShardCatalog#TID = {
    identifying.tag( ShardCatalog.ID( plan.id, algorithmLabel ).asInstanceOf[identifying.ID] ).asInstanceOf[ShardCatalog#TID]
  }


  final case class ID( planId: AnalysisPlan#ID, algorithmLabel: String ) {
    override def toString: String = planId + ":" + algorithmLabel
  }


  object ID {
    def fromString( rep: String ): Valid[ID] = {
      Validation
      .fromTryCatchNonFatal {
        val Array( pid, algo ) = rep.split( ':' )
        ID( planId = ShortUUID(pid), algorithmLabel = algo )
      }
      .toValidationNel
    }
  }



  trait ShardCatalogIdentifying[T <: ShardCatalog] extends Identifying[T] {
    override type ID = ShardCatalog.ID
    override val evID: ClassTag[ID] = classTag[ID]
    override val evTID: ClassTag[TID] = classTag[TID]

    override def fromString( idrep: String ): ID = {
      ShardCatalog.ID.fromString( idrep ).disjunction match {
        case \/-( id ) => id
        case -\/( exs ) => {
          exs foreach { ex => logger.error( s"failed to parse id string [${idrep}] into ${evID}", ex ) }
          throw exs.head
        }
      }
    }

    override def nextId: TryV[TID] = -\/( new IllegalStateException(s"${getClass.safeSimpleName} does not support nextId") )

    override def idOf( c: T ): TID = c.id
  }
}
