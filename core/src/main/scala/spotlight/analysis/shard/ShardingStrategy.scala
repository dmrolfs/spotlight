package spotlight.analysis.shard

import akka.actor.ActorRef
import demesne.{AggregateRootType, DomainModel}
import peds.commons.identifier.Identifying
import peds.akka.envelope._
import spotlight.model.outlier.OutlierPlan
import spotlight.model.outlier.OutlierPlan.Summary

/**
  * Created by rolfsd on 1/20/17.
  */
sealed trait ShardingStrategy {
  val rootType: AggregateRootType
  def makeAddCommand( plan: OutlierPlan.Summary, algorithmRootType: AggregateRootType ): Option[Any]

  implicit lazy val identifying: Identifying[ShardCatalog.ID] = rootType.identifying.asInstanceOf[Identifying[ShardCatalog.ID]]

  def actorFor( plan: OutlierPlan.Summary, algorithmRootType: AggregateRootType )( implicit model: DomainModel ): ActorRef = {
    val sid = idFor( plan, algorithmRootType.name )
    val ref = model( rootType, sid )
    val add = makeAddCommand( plan, algorithmRootType )
    add foreach { ref !+ _ }
    ref
  }

  def idFor( plan: OutlierPlan.Summary, algorithmLabel: String )( implicit identifying: Identifying[ShardCatalog#ID] ): ShardCatalog#TID = {
    identifying.tag( ShardCatalog.ID( plan.id, algorithmLabel ).asInstanceOf[identifying.ID] ).asInstanceOf[ShardCatalog#TID]
  }
}


case object CellShardingStrategy extends ShardingStrategy {
  override val rootType: AggregateRootType = CellShardModule.module.rootType

  override def makeAddCommand( plan: Summary, algorithmRootType: AggregateRootType ): Option[Any] = {
    Some( CellShardProtocol.Add( targetId = idFor(plan, algorithmRootType.name), plan, algorithmRootType, nrCells = 5000 ) )
  }
}

case object LookupShardingStrategy extends ShardingStrategy {
  override val rootType: AggregateRootType = LookupShardModule.rootType

  override def makeAddCommand( plan: Summary, algorithmRootType: AggregateRootType ): Option[Any] = {
    Some( LookupShardProtocol.Add( targetId = idFor(plan, algorithmRootType.name), plan, algorithmRootType ) )
  }
}
