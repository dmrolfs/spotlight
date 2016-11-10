package spotlight.model.outlier

import peds.akka.envelope.WorkId
import shapeless.Lens
import spotlight.model.outlier.OutlierPlan.Scope


/**
  * Created by rolfsd on 11/4/16.
  */
trait CorrelatedData[T] {
  def data: T
  def correlationIds: Set[WorkId]
  def scope: Option[OutlierPlan.Scope] = None
  def withData( newData: T ): CorrelatedData[T]
  def withCorrelationIds( newIds: Set[WorkId] ): CorrelatedData[T]
  def withScope( newScope: Option[OutlierPlan.Scope] ): CorrelatedData[T]
}

object CorrelatedData {
  def unapply( cdata: CorrelatedData[_] ): Option[(Any, Set[WorkId], Option[OutlierPlan.Scope])] = {
    Some( cdata.data, cdata.correlationIds, cdata.scope )
  }

  def dataLens[T]: Lens[CorrelatedData[T], T] = new Lens[CorrelatedData[T], T] {
    override def get( cd: CorrelatedData[T] ): T = cd.data
    override def set( cd: CorrelatedData[T] )( d: T ): CorrelatedData[T] = cd withData d
  }

  def correlationIdsLens[T]: Lens[CorrelatedData[T], Set[WorkId]] = new Lens[CorrelatedData[T], Set[WorkId]] {
    override def get( cd: CorrelatedData[T] ): Set[WorkId] = cd.correlationIds
    override def set( cd: CorrelatedData[T] )( cids: Set[WorkId] ): CorrelatedData[T] = cd withCorrelationIds cids
  }

  def scopeLens[T]: Lens[CorrelatedData[T], Option[OutlierPlan.Scope]] = new Lens[CorrelatedData[T], Option[OutlierPlan.Scope]] {
    override def get( cd: CorrelatedData[T] ): Option[Scope] = cd.scope
    override def set( cd: CorrelatedData[T] )( s: Option[Scope] ): CorrelatedData[T] = cd withScope s
  }
}
