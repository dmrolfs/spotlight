package spotlight.analysis.algorithm.shard

import demesne.MessageLike


/**
  * Created by rolfsd on 1/18/17.
  */
object ShardProtocol {
  case class RouteMessage( override val targetId: RouteMessage#TID, payload: Any ) extends MessageLike {
    override type ID = ShardCatalog.ID
  }
}
