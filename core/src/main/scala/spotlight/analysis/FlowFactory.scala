package spotlight.analysis

import scala.concurrent.Future
import akka.NotUsed
import akka.stream.scaladsl.Flow
import spotlight.{ BC, T, M }

/** Created by rolfsd on 3/15/17.
  */
abstract class FlowFactory[I, O] extends Serializable {
  def makeFlow[_: BC: T: M]( parallelism: Int ): Future[Flow[I, O, NotUsed]]
}
