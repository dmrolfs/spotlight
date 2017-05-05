package spotlight.analysis

import scala.concurrent.Future
import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import demesne.BoundedContext

/** Created by rolfsd on 3/15/17.
  */
abstract class FlowFactory[I, O] extends Serializable {
  def makeFlow(
    parallelism: Int
  )(
    implicit
    boundedContext: BoundedContext,
    timeout: Timeout,
    materializer: Materializer
  ): Future[Flow[I, O, NotUsed]]
}
