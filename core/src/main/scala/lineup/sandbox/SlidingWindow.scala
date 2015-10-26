package lineup.sandbox

import scala.concurrent.duration.FiniteDuration
import akka.stream.stage._
import org.joda.{ time => joda }
import com.github.nscala_time.time.Implicits._


object SlidingWindow {
  /**
   * Streaming stage that buffers events and slides a window over streaming input data. Transmits each observed window
   * downstream.
   *
   * @param releasePeriod duration of time frame series data is grouped into
   */
  def apply[In]( releasePeriod: FiniteDuration ): SlidingWindow[In] = new SlidingWindow( releasePeriod )
}

//todo alternative approach to define sliding window is defining a 2-inflow/1-out Shape; with 1-in assigned to trigger and other to elements
/**
 *
 * Created by rolfsd on 10/23/15.
 */
class SlidingWindow[In] private( releasePeriod: FiniteDuration ) extends PushPullStage[In, List[In]] {
  private var buffer: List[In] = List.empty[In]
  private var window: joda.Interval = frameWindowFrom( joda.DateTime.now )

  private def frameWindowFrom( start: joda.DateTime ): joda.Interval = start to ( start + releasePeriod.toMillis )


  override def onPush( elem: In, ctx: Context[List[In]] ): SyncDirective = {
    buffer ::= elem
    if ( window contains joda.DateTime.now ) ctx.pull() else releaseAndFrameWindow( ctx )
  }


  override def onPull( ctx: Context[List[In]] ): SyncDirective = {
    if ( ctx.isFinishing ) {
      // Streaming stage is shutting down, so we ensure that all buffer elements are flushed prior to finishing
      if ( buffer.isEmpty ) ctx.finish()
      else {
        // Buffer is non-empty, so empty it by sending undersized (non-empty) truncated window sequence - we will eventually finish here
        releaseAndFrameWindow( ctx, !window.contains(joda.DateTime.now) )
      }
    } else {
      if ( window contains joda.DateTime.now ) ctx.pull() else releaseAndFrameWindow( ctx )
    }
  }

  private def releaseAndFrameWindow( ctx: Context[List[In]], reframe: Boolean = true ): SyncDirective = {
    if ( reframe ) {
      window = frameWindowFrom( window.getEnd )
    }

    val released = buffer
    buffer = List.empty[In]
    ctx push released
  }

  override def onUpstreamFinish( ctx: Context[List[In]] ): TerminationDirective = ctx.absorbTermination()
}
