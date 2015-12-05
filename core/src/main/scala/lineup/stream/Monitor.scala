package lineup.stream

import java.util.concurrent.atomic.AtomicInteger
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.StrictLogging


/**
  * Created by rolfsd on 12/3/15.
  */
trait Monitor {
  def label: String
  def inProgress: Int
  def enter: Int
  def exit: Int

  def watch[I, O]( flow: Flow[I, O, Unit] ): Flow[I, O, Unit] = {
    Flow[I]
    .map { e =>
      if ( Monitor.isEnabled ) {
        enter
        Monitor.publish
      }

      e
    }
    .via( flow )
    .map { e =>
      if ( Monitor.isEnabled ) {
        exit
        Monitor.publish
      }

      e
    }
  }

  def block[R]( b: () => R ): R = {
    if ( Monitor.notEnabled ) b()
    else {
      enter
      val result = b()
      exit
      result
    }
  }

  override def toString: String = f"""$label[${inProgress}]"""
}

object Monitor extends StrictLogging {
  def source( label: String ): Monitor = {
    if ( notEnabled ) SilentMonitor
    else {
      val m = SourceMonitor( label )
      all :+= m
      m
    }
  }

  def flow( label: String ): Monitor = {
    if ( notEnabled ) SilentMonitor
    else {
      val m = FlowMonitor( label )
      all :+= m
      m
    }
  }

  def sink( label: String ): Monitor = {
    if ( notEnabled ) SilentMonitor
    else {
      val m = SinkMonitor( label )
      all :+= m
      m
    }
  }

  def publish: Unit = logger info all.mkString( "\t" )

  def isEnabled: Boolean = logger.underlying.isInfoEnabled
  @inline def notEnabled: Boolean = !isEnabled

  private var all: Seq[Monitor] = Seq.empty[Monitor]


  final case class SourceMonitor private[stream]( override val label: String ) extends Monitor {
    private val count = new AtomicInteger( 0 )
    override def inProgress: Int = count.intValue
    override def enter: Int = count.intValue
    override def exit: Int = count.incrementAndGet()
    override def toString: String = f"""$label[${inProgress}>]"""
  }

  final case class FlowMonitor private[stream]( override val label: String ) extends Monitor {
    private val count = new AtomicInteger( 0 )
    override def inProgress: Int = count.intValue
    override def enter: Int = count.incrementAndGet()
    override def exit: Int = count.decrementAndGet()
    override def toString: String = f"""$label[>${inProgress}>]"""
  }

  final case class SinkMonitor private[stream]( override val label: String ) extends Monitor {
    private val count = new AtomicInteger( 0 )
    override def inProgress: Int = count.intValue
    override def enter: Int = count.incrementAndGet()
    override def exit: Int = count.intValue
    override def toString: String = f"""$label[>${inProgress}]"""
  }

  final case object SilentMonitor extends Monitor {
    override val label: String = "silent"
    override val enter: Int = 0
    override val exit: Int = 0
    override val inProgress: Int = 0
    override val toString: String = f"""$label[${inProgress}]"""
  }
}
