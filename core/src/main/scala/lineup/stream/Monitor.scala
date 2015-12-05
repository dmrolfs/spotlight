package lineup.stream

import java.util.concurrent.atomic.AtomicInteger
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.slf4j.LoggerFactory


/**
  * Created by rolfsd on 12/3/15.
  */
trait Monitor {
  def label: Symbol
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
  def set( labels: Symbol* ): Unit = {
    tracked = labels filter { all contains _ }
    logger info csvHeader( tracked )
  }

  def source( label: Symbol ): Monitor = {
    if ( notEnabled ) SilentMonitor
    else {
      val m = SourceMonitor( label )
      all += ( label -> m )
      m
    }
  }

  def flow( label: Symbol ): Monitor = {
    if ( notEnabled ) SilentMonitor
    else {
      val m = FlowMonitor( label )
      all += ( label -> m )
      m
    }
  }

  def sink( label: Symbol ): Monitor = {
    if ( notEnabled ) SilentMonitor
    else {
      val m = SinkMonitor( label )
      all += ( label -> m )
      m
    }
  }

  @inline def publish: Unit = logger info csvLine( tracked, all )

  @inline def isEnabled: Boolean = logger.underlying.isInfoEnabled
  @inline def notEnabled: Boolean = !isEnabled


  override protected val logger: Logger = Logger( LoggerFactory getLogger "Monitor" )
  private var all: Map[Symbol, Monitor] = Map.empty[Symbol, Monitor]
  private var tracked: Seq[Symbol] = Seq.empty[Symbol]

  //todo could incorporate header and line into type class for different formats
  private def csvHeader( labels: Seq[Symbol] ): String = labels map { _.name } mkString ","
  private def csvLine( labels: Seq[Symbol], ms: Map[Symbol, Monitor] ): String = {
    labels.map{ ms get _ }.flatten.map{ _.inProgress }.mkString( "," )
  }


  final case class SourceMonitor private[stream]( override val label: Symbol ) extends Monitor {
    private val count = new AtomicInteger( 0 )
    override def inProgress: Int = count.intValue
    override def enter: Int = count.intValue
    override def exit: Int = count.incrementAndGet()
    override def toString: String = f"""${label.name}[${inProgress}>]"""
  }

  final case class FlowMonitor private[stream]( override val label: Symbol ) extends Monitor {
    private val count = new AtomicInteger( 0 )
    override def inProgress: Int = count.intValue
    override def enter: Int = count.incrementAndGet()
    override def exit: Int = count.decrementAndGet()
    override def toString: String = f"""${label.name}[>${inProgress}>]"""
  }

  final case class SinkMonitor private[stream]( override val label: Symbol ) extends Monitor {
    private val count = new AtomicInteger( 0 )
    override def inProgress: Int = count.intValue
    override def enter: Int = count.incrementAndGet()
    override def exit: Int = count.intValue
    override def toString: String = f"""${label.name}[>${inProgress}]"""
  }

  final case object SilentMonitor extends Monitor {
    override val label: Symbol = 'silent
    override val enter: Int = 0
    override val exit: Int = 0
    override val inProgress: Int = 0
    override val toString: String = f"""${label.name}[${inProgress}]"""
  }
}
