package lineup.stream

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import akka.stream.scaladsl.Flow
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.codahale.metrics.graphite.{PickledGraphite, GraphiteReporter}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{Logger, StrictLogging}
import nl.grons.metrics.scala.{MetricName, InstrumentedBuilder}
import org.slf4j.LoggerFactory


/**
  * Created by rolfsd on 12/3/15.
  */
trait Monitor extends Monitor.Instrumented {
  def label: Symbol
  def metricLabel: String
  def count: Long
  def enter: Long
  def exit: Long

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

  override def toString: String = f"""$label[${count}]"""
}

object Monitor extends StrictLogging { outer =>
  val metricRegistry = new MetricRegistry

  def makeGraphiteReporter( host: String, port: Int = 2004 ): GraphiteReporter = {
    val graphite = new PickledGraphite( new InetSocketAddress( host, port ) )
    var result = GraphiteReporter
                  .forRegistry( metricRegistry )
                  .convertRatesTo( TimeUnit.SECONDS )
                  .convertDurationsTo( TimeUnit.MILLISECONDS )
                  .filter( MetricFilter.ALL )

    val config = ConfigFactory.load
    val EnvNamePath = "lineup.graphite.env-name"
    result = if ( config hasPath EnvNamePath ) {
      result.prefixedWith( config getString EnvNamePath )
    } else {
      result
    }

    result build graphite
  }


  trait Instrumented extends InstrumentedBuilder {
    override lazy val metricBaseName: MetricName = MetricName( classOf[Monitor].getCanonicalName.toLowerCase )
    override val metricRegistry: MetricRegistry = outer.metricRegistry
  }


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

  @inline def publish: Unit = {
//    for {
//      t <- tracked
//      m <- all get t
//    } {
//      m.metrics.gauge( m.metricLabel ) { m.count }
//    }

    logger info csvLine( tracked, all )
  }

  @inline def isEnabled: Boolean = logger.underlying.isInfoEnabled
  @inline def notEnabled: Boolean = !isEnabled


  override protected val logger: Logger = Logger( LoggerFactory getLogger "Monitor" )
  private var all: Map[Symbol, Monitor] = Map.empty[Symbol, Monitor]
  private var tracked: Seq[Symbol] = Seq.empty[Symbol]

  //todo could incorporate header and line into type class for different formats
  private def csvHeader( labels: Seq[Symbol] ): String = labels map { _.name } mkString ","
  private def csvLine( labels: Seq[Symbol], ms: Map[Symbol, Monitor] ): String = {
    labels.map{ ms get _ }.flatten.map{_.count }.mkString( "," )
  }


  final case class SourceMonitor private[stream]( override val label: Symbol ) extends Monitor {
    override val metricLabel: String = ".sourced.counts"
    private val counter = metrics.counter( label.name + metricLabel )
    override def count: Long = counter.count
    override def enter: Long = counter.count
    override def exit: Long = {
      counter.inc()
      counter.count
    }
    override def toString: String = f"""${label.name}[${count}>]"""
  }

  final case class FlowMonitor private[stream]( override val label: Symbol ) extends Monitor {
    override val metricLabel: String = ".in-flow.counts"
    private val counter = metrics.counter( label.name + metricLabel )
    override def count: Long = counter.count
    override def enter: Long = {
      counter.inc()
      counter.count
    }
    override def exit: Long = {
      counter.dec()
      counter.count
    }
    override def toString: String = f"""${label.name}[>${count}>]"""
  }

  final case class SinkMonitor private[stream]( override val label: Symbol ) extends Monitor {
    override val metricLabel: String = ".consumed.counts"
    private val counter = metrics.counter( label.name + metricLabel )
    override def count: Long = counter.count
    override def enter: Long = {
      counter.inc()
      counter.count
    }
    override def exit: Long = counter.count
    override def toString: String = f"""${label.name}[>${count}]"""
  }

  final case object SilentMonitor extends Monitor {
    override val label: Symbol = 'silent
    override val metricLabel: String = label.name
    override val count: Long = 0L
    override val enter: Long = 0L
    override val exit: Long = 0L
    override val toString: String = f"""${label.name}[${count}]"""
  }
}
