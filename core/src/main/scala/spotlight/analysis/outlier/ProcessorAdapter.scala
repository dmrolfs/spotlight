package spotlight.analysis.outlier

import scala.reflect.ClassTag
import akka.actor.{ActorContext, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import akka.event.LoggingReceive
import akka.stream.{FlowShape, Graph, Materializer}
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, MaxInFlightRequestStrategy, RequestStrategy}
import akka.stream.scaladsl._
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.StreamMonitor
import spotlight.analysis.outlier.ProcessorAdapter.DeadWorkerError


/**
  * Created by rolfsd on 4/2/16.
  */
object ProcessorAdapter {
  def processorGraph[I, O](
    adapterPropsFromPublisher: ActorRef => Props,
    publisherProps: Props
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Graph[FlowShape[I, O], Unit] = {
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val ( publisherRef, publisher ) = {
        Source.actorPublisher[O]( publisherProps ).toMat( Sink.asPublisher(false) )( Keep.both ).run()
      }

      val processorPublisher = b.add( Source fromPublisher[O] publisher )
      val egress = b.add( Flow[O] map { identity } )

      processorPublisher ~> egress

      val adapter = b.add( Sink actorSubscriber[I] adapterPropsFromPublisher( publisherRef ) )
      FlowShape( adapter.in, egress.out )
    }
  }

  def fixedProcessorFlow[I, O: ClassTag](
    maxInFlight: Int
  )(
    workerPF: PartialFunction[Any, ActorRef]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[I, O, Unit] = {
    val publisherProps = ProcessorPublisher.props[O]
    val g = processorGraph[I, O](
      adapterPropsFromPublisher = ( publisher: ActorRef ) => { ProcessorAdapter.fixedProps( maxInFlight, publisher )( workerPF ) },
      publisherProps = publisherProps
    )

    import StreamMonitor._
    Flow.fromGraph( g ).watchFlow( ProcessorAdapter.WatchPoints.Processor )
  }

  def elasticProcessorFlow[I, O: ClassTag](
    maxInFlightCpuFactor: Double
  )(
    workerPF: PartialFunction[Any, ActorRef]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[I, O, Unit] = {
    val publisherProps = ProcessorPublisher.props[O]
    val g = processorGraph[I, O](
      adapterPropsFromPublisher = (publisher: ActorRef) => {
        ProcessorAdapter.elasticProps( maxInFlightCpuFactor, publisher )( workerPF )
      },
      publisherProps = publisherProps
    )

    import StreamMonitor._
    Flow.fromGraph( g ).watchFlow( ProcessorAdapter.WatchPoints.Processor )
  }

  def fixedProps( maxInFlightMessages: Int, publisher: ActorRef )( workerPF: PartialFunction[Any, ActorRef] ): Props = {
    Props(
      new ProcessorAdapter with TopologyProvider {
        override val workerFor: PartialFunction[Any, ActorRef] = workerPF
        override val maxInFlight: Int = maxInFlightMessages
        override def destinationPublisher( implicit ctx: ActorContext ): ActorRef = publisher
      }
    )
  }

  def elasticProps(
    maxInFlightMessagesCpuFactor: Double,
    publisher: ActorRef
  )(
    workerPF: PartialFunction[Any, ActorRef]
  ): Props = {
    Props(
      new ProcessorAdapter with TopologyProvider {
        override val workerFor: PartialFunction[Any, ActorRef] = workerPF
        override val maxInFlightCpuFactor: Double = maxInFlightMessagesCpuFactor
        override def destinationPublisher( implicit ctx: ActorContext ): ActorRef = publisher
      }
    )
  }

  object WatchPoints {
    val Processor = Symbol( "processor" )
  }


  trait TopologyProvider {
    def workerFor: PartialFunction[Any, ActorRef]
    def destinationPublisher( implicit context: ActorContext ): ActorRef
    def maxInFlight: Int = math.floor( Runtime.getRuntime.availableProcessors() * maxInFlightCpuFactor ).toInt
    def maxInFlightCpuFactor: Double = 1.0
  }


  case class DetectionJob( destination: ActorRef, startNanos: Long = System.nanoTime() )


  final case class DeadWorkerError private[outlier]( deadWorker: ActorRef )
  extends IllegalStateException( s"Flow Processor notified of worker death: [${deadWorker}]" )
}

class ProcessorAdapter extends ActorSubscriber with InstrumentedActor with ActorLogging {
  outer: ProcessorAdapter.TopologyProvider =>

  override lazy val metricBaseName: MetricName = MetricName( classOf[ProcessorAdapter] )
  val submissionMeter: Meter = metrics meter "submission"
  val reportMeter: Meter = metrics meter "report"

  var outstanding: Int = 0

  override protected def requestStrategy: RequestStrategy = {
    new MaxInFlightRequestStrategy( max = outer.maxInFlight ) {
      override def inFlightInternally: Int = outstanding
    }
  }

  override def receive: Receive = LoggingReceive { around( subscriber orElse publish ) }

  val subscriber: Receive = {
    case next @ ActorSubscriberMessage.OnNext( message ) if outer.workerFor.isDefinedAt( message ) => {
      submissionMeter.mark()
      outstanding += 1
      val worker = outer workerFor message
      context watch worker
      worker ! message
    }

    case ActorSubscriberMessage.OnComplete => outer.destinationPublisher ! ActorSubscriberMessage.OnComplete

    case onError: ActorSubscriberMessage.OnError => outer.destinationPublisher ! onError

    case Terminated( deadWorker ) => {
      log.error( "Flow Processor notified of worker death: [{}]", deadWorker )

      //todo is this response appropriate for spotlight and generally?
      outer.destinationPublisher ! ActorSubscriberMessage.OnError( DeadWorkerError(deadWorker) )
    }
  }

  val publish: Receive = {
    case message => {
      reportMeter.mark()
      outstanding -= 1
      outer.destinationPublisher ! message
    }
  }
}
