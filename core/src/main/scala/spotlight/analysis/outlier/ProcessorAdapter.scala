package spotlight.analysis.outlier

import scala.reflect.ClassTag
import akka.actor.{ActorContext, ActorLogging, ActorRef, ActorSystem, Props}
import akka.event.LoggingReceive
import akka.stream.{FlowShape, Materializer}
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, MaxInFlightRequestStrategy, RequestStrategy}
import akka.stream.scaladsl._
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.StreamMonitor


/**
  * Created by rolfsd on 4/2/16.
  */
object ProcessorAdapter {
  def processorFlow[I, O: ClassTag](
    maxInFlightCpuFactor: Double
  )(
    workerPF: PartialFunction[Any, ActorRef]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[I, O, Unit] = {
    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val (publisherRef, publisher) = {
        Source.actorPublisher[O]( ProcessorPublisher.props[O] ).toMat( Sink.asPublisher(false) )( Keep.both ).run()
      }

      val processorPublisher = b.add( Source fromPublisher publisher )
      val egress = b.add( Flow[O] map { identity } )

      processorPublisher ~> egress

      val adapterProps = ProcessorAdapter.props( maxInFlightCpuFactor, publisherRef )( workerPF )
      val adapter = b.add( Sink actorSubscriber adapterProps )

      FlowShape( adapter.in, egress.out )
    }

    import StreamMonitor._
    Flow.fromGraph( graph ).watchFlow( ProcessorAdapter.WatchPoints.Processor )
  }

  def props( maxInFlightCpuFactor: Double, publisher: ActorRef )( workerPF: PartialFunction[Any, ActorRef] ): Props = {
    Props(
      new ProcessorAdapter with TopologyProvider {
        override def workerFor: PartialFunction[ Any, ActorRef ] = workerPF
        override def destinationPublisher(implicit context: ActorContext): ActorRef = publisher
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
      outer.workerFor( message ) ! message
    }

    case ActorSubscriberMessage.OnComplete => outer.destinationPublisher ! ActorSubscriberMessage.OnComplete

    case onError: ActorSubscriberMessage.OnError => outer.destinationPublisher ! onError
  }

  val publish: Receive = {
    case message => {
      reportMeter.mark()
      outstanding -= 1
      outer.destinationPublisher ! message
    }
  }
}
