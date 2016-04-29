package spotlight.analysis.outlier

import akka.NotUsed

import scala.concurrent.duration.FiniteDuration
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, SupervisorStrategy}
import akka.event.LoggingReceive
import akka.stream.{ActorMaterializerSettings, _}
import akka.stream.scaladsl._
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Meter

import scalaz.\/-
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.{ProcessorAdapter, StreamEgress, StreamIngress, StreamMonitor}
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.model.outlier.{OutlierPlan, Outliers}
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase}
import spotlight.model.timeseries.TimeSeriesBase.Merging


/**
  * Created by rolfsd on 4/19/16.
  */
object OutlierPlanDetectionRouter extends LazyLogging {
  def props(
    _detectorRef: ActorRef,
    _detectionBudget: FiniteDuration,
    _bufferSize: Int,
    _maxInDetectionCpuFactor: Double
  ): Props = {
    Props(
      //todo change to vals where possible?
      new OutlierPlanDetectionRouter with ConfigurationProvider {
        override def detector: ActorRef = _detectorRef
        override def detectionBudget: FiniteDuration = _detectionBudget
        override def bufferSize: Int = _bufferSize
        override def maxInDetectionCpuFactor: Double = _maxInDetectionCpuFactor
      }
    )
  }

  def elasticPlanDetectionRouterFlow(
    planDetectorRouterRef: ActorRef,
    maxInDetectionCpuFactor: Double
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[(TimeSeries, OutlierPlan), Outliers, NotUsed] = {
    ProcessorAdapter.elasticProcessorFlow[(TimeSeries, OutlierPlan), Outliers](
      maxInDetectionCpuFactor,
      label = WatchPoints.PlanRouter
    ) {
      case m => planDetectorRouterRef
    }
  }


  object WatchPoints {
    val PlanRouter = Symbol( "plan.router" )
  }


  final case class Key private[outlier]( planId: OutlierPlan.TID, subscriber: ActorRef ) extends Equals {
    override def canEqual( rhs : Any ): Boolean = rhs.isInstanceOf[Key]

    override def hashCode(): Int = {
      41 * (
        41 + planId.##
      ) + subscriber.##
    }

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: Key => {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.planId == that.planId ) &&
            ( this.subscriber == that.subscriber )
          }
        }

        case _ => false
      }
    }
  }

  final case class PlanStream private[outlier]( ingressRef: ActorRef, graph: RunnableGraph[NotUsed] )

  trait ConfigurationProvider {
    def detector: ActorRef
    def maxInDetectionCpuFactor: Double
    def detectionBudget: FiniteDuration
    def bufferSize: Int
  }
}

class OutlierPlanDetectionRouter extends Actor with InstrumentedActor with ActorLogging {
  outer: OutlierPlanDetectionRouter.ConfigurationProvider =>

  import OutlierPlanDetectionRouter.{Key, PlanStream}

  lazy val planStreamFailureMeter: Meter = metrics meter "plan.failures"

  var planStreams: Map[Key, PlanStream] = Map.empty[Key, PlanStream]


  override def postStop(): Unit = {
    planStreams.values foreach { ps =>
      ps.ingressRef ! StreamIngress.CompleteAndStop
    }
  }

  override def receive: Receive = LoggingReceive{ around( route ) }

  val workflowSupervision: Supervision.Decider = {
    case ex => {
      log.error( ex, "Error caught by Supervisor" )
      planStreamFailureMeter.mark( )
      Supervision.Restart
    }
  }

  implicit val system: ActorSystem = context.system
  implicit val materializer = ActorMaterializer( ActorMaterializerSettings( system ) withSupervisionStrategy workflowSupervision )

  val route: Receive = {
    case (ts: TimeSeries, p: OutlierPlan) => {
      val subscriber = sender()
      streamIngressFor( p, subscriber ) forward ts
    }
  }


  def streamIngressFor(
    plan: OutlierPlan,
    subscriber: ActorRef
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): ActorRef = {
    val key = Key( plan.id, subscriber )

    val ingress = {
      planStreams
      .get( key )
      .map { _.ingressRef }
      .getOrElse {
        val ps = makePlanStream( plan, subscriber )
        planStreams += ( key -> ps )
        ps.ingressRef
      }
    }

    log.debug( "OutlierPlanDetectionRouter: [{} {}] ingress = {}", plan.name, subscriber, ingress )
    ingress
  }

  def makePlanStream(
    plan: OutlierPlan,
    subscriber: ActorRef
  )(
    implicit tsMerging: Merging[TimeSeries],
    system: ActorSystem,
    materializer: Materializer
  ): PlanStream = {
    log.info( "OutlierPlanDetectionRouter making new flow for plan:subscriber: [{} : {}]", plan.name, subscriber )

    val (ingressRef, ingress) = {
      Source
      .actorPublisher[TimeSeries]( StreamIngress.props[TimeSeries] )
      .toMat( Sink.asPublisher(false) )( Keep.both )
      .run()
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val ingressPublisher = b.add( Source fromPublisher ingress )
      val batch = plan.grouping map { g => b.add( batchSeries( g ) ) }
      val buffer = b.add( Flow[TimeSeriesBase].buffer(outer.bufferSize, OverflowStrategy.backpressure) )
      val detect = b.add( detectionFlow( plan ) )
      val egressSubscriber = b.add( Sink.actorSubscriber[Outliers]( StreamEgress.props(subscriber, high = 50) ) )

      if ( batch.isDefined ) {
        ingressPublisher ~> batch.get ~> buffer ~> detect ~> egressSubscriber
      } else {
        ingressPublisher ~> buffer ~> detect ~> egressSubscriber
      }

      ClosedShape
    }

    val runnable = RunnableGraph.fromGraph( graph ).named( s"plan-detection-${plan.name}-${subscriber.path}" )
    runnable.run()
    PlanStream( ingressRef, runnable )
  }

  def batchSeries(
    grouping: OutlierPlan.Grouping
  )(
    implicit tsMerging: Merging[TimeSeries]
  ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    log.debug( "batchSeries grouping = [{}]", grouping )

    Flow[TimeSeries]
    .groupedWithin( n = grouping.limit, d = grouping.window )
    .map {
      _.groupBy { _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
  }

  val detectionId = new java.util.concurrent.atomic.AtomicInteger()

  def detectionFlow( plan: OutlierPlan )( implicit system: ActorSystem ): Flow[TimeSeriesBase, Outliers, NotUsed] = {
    val label = Symbol( s"${OutlierDetection.WatchPoints.DetectionFlow.name}-${plan.name}-${detectionId.incrementAndGet()}" )

    val flow = Flow[TimeSeriesBase]
    .filter { plan.appliesTo }
    .map { ts => OutlierDetectionMessage( ts, plan ).disjunction }
    .collect { case \/-( m ) => m }
    .via {
      ProcessorAdapter.elasticProcessorFlow[OutlierDetectionMessage, DetectionResult](
        outer.maxInDetectionCpuFactor,
        label
      ) {
        case m => outer.detector
      }
    }
    .map { _.outliers }

    StreamMonitor.add( label )
    flow
  }


  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy
}
