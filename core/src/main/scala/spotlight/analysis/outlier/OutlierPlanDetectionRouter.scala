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
import peds.akka.stream.{MaxInFlightProcessorAdapter, StreamEgress, StreamIngress, StreamMonitor}
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.model.outlier.{OutlierPlan, Outliers}
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase}
import spotlight.model.timeseries.TimeSeriesBase.Merging


/**
  * Created by rolfsd on 4/19/16.
  */
object OutlierPlanDetectionRouter extends LazyLogging {
  def fixedPlanDetectionRouterFlow(
    planDetectionRouter: ActorRef,
    maxInFlight: Int
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[(TimeSeries, OutlierPlan), Outliers, NotUsed] = {
    MaxInFlightProcessorAdapter.fixedProcessorFlow[(TimeSeries, OutlierPlan), Outliers](
      name = WatchPoints.PlanRouter.name,
      maxInFlight = maxInFlight
    ) {
      case m => planDetectionRouter
    }
    .named( "fixed-plan-router-stage" )
  }

  def elasticPlanDetectionRouterFlow(
    planDetectorRouterRef: ActorRef,
    maxInDetectionCpuFactor: Double
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[(TimeSeries, OutlierPlan), Outliers, NotUsed] = {
    MaxInFlightProcessorAdapter.elasticProcessorFlow[(TimeSeries, OutlierPlan), Outliers](
      name = WatchPoints.PlanRouter.name,
      maxInDetectionCpuFactor
    ) {
      case m => planDetectorRouterRef
    }
    .named( "elastic-plan-router-stage" )
  }


  def props(
    detectorRef: ActorRef,
    detectionBudget: FiniteDuration,
    bufferSize: Int,
    maxInDetectionCpuFactor: Double
  ): Props = Props( new Default(detectorRef, detectionBudget, bufferSize, maxInDetectionCpuFactor) )

  private class Default(
    override val detector: ActorRef,
    override val detectionBudget: FiniteDuration,
    override val bufferSize: Int,
    override val maxInDetectionCpuFactor: Double
  ) extends OutlierPlanDetectionRouter with ConfigurationProvider


  object WatchPoints {
    val PlanRouter = Symbol( "plan.router" )
  }


  final case class Key private[outlier]( planId: OutlierPlan#TID, subscriber: ActorRef ) extends Equals {
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
      .actorPublisher[TimeSeries]( StreamIngress.props[TimeSeries] ).named( s"${plan.name}-detect-streamlet-inlet" )
      .toMat( Sink.asPublisher(false).named( s"${plan.name}-detect-streamlet-outlet") )( Keep.both )
      .run()
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val ingressPublisher = b.add( Source.fromPublisher( ingress ).named( s"${plan.name}-detect-streamlet" ) )
      val batch = plan.grouping map { g => b.add( batchSeries( g ) ) }
      val buffer = b.add( Flow[TimeSeriesBase].buffer(outer.bufferSize, OverflowStrategy.backpressure) )
      val detect = b.add( detectionFlow( plan ) )
      val egressSubscriber = b.add(
        Sink.actorSubscriber[Outliers]( StreamEgress.props(subscriber, high = 50) ).named( s"${plan.name}-detect-streamlet")
      )

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

  def detectionFlow( plan: OutlierPlan )( implicit system: ActorSystem ): Flow[TimeSeriesBase, Outliers, NotUsed] = {
    val label = Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + plan.name )

    val flow = Flow[TimeSeriesBase]
    .filter { plan.appliesTo }
    .map { ts => OutlierDetectionMessage( ts, plan ).disjunction }
    .collect { case \/-( m ) => m }
    .via {
      MaxInFlightProcessorAdapter.elasticProcessorFlow[OutlierDetectionMessage, DetectionResult](
        label.name,
        outer.maxInDetectionCpuFactor
      ) {
        case m => outer.detector
      }
    }
    .map { _.outliers }
    .named( s"${plan.name}-detection" )

    StreamMonitor.add( label )
    flow
  }


  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy
}
