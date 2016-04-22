package spotlight.analysis.outlier

import scala.concurrent.duration.FiniteDuration
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, SupervisorStrategy}
import akka.event.LoggingReceive
import akka.stream.{ActorMaterializerSettings, _}
import akka.stream.scaladsl._
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
object OutlierPlanDetectionRouter {
  def props(
    _detectorRef: ActorRef,
    _detectionBudget: FiniteDuration,
    _bufferSize: Int,
    _maxInDetectionCpuFactor: Double
  ): Props = {
    Props(
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
    maxInDetectionCpuFactor: Double,
    plans: () => Set[OutlierPlan]
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[TimeSeries, Outliers, Unit] = {
    import peds.akka.stream.StreamMonitor._

    Flow[TimeSeries]
    .map { ts =>
      plans()
      .collect { case p if p appliesTo ts => OutlierDetectionMessage( ts, plan = p ).disjunction }
      .collect { case \/-( odm ) => odm }
    }
    .mapConcat { identity }
    .via {
      ProcessorAdapter.elasticProcessorFlow[OutlierDetectionMessage, DetectionResult]( maxInDetectionCpuFactor ) {
        case m => planDetectorRouterRef
      }
      .watchFlow( WatchPoints.PlanRouter )
    }
    .map { _.outliers }
  }


  object WatchPoints {
    val PlanRouter = Symbol( "plan.router" )
    val PlanFlow = Symbol( "plan.flow" )
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

  final case class PlanStream private[outlier]( ingressRef: ActorRef, graph: RunnableGraph[Unit] )

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
    case m @ OutlierDetectionMessage( p, t, _ ) => {
      val subscriber = sender()
      streamIngressFor( p, subscriber ) forward m
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
    planStreams.get( key )
    .map { _.ingressRef }
    .getOrElse {
      val ps = makePlanStream( plan, subscriber )
      planStreams += ( key -> ps )
      ps.ingressRef
    }
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
      .actorPublisher[OutlierDetectionMessage]( StreamIngress.props[OutlierDetectionMessage] )
      .toMat( Sink.asPublisher(false) )( Keep.both )
      .run()
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val ingressPublisher = b.add( Source fromPublisher ingress )
      val series = b.add( Flow[OutlierDetectionMessage].map{ _.source }.collect{ case ts: TimeSeries => ts } )
      val batch = plan.grouping map { g => b.add( batchSeries( g ) ) }
      val buffer = b.add( Flow[TimeSeriesBase].buffer(outer.bufferSize, OverflowStrategy.backpressure) )
      val detect = b.add( detectionFlow( plan ) )
      val egressSubscriber = b.add( Sink.actorSubscriber[Outliers]( StreamEgress.props(subscriber, high = 50) ) )
      val flow = batch map { bt =>
        ingressPublisher ~> series ~> bt ~> buffer ~> detect ~> egressSubscriber
      } getOrElse {
        ingressPublisher ~> series ~> buffer ~> detect ~> egressSubscriber
      }

      ClosedShape
    }

    val runnable = RunnableGraph fromGraph graph
    runnable.run()
    PlanStream( ingressRef, runnable )
  }

  def batchSeries(
    grouping: OutlierPlan.Grouping
  )(
    implicit tsMerging: Merging[TimeSeries]
  ): Flow[TimeSeries, TimeSeries, Unit] = {
    import StreamMonitor._
    import OutlierPlanDetectionRouter.WatchPoints

    Flow[TimeSeries]
    .groupedWithin( n = grouping.limit, d = grouping.window )
    .map {
      _.groupBy { _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
    .watchFlow( WatchPoints.PlanFlow )
  }

  def detectionFlow( plan: OutlierPlan )( implicit system: ActorSystem ): Flow[TimeSeriesBase, Outliers, Unit] = {
    Flow[TimeSeriesBase]
    .filter { plan.appliesTo }
    .map { ts => OutlierDetectionMessage( ts, plan ).disjunction }
    .collect { case \/-( m ) => m }
    .via {
      ProcessorAdapter.elasticProcessorFlow[OutlierDetectionMessage, DetectionResult]( outer.maxInDetectionCpuFactor ) {
        case m => outer.detector
      }
    }
    .map { _.outliers }
  }


  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy
}
