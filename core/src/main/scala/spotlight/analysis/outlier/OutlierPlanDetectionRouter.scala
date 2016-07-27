package spotlight.analysis.outlier

import scala.concurrent.duration._
import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, Props, Terminated}
import akka.event.LoggingReceive
import akka.stream.actor.ActorSubscriberMessage
import akka.stream.{ActorMaterializerSettings, _}
import akka.stream.scaladsl._

import scalaz.\/-
import shapeless.TypeCase
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Meter
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.{CommonActorPublisher, StreamIngress}
import peds.akka.stream.StreamMonitor._
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.analysis.outlier.OutlierPlanDetectionRouter.StreamMessage
import spotlight.model.outlier.{OutlierPlan, Outliers}
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase}
import spotlight.model.timeseries.TimeSeriesBase.Merging


/**
  * Created by rolfsd on 4/19/16.
  */
//todo remove?
object OutlierPlanDetectionRouter extends LazyLogging {
  type TimeSeriesScope = (TimeSeries, OutlierPlan.Scope)

  def flow(
    planDetectionRouter: ActorRef
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Flow[TimeSeriesScope, Outliers, NotUsed] = {
    val outletProps = CommonActorPublisher.props[DetectionResult]

    val g = GraphDSL.create() { implicit b =>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val (outletRef, outlet) = {
        Source.actorPublisher[DetectionResult]( outletProps ).named( "plan-router-outlet" )
        .toMat( Sink.asPublisher(false) )( Keep.both )
        .run()
      }

      val addSubscriber = b.add( Flow[TimeSeriesScope] map { tss => StreamMessage(tss, outletRef) } )
      val workStage = b.add(
//todo: BAD discontinuity between orig (ts, plan) and new (ts, scope).
//todo: Will be fixed by ultimate removal of OutlierPlanDetectionRouter
        Sink
        .actorRef[StreamMessage[TimeSeriesScope]]( planDetectionRouter, ActorSubscriberMessage.OnComplete )
        .named( "plan-router-inlet")
      )
      addSubscriber ~> workStage

      val outletStage = b.add( Source fromPublisher[DetectionResult] outlet )
      val extractOutliers = b.add( Flow[DetectionResult]  map { _.outliers } )
      outletStage ~> extractOutliers

      FlowShape( addSubscriber.in, extractOutliers.out )
    }

    Flow.fromGraph( g ).watchFlow( Symbol("fixed-plan-router") )
  }

  private[outlier] case class StreamMessage[P]( payload: P, subscriber: ActorRef )


  def props(
    detectorRef: ActorRef,
    detectionBudget: FiniteDuration,
    bufferSize: Int,
    maxInDetectionCpuFactor: Double
  ): Props = Props( new Default(detector = detectorRef, detectionBudget, bufferSize, maxInDetectionCpuFactor) )

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
    def completionTimeout: FiniteDuration = 1.minute
  }
}

class OutlierPlanDetectionRouter extends Actor with InstrumentedActor with ActorLogging {
  outer: OutlierPlanDetectionRouter.ConfigurationProvider =>

  import OutlierPlanDetectionRouter.{Key, PlanStream}

  lazy val planStreamFailureMeter: Meter = metrics meter "plan.failures"

  var planStreams: Map[Key, PlanStream] = Map.empty[Key, PlanStream]


  override def preStart(): Unit = {
    super.preStart()
    context watch outer.detector
  }

  override def postStop(): Unit = {
    planStreams.values foreach { ps =>
      ps.ingressRef ! StreamIngress.CompleteAndStop
    }
  }

  override def receive: Receive = LoggingReceive { around( route ) }

  val workflowSupervision: Supervision.Decider = {
    case ex => {
      log.error( ex, "Error caught by outlier plan supervisor" )
      planStreamFailureMeter.mark( )
      Supervision.Restart
    }
  }

  implicit val system: ActorSystem = context.system
  implicit val materializer = ActorMaterializer( ActorMaterializerSettings( system ) withSupervisionStrategy workflowSupervision )

  val Payload = TypeCase[(TimeSeries, OutlierPlan)]

  val route: Receive = {
    case StreamMessage( Payload((ts, p)), subscriber ) => {
      streamIngressFor( p, subscriber ) forward StreamMessage[TimeSeries]( ts, subscriber )
    }

    case ActorSubscriberMessage.OnComplete => {
      log.info( "OutlierPlanDetectionRouter[{}]: notified that stream is completed", self.path )
      if ( planStreams.isEmpty ) {
        log.info( "OutlierPlanDetectionRouter[{}] is not waiting on sub-streams. Stopping.", self.path )
        context stop self
      } else {
        outer.detector ! ActorSubscriberMessage.OnComplete
        context become LoggingReceive { around( waitingToCompleteDetector(scheduleCompletion()) orElse manageComplete ) }
      }
    }

    case Terminated( actor ) if actor == outer.detector => {
      log.error(
        "OutlierPlanDetectionRouter[{}] notified of outlier-detector termination [{}]. erroring plan sub-streams",
        self.path,
        actor.path
      )

      val cause = new IllegalStateException( s"outlier-detector[${actor.path}] terminated" )
      planStreams foreach { case (_, stream) => stream.ingressRef ! ActorSubscriberMessage.OnError( cause ) }
    }

    case Terminated( actor ) => {
      log.warning(
        "OutlierPlanDetectionRouter[{}] notified of actor termination outside of complete: [{}]",
        self.path,
        actor.path
      )
      clearTerminatedPlanStream( actor )
    }
  }

  case object CompleteTimedOut

  def scheduleCompletion(): Cancellable = {
    implicit val ec = system.dispatcher
    system.scheduler.scheduleOnce( outer.completionTimeout, self, CompleteTimedOut )
  }

  def waitingToCompleteDetector( timeout: Cancellable ): Receive = {
    case Terminated( actor ) if actor == outer.detector => {
      log.info(
        "OutlierPlanDetectionRouter[{}] recognized outlier-detector [{}] termination during completion",
        self.path,
        actor.path
      )

      timeout.cancel()
      completePlanStreams()

      if ( planStreams.isEmpty ) {
        log.info( "OutlierPlanDetectionRouter[{}] cleaned all plan sub streams. Stopping.", self.path )
        context stop self
      } else {
        log.info( "OutlierPlanDetectionRouter[{}] waiting for sub-streams to complete...", self.path )
        context become LoggingReceive { around( waitingToCompleteSubStreams(scheduleCompletion()) orElse manageComplete ) }
      }
    }

  }

  def waitingToCompleteSubStreams( timeout: Cancellable ): Receive = {
    case Terminated( actor ) => {
      timeout.cancel()
      clearTerminatedPlanStream( actor )
      if ( planStreams.isEmpty ) {
        log.info( "OutlierPlanDetectionRouter[{}] cleaned all plan sub streams. Stopping.", self.path )
        context stop self
      } else {
        context become LoggingReceive { around( waitingToCompleteSubStreams(scheduleCompletion()) orElse manageComplete ) }
      }
    }
  }

  val manageComplete: Receive = {
    case CompleteTimedOut => {
      log.warning(
        "OutlierPlanDetectionRouter[{}] timed out before [{}] sub streams completed. Stopping.",
        self.path,
        planStreams.size
      )
      context stop self
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

    log.debug( "OutlierPlanDetectionRouter[{}]: [{} {}] ingress = {}", self.path, plan.name, subscriber, ingress )
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
    log.info( "OutlierPlanDetectionRouter[{}] making new flow for plan:subscriber: [{} : {}]", self.path, plan.name, subscriber )

    val (ingressRef, ingress) = {
      Source
      .actorPublisher[StreamMessage[TimeSeries]]( StreamIngress.props[StreamMessage[TimeSeries]] )
      .named( s"${plan.name}-detect-streamlet-inlet" )
      .toMat( Sink.asPublisher(false).named( s"${plan.name}-detect-streamlet-outlet") )( Keep.both )
      .run()
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val ingressPublisher = b.add(
        Source
        .fromPublisher( ingress )
        .map { _.payload }
        .named( s"${plan.name}-detect-streamlet" )
      )
      val batch = plan.grouping map { g => b.add( batchSeries( g ) ) }
      val detect = b.add(
        Flow[TimeSeries]
        .buffer( outer.bufferSize, OverflowStrategy.backpressure )
        .toMat( detectionSink(plan, subscriber) )( Keep.right )
      )

      if ( batch.isDefined ) {
        ingressPublisher ~> batch.get ~> detect
      } else {
        ingressPublisher ~> detect
      }

      ClosedShape
    }

    val runnable = RunnableGraph.fromGraph( graph ).named( s"plan-detection-${plan.name}-${subscriber.path}" )
    runnable.run()
    context watch ingressRef
    PlanStream( ingressRef, runnable )
  }

  def completePlanStreams(): Unit = {
    planStreams foreach { case ( Key(_, subscriber), stream ) =>
      log.info(
        "OutlierPlanDetectionRouter[{}] sending OnComplete to plan sub-stream: [{}]",
        self.path,
        stream.ingressRef
      )
      stream.ingressRef ! ActorSubscriberMessage.OnComplete
      subscriber ! ActorSubscriberMessage.OnComplete  // subscriber doesn't receive OnComplete from sub-stream
    }
  }

  def clearTerminatedPlanStream( ingress: ActorRef ): Unit = {
    planStreams find { case (_, PlanStream(planIngress, _)) =>
      log.debug(
        "OutlierPlanDetectionRouter[{}] checking ingress:[{}] == planIngress[{}] = [{}]",
        self.path,
        ingress.path,
        planIngress.path,
        (ingress == planIngress).toString
      )

      ingress == planIngress
    } match {
      case Some( (key, _) ) => {
        log.info( "OutlierPlanDetectionRouter[{}] found key [{}] to clear", self.path, key )
        planStreams -= key
      }

      case None => {
        log.warning(
          "OutlierPlanDetectionRouter[{}] waiting to complete notified of non-plan-stream terminated actor:[{}]",
          self.path,
          ingress.path
        )
      }
    }
  }

  def batchSeries(
    grouping: OutlierPlan.Grouping
  )(
    implicit tsMerging: Merging[TimeSeries]
  ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    log.debug( "OutlierPlanDetectionRouter[{}] batchSeries grouping = [{}]", self.path, grouping )

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

  def detectionSink( plan: OutlierPlan, subscriber: ActorRef )( implicit system: ActorSystem ): Sink[TimeSeriesBase, NotUsed] = {
    Flow[TimeSeriesBase]
    .filter { plan.appliesTo }
    .map { ts => OutlierDetectionMessage( ts, plan, subscriber ).disjunction }
    .collect { case \/-( m ) => m }
    .toMat {
      Sink
      .actorRef[OutlierDetectionMessage]( outer.detector, ActorSubscriberMessage.OnComplete )
      .named( "outlier-detector-inlet" )
    }( Keep.right )
  }
}
