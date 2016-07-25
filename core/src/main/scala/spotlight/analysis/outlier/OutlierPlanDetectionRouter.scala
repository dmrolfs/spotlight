package spotlight.analysis.outlier

import akka.NotUsed

import scala.concurrent.duration.FiniteDuration
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.event.LoggingReceive
import akka.stream.actor.ActorSubscriberMessage
import akka.stream.{ActorMaterializerSettings, _}
import akka.stream.scaladsl._
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Meter

import scalaz.\/-
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.{CommonActorPublisher, StreamIngress}
import peds.akka.stream.StreamMonitor._
import shapeless.TypeCase
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
      val extractOutliers = b.add(
        Flow[DetectionResult]
        .map { m => logger.info( "TEST: OUTLET RECEIVED RESULT: [{}]", m ); m }
        .map { _.outliers }
      )
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
      log.info( "OutlierPlanDetectionRouter[{}][{}]: notified that stream is completed", self.path, this.## )
      Thread.sleep( 10000 ) //todo test
      completePlanStreams()
      //      context stop self
    }

//    case (ts: TimeSeries, p: OutlierPlan) if outer.plans( p ) => streamIngressFor( p, sender() ) forwardEnvelope  ts
//    case (ts: TimeSeries, s: OutlierPlan.Scope) if outer.plans.exists{ _.name == s.plan } => {
//      log.info( "RECEIVED SCOPE:[{}] for series:[{}]", s, ts )
//      outer.plans
//      .find { _.name == s.plan }
//      .foreach { p =>
//        log.info( "PASSING ALONG to PLAN STREAM:[{}] for series:[{}]", p, ts )
//        streamIngressFor( p, sender() ) forwardEnvelope ts
//      }
//    }
//
//    case (ts, scope: OutlierPlan.Scope) => {
//      log.error( "UNKNOWN SCOPE:{} scope-id:[{}] ref-plan-ids:[{}]", scope, scope.plan, outer.plans.map{ _.id }.mkString(",") )
//    }
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
        .map { m => log.info( "TEST: detect-streamlet exitingbuffer: [{}]", m); m }
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
    PlanStream( ingressRef, runnable )
  }

  def completePlanStreams(): Unit = {
    planStreams foreach { case ( Key(_, subscriber), stream ) =>
      stream.ingressRef ! ActorSubscriberMessage.OnComplete
      subscriber ! ActorSubscriberMessage.OnComplete
    }
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

  def detectionSink( plan: OutlierPlan, subscriber: ActorRef )( implicit system: ActorSystem ): Sink[TimeSeriesBase, NotUsed] = {
    Flow[TimeSeriesBase]
    .filter { plan.appliesTo }
    .map { ts => OutlierDetectionMessage( ts, plan, subscriber ).disjunction }
    .collect { case \/-( m ) => m }
    //    .watchFlow( Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + plan.name ) )
    .map { m => log.info( "TEST: sending to outlier-detector: [{}]", m); m }
    .toMat {
      Sink
      .actorRef[OutlierDetectionMessage]( outer.detector, ActorSubscriberMessage.OnComplete )
      .named( "outlier-detector-inlet" )
    }( Keep.right )
  }


//  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy
}
