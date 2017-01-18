package spotlight.analysis

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.Done
import akka.actor.{ActorLogging, ActorRef, Props}
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, MaxInFlightRequestStrategy, RequestStrategy}
import com.codahale.metrics.{Metric, MetricFilter}

import scalaz._
import Scalaz._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import spotlight.analysis.OutlierDetection.DetectionResult
import spotlight.analysis.PlanCatalogProtocol.{Started, WaitForStart}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 5/20/16.
  */
object PlanCatalogProxy extends Instrumented with LazyLogging {
  def props(
    underlying: ActorRef,
    configuration: Config,
    finishSubscriberOnComplete: Boolean = true,
    maxInFlightCpuFactor: Double = 8.0,
    applicationDetectionBudget: Option[FiniteDuration] = None
  ): Props = {
    Props(
      DefaultProxy(
        underlying,
        configuration,
        maxInFlightCpuFactor,
        applicationDetectionBudget,
        finishSubscriberOnComplete
      )
    )
  }

  final case class DefaultProxy private[PlanCatalogProxy](
    override val underlying: ActorRef,
    override val configuration: Config,
    override val maxInFlightCpuFactor: Double,
    override val applicationDetectionBudget: Option[FiniteDuration],
    finishSubscriberOnComplete: Boolean = true
  ) extends PlanCatalogProxy( finishSubscriberOnComplete ) with PlanCatalog.DefaultExecutionProvider


  val subscribers: Agent[Set[ActorRef]] = Agent( Set.empty[ActorRef] )( scala.concurrent.ExecutionContext.global )
  def clearSubscriber( subscriber: ActorRef )( implicit ec: ExecutionContext ): Future[Done] = {
    for {
      subs <- subscribers.future() if subs contains subscriber
      _ = subscriber ! ActorSubscriberMessage.OnComplete
      _ <- subscribers alter { oldSubs =>
        logger.info( "PlanCatalogProxy: subscriber cleared: [{}]", subscriber.path.name )
        oldSubs - subscriber
      }
    } yield Done
  }


  override lazy val metricBaseName: MetricName = MetricName( classOf[PlanCatalog] )
  metrics.gauge( "subscribers" ){ subscribers.get().size }

}

abstract class PlanCatalogProxy( finishSubscriberOnComplete: Boolean = true )
extends ActorSubscriber with EnvelopingActor with InstrumentedActor with ActorLogging {
  outer: PlanCatalog.ExecutionProvider =>

  import PlanCatalog.{ PlanRequest }

  override lazy val metricBaseName: MetricName = MetricName( classOf[PlanCatalogProxy] )
  val catalogTimer: Timer = metrics timer "catalog"

  val outstandingMetricName: String = "outstanding"

  def initializeMetrics(): Unit = {
    stripLingeringMetrics()
    metrics.gauge( outstandingMetricName ) { outstandingRequests }
  }

  def stripLingeringMetrics(): Unit = {
    metrics.registry.removeMatching(
      new MetricFilter {
        override def matches( name: String, metric: Metric ): Boolean = {
          name.contains( classOf[PlanCatalog].getName ) && name.contains( outstandingMetricName )
        }
      }
    )
  }

  def underlying: ActorRef


  //todo: associate timestamp with workId in order to reap tombstones
  var _subscribersSeen: Set[ActorRef] = Set.empty[ActorRef]
  var _workRequests: Map[WorkId, PlanRequest] = Map.empty[WorkId, PlanRequest]
  def outstandingRequests: Int = _workRequests.size
  def addWorkRequest( correlationId: WorkId, subscriber: ActorRef ): Unit = {
    _subscribersSeen += subscriber
    _workRequests += ( correlationId -> PlanRequest(subscriber) )
  }

  def removeWorkRequests( correlationIds: Set[WorkId] ): Unit = {
    for {
      cid <- correlationIds
      knownRequest <- _workRequests.get( cid ).toSet
    } {
      catalogTimer.update( System.nanoTime() - knownRequest.startNanos, scala.concurrent.duration.NANOSECONDS )
      _subscribersSeen -= knownRequest.subscriber
    }

    _workRequests --= correlationIds
  }

  def knownWork: Set[WorkId] = _workRequests.keySet
  val isKnownWork: WorkId => Boolean = _workRequests.contains( _: WorkId )

  def hasWorkInProgress( workIds: Set[WorkId] ): Boolean = findOutstandingCorrelationIds( workIds ).nonEmpty

  def findOutstandingWorkRequests( correlationIds: Set[WorkId] ): Set[(WorkId, PlanRequest)] = {
    correlationIds.collect{ case cid => _workRequests.get( cid ) map { req => ( cid, req ) } }.flatten
  }

  def findOutstandingCorrelationIds( workIds: Set[WorkId] ): Set[WorkId] = {
    log.debug( "outstanding work: [{}]", knownWork )
    log.debug( "returning work: [{}]", workIds )
    log.debug( "known intersect: [{}]", knownWork intersect workIds )
    knownWork intersect workIds
  }

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy( outer.maxInFlight ) {
    override def inFlightInternally: Int = outstandingRequests
  }

  var isWaitingToComplete: Boolean = false
  private def stopIfFullyComplete()( implicit ec: ExecutionContext ): Unit = {
    if ( isWaitingToComplete ) {
      log.info(
        "PlanCatalogProxy[{}] waiting to complete on [{}] outstanding work: [{}]",
        self.path.name,
        outstandingRequests,
        knownWork.mkString( ", " )
      )

      if ( outstandingRequests == 0 ) {
        log.info(
          "PlanCatalogProxy[{}] is closing upon work completion...will notify {} subscribers",
          self.path.name,
          _subscribersSeen.size
        )

        if ( finishSubscriberOnComplete ) _subscribersSeen foreach { PlanCatalogProxy.clearSubscriber }

        log.info( "PlanCatalogProxy[{}] closing with completion", self.path.name )
        context stop self
      }
    }
  }


  override def preStart(): Unit = {
    initializeMetrics()
    underlying ! WaitForStart
  }

  import spotlight.analysis.outlier.{ PlanCatalogProtocol => P }

  override def receive: Receive = LoggingReceive { around( quiscent ) }

  val quiscent: Receive = {
    case Started => {
      log.info( "starting PlanCatalogProxy stream..." )
      context.become( LoggingReceive{ around( stream orElse active ) } )
    }
  }

  val active: Receive = {
    case route @ P.Route( _, rcid ) => {
      val cid = correlationId
      for { rid <- rcid if rid != cid } { log.warning( "PlanCatalogProxy: incoming cid[{}] != dispatching cid[{}]", rid, cid ) }

      addWorkRequest( cid, sender() )
      log.debug( "PlanCatalogProxy:ACTIVE: forwarding StreamMessage to PlanCatalog" )
      underlying !+ route.copy( correlationId = Some(cid) )
    }

    case P.UnknownRoute( ts, cid ) => {
      log.warning( "PlanCatalogProxy[{}]: unknown route for timeseries. Dropping series for:[{}]", cid, ts.topic )
      cid foreach { id => removeWorkRequests( Set( id ) ) }
      stopIfFullyComplete()( context.dispatcher )
    }

    case result: DetectionResult if !hasWorkInProgress( result.correlationIds ) => {
      log.error(
        "PlanCatalogProxy:ACTIVE[{}]: stashing received result for UNKNOWN workId:[{}] all-ids:[{}]: [{}]",
        self.path.name,
        workId,
        knownWork.mkString(", "),
        result
      )

      stopIfFullyComplete()( context.dispatcher )
    }

    case result @ DetectionResult( outliers, correlationIds ) => {
      log.debug(
        "PlanCatalogProxy:ACTIVE[{}]: received outlier result[{}] for workId:[{}] subscriber:[{}]",
        self.path,
        outliers.hasAnomalies,
        workId,
        _workRequests.get( workId ).map{ _.subscriber.path.name }
      )
      log.debug(
        "PlanCatalogProxy:ACTIVE: received (workId:[{}] -> subscriber:[{}])  all:[{}]",
        workId, _workRequests.get(workId), _workRequests.mkString(", ")
      )

      val outstanding = clearCompletedWork( correlationIds, outliers.topic, outliers.algorithms )
      log.debug(
        "PlanCatalogProxy:ACTIVE sending result to {} subscriber{}",
        outstanding.size,
        if ( outstanding.size == 1 ) "" else "s"
      )
      outstanding foreach { case (_, r) => r.subscriber ! result }
      stopIfFullyComplete()( context.dispatcher )
    }
  }

  def clearCompletedWork( correlationIds: Set[WorkId], topic: Topic, algorithms: Set[Symbol] ): Set[(WorkId, PlanRequest)] = {
    val (known, unknown) = correlationIds partition isKnownWork
    if ( unknown.nonEmpty ) {
      log.warning(
        "PlanCatalogProxy:ACTIVE received topic:[{}] algorithms:[{}] results for unrecognized workIds:[{}]",
        topic,
        algorithms.map{ _.name }.mkString( ", " ),
        unknown.mkString(", ")
      )
    }

    val outstanding = findOutstandingWorkRequests( known )
    removeWorkRequests( correlationIds )
    outstanding
  }

  val stream: Receive = {
    case OnNext( message ) if active isDefinedAt message => {
      log.debug( "PlanCatalogProxy:STREAM: evaluating OnNext( {} )", message )
      active( message )
    }

    case OnComplete => {
      log.info( "PlanCatalogProxy:STREAM[{}] closing on completed stream", self.path.name )
      isWaitingToComplete = true
      stopIfFullyComplete()( context.dispatcher )
    }

    case OnError( ex ) => {
      log.error( ex, "PlanCatalogProxy:STREAM closing on errored stream" )
      context stop self
    }
  }

}
