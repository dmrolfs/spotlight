package spotlight.analysis.outlier

import scala.concurrent.duration._
import akka.actor.{ActorLogging, ActorRef, Props}
import akka.event.LoggingReceive
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, MaxInFlightRequestStrategy, RequestStrategy}
import com.codahale.metrics.{Metric, MetricFilter}

import scalaz._
import Scalaz._
import com.typesafe.config.Config
import nl.grons.metrics.scala.{MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor
import peds.commons.log.Trace
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 5/20/16.
  */
object PlanCatalogProxy {
  def props(
    underlying: ActorRef,
    configuration: Config,
    maxInFlightCpuFactor: Double = 8.0,
    applicationDetectionBudget: Option[FiniteDuration] = None
  ): Props = Props( DefaultProxy(underlying, configuration, maxInFlightCpuFactor, applicationDetectionBudget) )

  final case class DefaultProxy private[PlanCatalogProxy](
    override val underlying: ActorRef,
    override val configuration: Config,
    override val maxInFlightCpuFactor: Double,
    override val applicationDetectionBudget: Option[FiniteDuration]
  ) extends PlanCatalogProxy with PlanCatalog.DefaultExecutionProvider
}

abstract class PlanCatalogProxy extends ActorSubscriber with EnvelopingActor with InstrumentedActor with ActorLogging {
  outer: PlanCatalog.ExecutionProvider =>

  import PlanCatalog.{ PlanRequest }

  private val trace = Trace[PlanCatalogProxy]

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
  var _workRequests: Map[WorkId, PlanRequest] = Map.empty[WorkId, PlanRequest]
  def outstandingRequests: Int = _workRequests.size
  def addWorkRequest( correlationId: WorkId, subscriber: ActorRef ): Unit = {
    _workRequests += ( correlationId -> PlanRequest(subscriber) )
  }

  def removeWorkRequests( correlationIds: Set[WorkId] ): Unit = {
    for {
      cid <- correlationIds
      knownRequest <- _workRequests.get( cid ).toSet
    } {
      catalogTimer.update( System.nanoTime() - knownRequest.startNanos, scala.concurrent.duration.NANOSECONDS )
    }

    _workRequests --= correlationIds
  }

  def knownWork: Set[WorkId] = _workRequests.keySet
  val isKnownWork: WorkId => Boolean = _workRequests.contains( _: WorkId )

  def hasWorkInProgress( workIds: Set[WorkId] ): Boolean = findOutstandingCorrelationIds( workIds ).nonEmpty

  def findOutstandingWorkRequests( correlationIds: Set[WorkId] ): Set[(WorkId, PlanRequest)] = {
    correlationIds.collect{ case cid => _workRequests.get( cid ) map { req => ( cid, req ) } }.flatten
  }

  def findOutstandingCorrelationIds( workIds: Set[WorkId] ): Set[WorkId] = trace.briefBlock( "findOutstandingCorrelationIds" ) {
    log.debug( "outstanding work: [{}]", knownWork )
    log.debug( "returning work: [{}]", workIds )
    log.debug( "known intersect: [{}]", knownWork intersect workIds )
    knownWork intersect workIds
  }

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy( outer.maxInFlight ) {
    override def inFlightInternally: Int = outstandingRequests
  }

  var isWaitingToComplete: Boolean = false
  private def stopIfFullyComplete(): Unit = {
    if ( isWaitingToComplete ) {
      log.info(
        "PlanCatalog[{}] waiting to complete on [{}] outstanding work: [{}]",
        self.path.name,
        outstandingRequests,
        knownWork.mkString( ", " )
      )

      if ( outstandingRequests == 0 ) {
        log.info(
          "PlanCatalog[{}] is closing upon work completion...will notify {} subscribers",
          self.path.name,
          PlanCatalog.subscribers.get().size
        )

        import scala.concurrent.ExecutionContext.Implicits.global
        PlanCatalog.subscribers.future().foreach { subs =>
          subs foreach { s =>
            log.debug( "propagating OnComplete to subscriber:[{}]", s.path.name )
            s ! ActorSubscriberMessage.OnComplete
          }
        }

        log.info( "PlanCatalog[{}] closing with completion", self.path.name )
        context stop self
      }
    }
  }


  override def preStart(): Unit = initializeMetrics()

  import spotlight.analysis.outlier.{ PlanCatalogProtocol => P }

  override def receive: Receive = LoggingReceive { around( stream orElse active ) }

  val active: Receive = {
    case route @ P.Route( _, subscriber, rcid ) => {
      val cid = correlationId
      for { rid <- rcid if rid != cid } { log.warning( "PlanCatalogProxy: incoming cid[{}] != dispatching cid[{}]", rid, cid ) }

      addWorkRequest( cid, subscriber )
      log.debug( "PlanCatalogProxy:ACTIVE: forwarding StreamMessage to PlanCatalog" )
      underlying !+ route.copy( correlationId = Some(cid) )
    }

    case P.UnknownRoute( ts, subscriber, cid ) => {
      log.warning( "PlanCatalogProxy[{}]: unknown route for timeseries. Dropping series for:[{}]", cid, ts.topic )
      cid foreach { id => removeWorkRequests( Set( id ) ) }
      stopIfFullyComplete()
    }

    case result: DetectionResult if !hasWorkInProgress( result.correlationIds ) => {
      log.error(
        "PlanCatalogProxy:ACTIVE[{}]: stashing received result for UNKNOWN workId:[{}] all-ids:[{}]: [{}]",
        self.path.name,
        workId,
        knownWork.mkString(", "),
        result
      )

      stopIfFullyComplete()
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
      stopIfFullyComplete()
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
      stopIfFullyComplete()
    }

    case OnError( ex ) => {
      log.error( ex, "PlanCatalogProxy:STREAM closing on errored stream" )
      context stop self
    }
  }

}
