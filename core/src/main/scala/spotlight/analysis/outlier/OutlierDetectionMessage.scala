package spotlight.analysis.outlier

import scala.reflect.ClassTag
import akka.actor.ActorRef
import scalaz.{Scalaz, Validation}
import Scalaz._
import com.typesafe.config.{Config, ConfigFactory}
import peds.commons.Valid
import peds.commons.identifier.TaggedID
import demesne.CommandLike
import peds.akka.envelope.WorkId
import spotlight.model.outlier.{CorrelatedSeries, OutlierPlan}
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase, Topic}
import spotlight.model.outlier.OutlierPlan.Scope


/**
  * Created by rolfsd on 9/21/16.
  */
sealed trait OutlierDetectionMessage extends CommandLike {
  override type ID = OutlierPlan.Scope
  //todo: detect message is routed to many algorithms, each with own tag. This targetId is set to a dummy tag knowing that
  // aggregate routing uses id portion only and ignores tag.
  override def targetId: TID = TaggedID( 'detect, OutlierPlan.Scope(plan, topic) )
  def topic: Topic
  type Source <: TimeSeriesBase
  def evSource: ClassTag[Source]
  def source: Source
  def plan: OutlierPlan
  def subscriber: Option[ActorRef] = None
  def correlationIds: Set[WorkId]
  lazy val scope: OutlierPlan.Scope = OutlierPlan.Scope( plan, topic )
//  def message: CorrelatedSeries
}

object OutlierDetectionMessage {
  def apply(
    ts: TimeSeries,
    plan: OutlierPlan,
    subscriber: Option[ActorRef] = None,
    correlationIds: Set[WorkId] = Set.empty[WorkId]
  ): Valid[OutlierDetectionMessage] = {
    checkPlan( plan, ts ) map { p => DetectOutliersInSeries( ts, p, subscriber, correlationIds ) }
  }

  def unapply( m: OutlierDetectionMessage ): Option[(OutlierPlan, Topic, m.Source)] = Some( (m.plan, m.topic, m.source) )

  def checkPlan( plan: OutlierPlan, ts: TimeSeriesBase ): Valid[OutlierPlan] = {
    if ( plan appliesTo ts ) plan.successNel else Validation.failureNel( PlanMismatchError( plan, ts ) )
  }
}


final case class DetectOutliersInSeries private[outlier](
  override val source: TimeSeries,
  override val plan: OutlierPlan,
  override val subscriber: Option[ActorRef],
  override val correlationIds: Set[WorkId]
) extends OutlierDetectionMessage {
  override def topic: Topic = source.topic
  override type Source = TimeSeries
  override def evSource: ClassTag[TimeSeries] = ClassTag( classOf[TimeSeries] )
}


final case class DetectUsing private[outlier](
  algorithm: Symbol,
  payload: OutlierDetectionMessage,
  @deprecated("???replace with RecentHistory or remove or ???", "20161004") history: HistoricalStatistics,
  properties: Config = ConfigFactory.empty()
) extends OutlierDetectionMessage {
  override def topic: Topic = payload.topic
  override type Source = payload.Source
  override def evSource: ClassTag[Source] = payload.evSource

  def recent: RecentHistory = RecentHistory( history.lastPoints )
  override def source: Source = payload.source
  override def plan: OutlierPlan = payload.plan
  override def subscriber: Option[ActorRef] = payload.subscriber
  override def correlationIds: Set[WorkId] = payload.correlationIds

  override def toString: String = s"DetectUsing(algorithm:[${algorithm}] payload:[${payload}] properties:[${properties}])"
}


final case class UnrecognizedPayload private[outlier](
  algorithm: Symbol,
  request: DetectUsing
) extends OutlierDetectionMessage {
  override def topic: Topic = request.topic
  override type Source = request.Source
  override def evSource: ClassTag[Source] = request.evSource
  override def source: Source = request.source
  override def plan: OutlierPlan = request.plan
  override def subscriber: Option[ActorRef] = request.subscriber
  override def correlationIds: Set[WorkId] = request.correlationIds
}
