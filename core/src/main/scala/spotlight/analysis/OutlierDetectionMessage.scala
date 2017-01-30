package spotlight.analysis

import scala.reflect.ClassTag
import akka.actor.ActorRef
import scalaz.{Scalaz, Validation}
import Scalaz._
import com.typesafe.config.{Config, ConfigFactory}
import peds.commons.Valid
import peds.commons.identifier.TaggedID
import demesne.CommandLike
import peds.akka.envelope.WorkId
import spotlight.model.outlier.{CorrelatedSeries, AnalysisPlan}
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase, Topic}
import spotlight.model.outlier.AnalysisPlan.Scope


/**
  * Created by rolfsd on 9/21/16.
  */
sealed trait OutlierDetectionMessage extends CommandLike {
  override type ID = AnalysisPlanModule.module.ID
  //todo: detect message is routed to many algorithms, each with own tag. This targetId is set to a dummy tag knowing that
  // aggregate routing uses id portion only and ignores tag.
//  override def targetId: TID = plan.id
  def topic: Topic
  type Source <: TimeSeriesBase
  def evSource: ClassTag[Source]
  def source: Source
  def plan: AnalysisPlan
  def subscriber: Option[ActorRef] = None
  def correlationIds: Set[WorkId]
  lazy val scope: AnalysisPlan.Scope = AnalysisPlan.Scope( plan, topic )
//  def message: CorrelatedSeries
}

object OutlierDetectionMessage {
  def apply(
    ts: TimeSeries,
    plan: AnalysisPlan,
    subscriber: Option[ActorRef] = None,
    correlationIds: Set[WorkId] = Set.empty[WorkId]
  ): Valid[OutlierDetectionMessage] = {
    checkPlan( plan, ts ) map { p => DetectOutliersInSeries( ts, p, subscriber, correlationIds ) }
  }

  def unapply( m: OutlierDetectionMessage ): Option[(AnalysisPlan, Topic, m.Source)] = Some( (m.plan, m.topic, m.source) )

  def checkPlan( plan: AnalysisPlan, ts: TimeSeriesBase ): Valid[AnalysisPlan] = {
    if ( plan appliesTo ts ) plan.successNel else Validation.failureNel( PlanMismatchError( plan, ts ) )
  }
}


final case class DetectOutliersInSeries private[analysis](
  override val source: TimeSeries,
  override val plan: AnalysisPlan,
  override val subscriber: Option[ActorRef],
  override val correlationIds: Set[WorkId]
) extends OutlierDetectionMessage {
  override def targetId: TID = plan.id
  override def topic: Topic = source.topic
  override type Source = TimeSeries
  override def evSource: ClassTag[TimeSeries] = ClassTag( classOf[TimeSeries] )
}


final case class DetectUsing private[analysis](
  override val targetId: DetectUsing#TID,
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
  override def plan: AnalysisPlan = payload.plan
  override def subscriber: Option[ActorRef] = payload.subscriber
  override def correlationIds: Set[WorkId] = payload.correlationIds

  override def toString: String = s"DetectUsing(algorithm:[${algorithm}] payload:[${payload}] properties:[${properties}])"
}


final case class UnrecognizedPayload private[analysis](
  algorithm: Symbol,
  request: DetectUsing
) extends OutlierDetectionMessage {
  override def targetId: TID = plan.id
  override def topic: Topic = request.topic
  override type Source = request.Source
  override def evSource: ClassTag[Source] = request.evSource
  override def source: Source = request.source
  override def plan: AnalysisPlan = request.plan
  override def subscriber: Option[ActorRef] = request.subscriber
  override def correlationIds: Set[WorkId] = request.correlationIds
}
