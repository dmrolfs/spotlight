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
  def subscriber: ActorRef
  def correlationIds: Set[WorkId]
  def scope: OutlierPlan.Scope
  def message: CorrelatedSeries
}

object OutlierDetectionMessage {
  def apply( message: CorrelatedSeries, plan: OutlierPlan, subscriber: ActorRef ): Valid[OutlierDetectionMessage] = {
    checkPlan( plan, message.data ) map { p =>
      DetectOutliersInSeries( message, p, subscriber )
//      message.data match {
//        case ts: TimeSeries => DetectOutliersInSeries( message, p, subscriber )
//        case cohort: TimeSeriesCohort => DetectOutliersInCohort( message, p, subscriber )
//      }
    }
  }

//  def apply(
//    ts: TimeSeriesBase,
//    plan: OutlierPlan,
//    subscriber: ActorRef,
//    workIds: Set[WorkId] = Set.empty[WorkId]
//  ): Valid[OutlierDetectionMessage] = {
//    checkPlan(plan, ts) map { p  =>
//      ts match {
//        case data: TimeSeries => DetectOutliersInSeries( source = data, plan = p, subscriber, workIds )
//        case data: TimeSeriesCohort => DetectOutliersInCohort( source = data, plan = p, subscriber, workIds )
//      }
//    }
//  }

  def unapply( m: OutlierDetectionMessage ): Option[(OutlierPlan, Topic, m.Source)] = Some( (m.plan, m.topic, m.source) )

  def checkPlan( plan: OutlierPlan, ts: TimeSeriesBase ): Valid[OutlierPlan] = {
    if ( plan appliesTo ts ) plan.successNel else Validation.failureNel( PlanMismatchError( plan, ts ) )
  }
}


final case class DetectOutliersInSeries private[outlier](
  override val message: CorrelatedSeries,
  override val plan: OutlierPlan,
  override val subscriber: ActorRef
) extends OutlierDetectionMessage {
  override def source: TimeSeries = message.data
  override def topic: Topic = source.topic
  override def scope: Scope = message.scope getOrElse OutlierPlan.Scope( plan, source )
  override def correlationIds: Set[WorkId] = message.correlationIds
  override type Source = TimeSeries
  override def evSource: ClassTag[TimeSeries] = ClassTag( classOf[TimeSeries] )
}

//final case class DetectOutliersInCohort private[outlier](
//  message: AcceptTimeSeries,
//  override val plan: OutlierPlan,
//  override val subscriber: ActorRef,
//) extends OutlierDetectionMessage {
//  override def source: TimeSeriesCohort = message.data
//  override def topic: Topic = source.topic
//  override def correlationIds: Set[WorkId] = message.correlationIds
//  override type Source = TimeSeriesCohort
//  override def evSource: ClassTag[TimeSeriesCohort] = ClassTag( classOf[TimeSeriesCohort] )
//}


final case class DetectUsing private[outlier](
  algorithm: Symbol,
  payload: OutlierDetectionMessage,
  @deprecated("???replace with RecentHistory or remove or ???", "20161004") history: HistoricalStatistics,
  properties: Config = ConfigFactory.empty()
) extends OutlierDetectionMessage {
  override def message: CorrelatedSeries = payload.message
  override def scope: Scope = payload.scope

  override def topic: Topic = payload.topic
  override type Source = payload.Source
  override def evSource: ClassTag[Source] = payload.evSource

  def recent: RecentHistory = RecentHistory( history.lastPoints )
  override def source: Source = payload.source
  override def plan: OutlierPlan = payload.plan
  override def subscriber: ActorRef = payload.subscriber
  override def correlationIds: Set[WorkId] = message.correlationIds

  override def toString: String = s"DetectUsing(algorithm:[${algorithm}] payload:[${payload}] properties:[${properties}])"
}


final case class UnrecognizedPayload private[outlier](
  algorithm: Symbol,
  request: DetectUsing
) extends OutlierDetectionMessage {
  override def message: CorrelatedSeries = request.message
  override def scope: Scope = request.scope
  override def topic: Topic = request.topic
  override type Source = request.Source
  override def evSource: ClassTag[Source] = request.evSource
  override def source: Source = request.source
  override def plan: OutlierPlan = request.plan
  override def subscriber: ActorRef = request.subscriber
  override def correlationIds: Set[WorkId] = request.correlationIds
}
