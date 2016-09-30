package spotlight.analysis.outlier

import scala.reflect.ClassTag
import akka.actor.ActorRef
import com.typesafe.config.{Config, ConfigFactory}
import scalaz.{Scalaz, Validation}
import Scalaz._
import peds.commons.Valid
import peds.commons.identifier.TaggedID
import demesne.CommandLike
import peds.akka.envelope.WorkId
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase, TimeSeriesCohort, Topic}


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
  def workIds: Set[WorkId]
  lazy val scope: OutlierPlan.Scope = OutlierPlan.Scope( plan, topic )
}

object OutlierDetectionMessage {
  def apply(
    ts: TimeSeriesBase,
    plan: OutlierPlan,
    subscriber: ActorRef,
    workIds: Set[WorkId] = Set.empty[WorkId]
  ): Valid[OutlierDetectionMessage] = {
    checkPlan(plan, ts) map { p  =>
      ts match {
        case data: TimeSeries => DetectOutliersInSeries( source = data, plan = p, subscriber, workIds )
        case data: TimeSeriesCohort => DetectOutliersInCohort( source = data, plan = p, subscriber, workIds )
      }
    }
  }

  def unapply( m: OutlierDetectionMessage ): Option[(OutlierPlan, Topic, m.Source)] = Some( (m.plan, m.topic, m.source) )

  def checkPlan( plan: OutlierPlan, ts: TimeSeriesBase ): Valid[OutlierPlan] = {
    if ( plan appliesTo ts ) plan.successNel else Validation.failureNel( PlanMismatchError( plan, ts ) )
  }
}


final case class DetectOutliersInSeries private[outlier](
  override val source: TimeSeries,
  override val plan: OutlierPlan,
  override val subscriber: ActorRef,
  override val workIds: Set[WorkId]
) extends OutlierDetectionMessage {
  override def topic: Topic = source.topic
  override type Source = TimeSeries
  override def evSource: ClassTag[TimeSeries] = ClassTag( classOf[TimeSeries] )
}

final case class DetectOutliersInCohort private[outlier](
  override val source: TimeSeriesCohort,
  override val plan: OutlierPlan,
  override val subscriber: ActorRef,
  override val workIds: Set[WorkId]
) extends OutlierDetectionMessage {
  override def topic: Topic = source.topic
  override type Source = TimeSeriesCohort
  override def evSource: ClassTag[TimeSeriesCohort] = ClassTag( classOf[TimeSeriesCohort] )
}


final case class DetectUsing private[outlier](
  algorithm: Symbol,
//  aggregator: ActorRef,
  payload: OutlierDetectionMessage,
  history: HistoricalStatistics,
  properties: Config = ConfigFactory.empty()
) extends OutlierDetectionMessage {
  override def topic: Topic = payload.topic
  override type Source = payload.Source
  override def evSource: ClassTag[Source] = payload.evSource

  override def source: Source = payload.source
  override def plan: OutlierPlan = payload.plan
  override def subscriber: ActorRef = payload.subscriber
  override def workIds: Set[WorkId] = payload.workIds
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
  override def subscriber: ActorRef = request.subscriber
  override def workIds: Set[WorkId] = request.workIds
}
