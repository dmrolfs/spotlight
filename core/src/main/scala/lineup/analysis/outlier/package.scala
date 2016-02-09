package lineup.analysis

import akka.actor.ActorRef
import scalaz._, Scalaz._
import com.typesafe.config.{ ConfigFactory, Config }
import peds.commons.Valid
import lineup.model.outlier.OutlierPlan
import lineup.model.timeseries.{ Topic, TimeSeriesBase, TimeSeriesCohort, TimeSeries }


/**
 * Created by rolfsd on 10/4/15.
 */
package object outlier {
  sealed trait OutlierDetectionMessage {
    def topic: Topic
    type Source <: TimeSeriesBase
    def source: Source
    def plan: OutlierPlan
  }

  object OutlierDetectionMessage {
    def apply( ts: TimeSeriesBase, plan: OutlierPlan ): Valid[OutlierDetectionMessage] = {
      checkPlan(plan, ts) map { p =>
        ts match {
          case s: TimeSeries => DetectOutliersInSeries( s, p )
          case c: TimeSeriesCohort => DetectOutliersInCohort( c, p )
        }
      }
    }
  }

  def checkPlan( plan: OutlierPlan, ts: TimeSeriesBase ): Valid[OutlierPlan] = {
    if ( plan appliesTo ts ) plan.successNel else Validation.failureNel( PlanMismatchError( plan, ts ) )
  }


  final case class PlanMismatchError private[outlier]( plan: OutlierPlan, timeseries: TimeSeriesBase )
  extends IllegalStateException( s"plan [${plan.name}:${plan.id}] improperly associated with time series [${timeseries.topic}]" )


  final case class DetectOutliersInSeries private[outlier](
    override val source: TimeSeries,
    override val plan: OutlierPlan
  ) extends OutlierDetectionMessage {
    override def topic: Topic = source.topic
    override type Source = TimeSeries
  }

  final case class DetectOutliersInCohort private[outlier](
    override val source: TimeSeriesCohort,
    override val plan: OutlierPlan
  ) extends OutlierDetectionMessage {
    override def topic: Topic = source.topic
    override type Source = TimeSeriesCohort
  }


  final case class DetectUsing private[outlier](
    algorithm: Symbol,
    aggregator: ActorRef,
    payload: OutlierDetectionMessage,
    history: Option[HistoricalStatistics],
    properties: Config = ConfigFactory.empty()
  ) extends OutlierDetectionMessage {
    override def topic: Topic = payload.topic
    override type Source = payload.Source
    override def source: Source = payload.source
    override val plan: OutlierPlan = payload.plan
  }


  final case class UnrecognizedPayload private[outlier](
    algorithm: Symbol,
    request: DetectUsing
  ) extends OutlierDetectionMessage {
    override def topic: Topic = request.topic
    override type Source = request.Source
    override def source: Source = request.source
    override def plan: OutlierPlan = request.plan
  }
}
