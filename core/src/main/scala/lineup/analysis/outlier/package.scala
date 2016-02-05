package lineup.analysis

import akka.actor.ActorRef
import com.typesafe.config.{ ConfigFactory, Config }
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
  }

  object OutlierDetectionMessage {
    def apply( ts: TimeSeriesBase ): OutlierDetectionMessage = ts match {
      case s: TimeSeries => DetectOutliersInSeries( s )
      case c: TimeSeriesCohort => DetectOutliersInCohort( c )
    }
  }


  case class DetectOutliersInSeries( override val source: TimeSeries ) extends OutlierDetectionMessage {
    override def topic: Topic = source.topic
    override type Source = TimeSeries
  }

  case class DetectOutliersInCohort( override val source: TimeSeriesCohort ) extends OutlierDetectionMessage {
    override def topic: Topic = source.topic
    override type Source = TimeSeriesCohort
  }


  case class DetectUsing(
    algorithm: Symbol,
    aggregator: ActorRef,
    payload: OutlierDetectionMessage,
    plan: OutlierPlan,
    history: Option[HistoricalStatistics],
    properties: Config = ConfigFactory.empty()
  ) extends OutlierDetectionMessage {
    override def topic: Topic = payload.topic
    override type Source = payload.Source
    override def source: Source = payload.source
  }


  case class UnrecognizedPayload( algorithm: Symbol, request: DetectUsing ) extends OutlierDetectionMessage {
    override def topic: Topic = request.topic
    override type Source = request.Source
    override def source: Source = request.source
  }
}
