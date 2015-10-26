package lineup.analysis

import akka.actor.ActorRef
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


  case class DetectOutliersInSeries( data: TimeSeries ) extends OutlierDetectionMessage {
    override def topic: Topic = data.topic
    override type Source = TimeSeries
    override def source: Source = data
  }

  case class DetectOutliersInCohort( data: TimeSeriesCohort ) extends OutlierDetectionMessage {
    override def topic: Topic = data.topic
    override type Source = TimeSeriesCohort
    override def source: Source = data
  }


  case class DetectUsing(
    algorithm: Symbol,
    destination: ActorRef,
    payload: OutlierDetectionMessage,
    properties: Map[String, Any] = Map()
  ) extends OutlierDetectionMessage {
    override def topic: Topic = payload.topic
    override type Source = payload.Source
    override def source: Source = payload.source
  }
}
