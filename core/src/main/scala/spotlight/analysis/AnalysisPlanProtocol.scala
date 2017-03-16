package spotlight.analysis

import akka.NotUsed
import akka.actor.{ ActorRef, ActorSystem, NotInfluenceReceiveTimeout }
import akka.stream.Supervision.Decider
import akka.stream.{ ActorAttributes, Materializer, Supervision }
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import com.codahale.metrics.Meter
import com.persist.logging._
import com.typesafe.config.Config
import demesne.AggregateRootModule.{ Command, Event }
import demesne.module.entity.EntityProtocol
import omnibus.akka.envelope.{ Envelope, WorkId }
import spotlight.analysis.OutlierDetection.{ DetectionResult, DetectionTimedOut }
import spotlight.model.outlier.AnalysisPlan.Scope
import spotlight.model.outlier._
import spotlight.model.timeseries.TimeSeries
import spotlight.model.timeseries.TimeSeriesBase.Merging

import scala.concurrent.TimeoutException

/** Created by rolfsd on 3/15/17.
  */
object AnalysisPlanProtocol extends EntityProtocol[AnalysisPlanState#ID] {
  //todo add info change commands
  //todo reify algorithm
  //      case class AddAlgorithm( override val targetId: AnalysisPlan#TID, algorithm: Symbol ) extends Command with AnalysisPlanMessage
  case class ApplyTo( override val targetId: ApplyTo#TID, appliesTo: AnalysisPlan.AppliesTo ) extends Command

  case class UseAlgorithms( override val targetId: UseAlgorithms#TID, algorithms: Map[String, Config] ) extends Command

  case class ResolveVia(
    override val targetId: ResolveVia#TID,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers
  ) extends Command

  override def tags: Set[String] = Set( AnalysisPlanModule.module.rootType.name )

  case class ScopeChanged( override val sourceId: ScopeChanged#TID, appliesTo: AnalysisPlan.AppliesTo ) extends TaggedEvent

  case class AlgorithmsChanged(
    override val sourceId: AlgorithmsChanged#TID,
    algorithms: Map[String, Config],
    added: Set[String],
    dropped: Set[String]
  ) extends TaggedEvent

  case class AnalysisResolutionChanged(
    override val sourceId: AnalysisResolutionChanged#TID,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers
  ) extends TaggedEvent

  case class GetPlan( override val targetId: GetPlan#TID ) extends Command
  case class PlanInfo( override val sourceId: PlanInfo#TID, info: AnalysisPlan ) extends Event {
    def toSummary: AnalysisPlan.Summary = {
      AnalysisPlan.Summary( id = sourceId, name = info.name, slug = info.slug, appliesTo = Option( info.appliesTo ) )
    }
  }

  case class RouteDetection( override val targetId: RouteDetection#TID, payload: OutlierDetectionMessage ) extends Message

  case class MakeFlow( override val targetId: MakeFlow#TID ) extends Message with NotInfluenceReceiveTimeout

  case class AnalysisFlow(
    val sourceId: AnalysisPlan#TID,
    factory: FlowFactory[TimeSeries, Outliers]
  ) extends ProtocolMessage with NotInfluenceReceiveTimeout

  @deprecated( "remove with other dynamic routing flow", "2016" )
  case class AcceptTimeSeries(
      override val targetId: AcceptTimeSeries#TID,
      override val correlationIds: Set[WorkId],
      override val data: TimeSeries,
      override val scope: Option[AnalysisPlan.Scope] = None
  ) extends Command with CorrelatedSeries {
    override def withData( newData: TimeSeries ): CorrelatedData[TimeSeries] = this.copy( data = newData )
    override def withCorrelationIds( newIds: Set[WorkId] ): CorrelatedData[TimeSeries] = this.copy( correlationIds = newIds )
    override def withScope( newScope: Option[Scope] ): CorrelatedData[TimeSeries] = this.copy( scope = newScope )
  }
}
