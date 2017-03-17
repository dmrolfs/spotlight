package spotlight.analysis

import akka.actor.NotInfluenceReceiveTimeout
import omnibus.akka.envelope._
import spotlight.model.outlier.{ AnalysisPlan, Outliers }
import spotlight.model.timeseries.{ TimeSeries, Topic }

/** Created by rolfsd on 3/14/17.
  */
object PlanCatalogProtocol {
  trait CatalogMessage

  case object WaitForStart extends CatalogMessage
  case object Started extends CatalogMessage

  sealed trait PlanDirective extends CatalogMessage
  case class AddPlan( plan: AnalysisPlan ) extends PlanDirective
  case class DisablePlan( pid: AnalysisPlan#TID ) extends PlanDirective
  case class EnablePlan( pid: AnalysisPlan#TID ) extends PlanDirective
  case class RenamePlan( pid: AnalysisPlan#TID, newName: String ) extends PlanDirective

  case class GetPlansForTopic( topic: Topic ) extends CatalogMessage
  case class CatalogedPlans( plans: Set[AnalysisPlan.Summary], request: GetPlansForTopic ) extends CatalogMessage

  case class MakeFlow() extends CatalogMessage with NotInfluenceReceiveTimeout

  case class SpotlightFlow(
    plans: Set[AnalysisPlan.Summary],
    factory: FlowFactory[TimeSeries, Outliers]
  ) extends CatalogMessage with NotInfluenceReceiveTimeout

  @deprecated( "remove with other dynamic routing flow", "2016" )
  case class Route( timeSeries: TimeSeries, correlationId: Option[WorkId] = None ) extends CatalogMessage

  @deprecated( "remove with other dynamic routing flow", "2016" )
  case class UnknownRoute( timeSeries: TimeSeries, correlationId: Option[WorkId] ) extends CatalogMessage

  @deprecated( "remove with other dynamic routing flow", "2016" )
  object UnknownRoute {
    def apply( route: Route ): UnknownRoute = UnknownRoute( route.timeSeries, route.correlationId )
  }
}
