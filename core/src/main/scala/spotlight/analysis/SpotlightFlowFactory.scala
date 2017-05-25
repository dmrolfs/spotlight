package spotlight.analysis

import scala.concurrent.Future
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge }
import shapeless.the
import com.persist.logging._
import demesne.{ BoundedContext, DomainModel }
import omnibus.akka.envelope._
import omnibus.akka.envelope.pattern.ask
import spotlight.{ BC, EC, M, T }
import spotlight.analysis.PlanCatalog.WatchPoints
import spotlight.analysis.{ AnalysisPlanProtocol ⇒ AP }
import spotlight.model.outlier.{ AnalysisPlan, Outliers }
import spotlight.model.timeseries._

/** Created by rolfsd on 3/16/17.
  */
class SpotlightFlowFactory( plans: Set[AnalysisPlan.Summary] ) extends FlowFactory[TimeSeries, Outliers] with ClassLogging {
  override def makeFlow[_: BC: T: M]( parallelism: Int ): Future[DetectFlow] = {
    implicit val system = the[BoundedContext].system
    implicit val ec = system.dispatcher

    log.debug( Map( "@msg" → "making detection flow with plans", "plans" → plans.toSeq.map { p ⇒ ( p.name, p.id ) } ) )

    for {
      model ← the[BoundedContext].futureModel
      planFlows ← collectPlanFlows( parallelism, model, plans )
      f ← detectFrom( planFlows ) map { _.named( "Spotlight" ) }
    } yield {
      log.info( Map( "@msg" → "Made catalog flow", "flow" → f.toString ) )
      f
    }
  }

  private def collectPlanFlows[_: BC: T: M](
    parallelism: Int,
    model: DomainModel,
    plans: Set[AnalysisPlan.Summary]
  ): Future[Set[DetectFlow]] = {
    implicit val ec = the[BoundedContext].system.dispatcher

    def makeFlow( plan: AnalysisPlan.Summary, af: AP.AnalysisFlow ): Future[DetectFlow] = {
      log.debug(
        Map(
          "@msg" → "PlanCatalog: created analysis flow for",
          "plan" → Map( "id" → af.sourceId.toString, "name" → plan.name ),
          "factory" → af.factory.toString
        )
      )

      af.factory makeFlow parallelism
    }

    Future sequence {
      plans map { p ⇒
        val ref = model( AnalysisPlanModule.module.rootType, p.id )
        //        NEED TO MAKE PLAN -FLOW - SEED IN CATALOG AND INCL IN SPOTLIGHT - SEED
        ( ref ?+ AP.MakeFlow( p.id ) )
          .flatMap {
            case af: AP.AnalysisFlow ⇒ makeFlow( p, af )
            case Envelope( af: AP.AnalysisFlow, _ ) ⇒ makeFlow( p, af )
            case m ⇒ Future.failed( new MatchError( m ) )
          }
      }
    }
  }

  private def detectFrom[_: EC]( planFlows: Set[DetectFlow] ): Future[DetectFlow] = {
    val nrFlows = planFlows.size
    log.debug( Map( "@msg" → "making PlanCatalog graph for plans", "nr-plans" → nrFlows ) )

    val graph = Future {
      GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._

        if ( planFlows.isEmpty ) throw PlanCatalog.NoRegisteredPlansError
        else {
          //todo make use of dynamic Merge/Broadcast Hub & retain KillSwitches
          val broadcast = b.add( Broadcast[TimeSeries]( nrFlows ) )
          val merge = b.add( Merge[Outliers]( nrFlows ) )

          planFlows.zipWithIndex foreach {
            case ( pf, i ) ⇒
              log.debug( Map( "@msg" → "adding to catalog flow, order undefined analysis flow", "index" → i ) )
              val flow = b.add( pf )
              broadcast.out( i ) ~> flow ~> merge.in( i )
          }

          FlowShape( broadcast.in, merge.out )
        }
      }
    }

    graph map { g ⇒
      import omnibus.akka.stream.StreamMonitor._
      Flow.fromGraph( g ).named( "SpotlightFlow" ).watchFlow( WatchPoints.Catalog )
    }
  }

}
