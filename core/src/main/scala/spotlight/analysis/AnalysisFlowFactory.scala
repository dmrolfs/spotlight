package spotlight.analysis

import scala.concurrent.{ ExecutionContext, Future, TimeoutException }
import akka.NotUsed
import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.Supervision.Decider
import akka.stream.{ ActorAttributes, Supervision }
import akka.stream.scaladsl.Flow
import akka.util.Timeout
//import cats.syntax.traverse._
//import cats.instances.list._
import shapeless.the
import com.persist.logging._
import demesne.BoundedContext
import nl.grons.metrics.scala.{ Meter, MetricName }
import omnibus.akka.envelope.Envelope
import omnibus.akka.metrics.Instrumented
import spotlight.{ BC, T, M }
import spotlight.analysis.AnalysisPlanProtocol.RouteDetection
import spotlight.analysis.OutlierDetection.{ DetectionResult, DetectionTimedOut }
import spotlight.model.outlier.{ AnalysisPlan, Outliers }
import spotlight.model.timeseries._
import spotlight.model.timeseries.TimeSeriesBase.Merging

/** Created by rolfsd on 3/15/17.
  */
class AnalysisFlowFactory( plan: AnalysisPlan ) extends FlowFactory[TimeSeries, Outliers] with ClassLogging {
  import AnalysisFlowFactory.Metrics

  override def makeFlow[_: BC: T: M]( parallelism: Int ): Future[DetectFlow] = {
    implicit val system = the[BoundedContext].system
    implicit val ec = system.dispatcher

    val entry = Flow[TimeSeries].filter { plan.appliesTo }
    val withGrouping = plan.grouping map { g ⇒ entry.via( batchSeries( g ) ) } getOrElse entry

    detectorProxy() map { detector ⇒
      val name = s"AnalysisPlan:${plan.name}@${plan.id.id}"
      val flow = withGrouping.via( detectionFlow( parallelism, detector ) ).named( name )
      log.info( Map( "@msg" → "Made analysis plan flow", "flow" → flow.toString ) )
      flow
    }
  }

  private[this] def detectorProxy()( implicit bc: BoundedContext, ec: ExecutionContext ): Future[ActorRef] = {
    bc.futureModel map { _( AnalysisPlanModule.module.rootType, plan.id ) }
  }

  def batchSeries(
    grouping: AnalysisPlan.Grouping
  )(
    implicit
    tsMerging: Merging[TimeSeries]
  ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    Flow[TimeSeries]
      .groupedWithin( n = grouping.limit, d = grouping.window )
      .map {
        _
          .groupBy { _.topic }
          .map {
            case ( _, tss ) ⇒
              tss.tail.foldLeft( tss.head ) { case ( acc, ts ) ⇒ tsMerging.merge( acc, ts ) valueOr { exs ⇒ throw exs.head } }
          }
      }
      .mapConcat { identity }
  }

  def detectionFlow( parallelism: Int, detector: ActorRef )( implicit system: ActorSystem, timeout: Timeout ): DetectFlow = {
    import omnibus.akka.envelope.pattern.ask

    implicit val ec: scala.concurrent.ExecutionContext = system.dispatcher

    if ( detector == null && detector != system.deadLetters ) {
      val ex = new IllegalStateException( s"analysis plan [${plan.name}] flow invalid detector reference:[${detector}]" )

      log.error(
        Map(
          "@msg" → "analysis plan [${p.name}] flow created missing valid detector reference:[${detector}]",
          "plan" → plan.name,
          "detector" → detector.path
        ),
        ex
      )

      throw ex
    }

    Flow[TimeSeries]
      .map { ts ⇒ OutlierDetectionMessage( ts, plan ).toEither }
      .collect { case Right( m ) ⇒ m }
      .map { m ⇒
        Metrics.inletSeries.mark()
        Metrics.inletPoints.mark( m.source.points.size )
        m
      }
      .mapAsyncUnordered( parallelism ) { m ⇒
        ( detector ?+ RouteDetection( plan.id, m ) )
          .recover {
            case ex: TimeoutException ⇒ {
              log.error(
                Map(
                  "@msg" → "timeout exceeded waiting for detection",
                  "timeout" → timeout.duration.toCoarsest.toString,
                  "plan" → m.plan.name,
                  "topic" → m.topic.toString
                ),
                ex
              )

              DetectionTimedOut( m.source, m.plan )
            }
          }
      }
      .withAttributes( ActorAttributes supervisionStrategy detectorDecider )
      .filter {
        case Envelope( DetectionResult( outliers, _ ), _ ) ⇒ true
        case DetectionResult( outliers, _ ) ⇒ true
        case Envelope( DetectionTimedOut( s, _ ), _ ) ⇒ {
          Metrics.droppedSeriesMeter.mark()
          Metrics.droppedPointsMeter.mark( s.points.size )
          false
        }
        case DetectionTimedOut( s, _ ) ⇒ {
          Metrics.droppedSeriesMeter.mark()
          Metrics.droppedPointsMeter.mark( s.points.size )
          false
        }
      }
      .collect {
        case Envelope( DetectionResult( outliers, _ ), _ ) ⇒ outliers
        case DetectionResult( outliers, _ ) ⇒ outliers
      }
      .map {
        case m ⇒ {
          val nrPoints = m.source.size
          val nrAnomalyPoints = m.anomalySize
          Metrics.outletResults.mark()
          Metrics.outletResultsPoints.mark( nrPoints )
          if ( m.hasAnomalies ) Metrics.outletResultsAnomalies.mark() else Metrics.outletResultsConformities.mark()
          Metrics.outletResultsPointsAnomalies.mark( nrAnomalyPoints )
          Metrics.outletResultsPointsConformities.mark( nrPoints - nrAnomalyPoints )
          m
        }
      }
  }

  val detectorDecider: Decider = new Decider {
    override def apply( ex: Throwable ): Supervision.Directive = {
      log.error( "error in detection dropping series", ex )
      Metrics.droppedSeriesMeter.mark()
      Supervision.Resume
    }
  }
}

object AnalysisFlowFactory {
  object Metrics extends Instrumented {
    override lazy val metricBaseName: MetricName = MetricName( spotlight.BaseMetricName, spotlight.analysis.BaseMetricName )

    val InletBaseMetricName = "inlet"
    val inletSeries: Meter = metrics.meter( InletBaseMetricName, "series" )
    val inletPoints: Meter = metrics.meter( InletBaseMetricName, "points" )
    val droppedSeriesMeter: Meter = metrics.meter( "dropped", "series" )
    val droppedPointsMeter: Meter = metrics.meter( "dropped", "points" )

    val OutletResultsBaseMetricName = "outlet.results"
    val outletResults: Meter = metrics.meter( OutletResultsBaseMetricName )
    val outletResultsAnomalies: Meter = metrics.meter( OutletResultsBaseMetricName, "anomalies" )
    val outletResultsConformities: Meter = metrics.meter( OutletResultsBaseMetricName, "conformities" )
    val outletResultsPoints: Meter = metrics.meter( OutletResultsBaseMetricName, "points" )
    val outletResultsPointsAnomalies: Meter = metrics.meter( OutletResultsBaseMetricName, "points.anomalies" )
    val outletResultsPointsConformities: Meter = metrics.meter( OutletResultsBaseMetricName, "points.conformities" )
  }
}