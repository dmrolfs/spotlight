package spotlight.stream

import java.net.InetSocketAddress
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.event.LoggingReceive
import com.typesafe.config.Config
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.supervision.IsolatedLifeCycleSupervisor.ChildStarted
import peds.akka.metrics.InstrumentedActor
import peds.commons.util._
import peds.akka.supervision.{IsolatedLifeCycleSupervisor, OneForOneStrategyFactory, SupervisionStrategyFactory}
import spotlight.analysis.outlier.algorithm.density.{CohortDensityAnalyzer, SeriesCentroidDensityAnalyzer}
import spotlight.analysis.outlier.algorithm.skyline._
import spotlight.analysis.outlier.algorithm.density.SeriesDensityAnalyzer
import spotlight.analysis.outlier.{DetectionAlgorithmRouter, OutlierDetection, OutlierPlanDetectionRouter}
import spotlight.protocol.GraphiteSerializationProtocol


/**
  * Created by rolfsd on 1/6/16.
  */
object OutlierDetectionBootstrap {
  def props( makeWorkflow: => OutlierDetectionBootstrap with OneForOneStrategyFactory with ConfigurationProvider ): Props = {
    Props( makeWorkflow )
  }


  case object GetOutlierDetector
  case object GetOutlierPlanDetectionRouter


  type MakeProps = ActorRef => Props

  trait ConfigurationProvider { outer: Actor with ActorLogging =>
    def sourceAddress: InetSocketAddress
    def maxFrameLength: Int
    def protocol: GraphiteSerializationProtocol
    def windowDuration: FiniteDuration
    def detectionBudget: FiniteDuration
    def bufferSize: Int
    def maxInDetectionCpuFactor: Double
//    def makePlans: OutlierPlan.Creator
    def configuration: Config

    def makePlanDetectionRouter(
      detector: ActorRef,
      detectionBudget: FiniteDuration,
      bufferSize: Int,
      maxInDetectionCpuFactor: Double
    )(
      implicit ctx: ActorContext
    ): ActorRef = {
      log.info(
        "bootstrap making plan detection router for detector:[{}] budget:[{}] buffer:[{}] cpu factor:[{}]",
        detector,
        detectionBudget,
        bufferSize,
        maxInDetectionCpuFactor
      )

      ctx.actorOf(
        OutlierPlanDetectionRouter
        .props( detector, detectionBudget, bufferSize, maxInDetectionCpuFactor )
        .withDispatcher( "plan-router-dispatcher" ),
        "plan-detection-router"
      )
    }

    def makeAlgorithmRouter()( implicit ctx: ActorContext ): ActorRef = {
      ctx.actorOf( DetectionAlgorithmRouter.props.withDispatcher("outlier-detection-dispatcher"), "router" )
    }

    def makeAlgorithmWorkers( router: ActorRef )( implicit ctx: ActorContext ): Map[Symbol, ActorRef] = {
      Map(
        SeriesDensityAnalyzer.Algorithm -> SeriesDensityAnalyzer.props( router ),
        SeriesCentroidDensityAnalyzer.Algorithm -> SeriesCentroidDensityAnalyzer.props( router ),
        CohortDensityAnalyzer.Algorithm -> CohortDensityAnalyzer.props( router ),
        ExponentialMovingAverageAnalyzer.Algorithm -> ExponentialMovingAverageAnalyzer.props( router ),
        FirstHourAverageAnalyzer.Algorithm -> FirstHourAverageAnalyzer.props( router ),
        GrubbsAnalyzer.Algorithm -> GrubbsAnalyzer.props( router ),
        HistogramBinsAnalyzer.Algorithm -> HistogramBinsAnalyzer.props( router ),
        KolmogorovSmirnovAnalyzer.Algorithm -> KolmogorovSmirnovAnalyzer.props( router ),
        LeastSquaresAnalyzer.Algorithm -> LeastSquaresAnalyzer.props( router ),
        MeanSubtractionCumulationAnalyzer.Algorithm -> MeanSubtractionCumulationAnalyzer.props( router ),
        MedianAbsoluteDeviationAnalyzer.Algorithm -> MedianAbsoluteDeviationAnalyzer.props( router ),
        SimpleMovingAverageAnalyzer.Algorithm -> SimpleMovingAverageAnalyzer.props( router ),
        SeasonalExponentialMovingAverageAnalyzer.Algorithm -> SeasonalExponentialMovingAverageAnalyzer.props( router )
      ) map { case (n, p) =>
        n -> ctx.actorOf( p.withDispatcher("outlier-algorithm-dispatcher"), n.name )
      }
    }

    def makeOutlierDetector( routerRef: ActorRef )( implicit ctx: ActorContext ): ActorRef = {
      ctx.actorOf(
        OutlierDetection.props( routerRef = routerRef ).withDispatcher( "outlier-detection-dispatcher" ),
        "outlierDetector"
      )
    }
  }
}

class OutlierDetectionBootstrap(
  maxNrRetries: Int = -1,
  withinTimeRange: Duration = Duration.Inf
) extends IsolatedLifeCycleSupervisor with InstrumentedActor with ActorLogging {
  outer: SupervisionStrategyFactory with OutlierDetectionBootstrap.ConfigurationProvider =>

  import spotlight.stream.OutlierDetectionBootstrap._

  var detectorRef: ActorRef = _
  var planRouterRef: ActorRef = _


  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierDetectionBootstrap] )
  val failuresMeter: Meter = metrics.meter( "actor.failures" )

  override def childStarter(): Unit = {
    val routerRef = makeAlgorithmRouter( )
    makeAlgorithmWorkers( routerRef )
    detectorRef = makeOutlierDetector( routerRef )
    planRouterRef = makePlanDetectionRouter(
      detector = detectorRef,
      detectionBudget = outer.detectionBudget,
      bufferSize = outer.bufferSize,
      maxInDetectionCpuFactor = outer.maxInDetectionCpuFactor
    )

    context become LoggingReceive{ around( active ) }
  }

  override val supervisorStrategy: SupervisorStrategy = makeStrategy( maxNrRetries, withinTimeRange ) {
    case _: ActorInitializationException => Stop

    case _: ActorKilledException => Stop

    case ex: InvalidActorNameException => {
      log.warning( "{} restarting on caught invalid actor name: [{}]", getClass.safeSimpleName, ex)
      failuresMeter.mark()
      Restart
    }

    case ex: Exception => {
      log.warning( "{} restarting on caught exception from child: [{}]", getClass.safeSimpleName, ex )
      failuresMeter.mark()
      Restart
    }

    case _ => Escalate
  }

  override def receive: Receive = around( LoggingReceive{ super.receive } )

  val active: Receive = LoggingReceive {
    super.receive orElse {
      case GetOutlierDetector => sender() ! ChildStarted( detectorRef )
      case GetOutlierPlanDetectionRouter => sender() ! ChildStarted( planRouterRef )
    }
  }
}
