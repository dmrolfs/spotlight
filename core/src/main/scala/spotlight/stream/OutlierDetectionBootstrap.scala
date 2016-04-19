package spotlight.stream

import java.net.{InetSocketAddress, Socket}

import scala.concurrent.duration._
import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.event.LoggingReceive
import com.typesafe.config.Config
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.supervision.IsolatedLifeCycleSupervisor.ChildStarted
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.Limiter
import peds.commons.util._
import peds.akka.supervision.{IsolatedLifeCycleSupervisor, OneForOneStrategyFactory, SupervisionStrategyFactory}
import spotlight.analysis.outlier.algorithm.density.{CohortDensityAnalyzer, SeriesCentroidDensityAnalyzer}
import spotlight.analysis.outlier.algorithm.skyline._
import spotlight.analysis.outlier.algorithm.density.SeriesDensityAnalyzer
import spotlight.analysis.outlier.{DetectionAlgorithmRouter, OutlierDetection}
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.Topic
import spotlight.publish.{GraphitePublisher, LogPublisher}
import spotlight.protocol.GraphiteSerializationProtocol


/**
  * Created by rolfsd on 1/6/16.
  */
object OutlierDetectionBootstrap {
  def props( makeWorkflow: => OutlierDetectionBootstrap with OneForOneStrategyFactory with ConfigurationProvider ): Props = {
    Props( makeWorkflow )
  }


//  val OutlierMetricPrefix = "spotlight.outlier."


  case object GetOutlierDetector
//  case object GetPublishRateLimiter
//  case object GetPublisher


  type MakeProps = ActorRef => Props

  trait ConfigurationProvider { outer =>
    def sourceAddress: InetSocketAddress
    def maxFrameLength: Int
    def protocol: GraphiteSerializationProtocol
    def windowDuration: FiniteDuration
//    def graphiteAddress: Option[InetSocketAddress]
//    def makePlans: OutlierPlan.Creator
    def configuration: Config

//    def makePublishRateLimiter()(implicit context: ActorContext ): ActorRef = {
//      context.actorOf( Limiter.props(20000, 100.milliseconds, 5000), "limiter" )
//    }

//    def makePublisher( publisherProps: Props )( implicit context: ActorContext ): ActorRef = {
//      context.actorOf( publisherProps.withDispatcher("publish-dispatcher"), "publisher" )
//    }

//    def publisherProps: Props = {
//      graphiteAddress map { address =>
//        GraphitePublisher.props {
//          new GraphitePublisher with GraphitePublisher.PublishProvider {
//            override lazy val metricBaseName = MetricName( classOf[GraphitePublisher] )
//            override def destinationAddress: InetSocketAddress = address
//            override def batchSize: Int = 1000
//            override def createSocket( address: InetSocketAddress ): Socket = {
//              new Socket( destinationAddress.getAddress, destinationAddress.getPort )
//            }
//            override def publishingTopic( p: OutlierPlan, t: Topic ): Topic = OutlierMetricPrefix + super.publishingTopic( p, t )
//          }
//        }
//      } getOrElse {
//        LogPublisher.props
//      }
//    }


    def makePlanRouter()( implicit context: ActorContext ): ActorRef = {
      context.actorOf( DetectionAlgorithmRouter.props.withDispatcher("outlier-detection-dispatcher"), "router" )
    }

    def makeAlgorithmWorkers( router: ActorRef )( implicit context: ActorContext ): Map[Symbol, ActorRef] = {
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
        n -> context.actorOf( p.withDispatcher("outlier-algorithm-dispatcher"), n.name )
      }
    }

    def makeOutlierDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
      context.actorOf(
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
//  var publishRateLimiterRef: ActorRef = _
//  var publisherRef: ActorRef = _


  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierDetectionBootstrap] )
  val failuresMeter: Meter = metrics.meter( "actor.failures" )

  override def childStarter(): Unit = {
//    publishRateLimiterRef = makePublishRateLimiter( )
//    publisherRef = makePublisher( publisherProps )
    val routerRef = makePlanRouter()
    makeAlgorithmWorkers( routerRef )
    detectorRef = makeOutlierDetector( routerRef )
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
      case GetOutlierDetector => sender( ) ! ChildStarted( detectorRef )
//      case GetPublishRateLimiter => sender( ) ! ChildStarted( publishRateLimiterRef )
//      case GetPublisher => sender() ! ChildStarted( publisherRef )
    }
  }
}
