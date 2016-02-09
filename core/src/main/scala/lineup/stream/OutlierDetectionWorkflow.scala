package lineup.stream

import java.net.{ Socket, InetSocketAddress }
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.event.LoggingReceive

import com.typesafe.config.Config
import nl.grons.metrics.scala.{ MetricName, Meter }
import peds.akka.supervision.IsolatedLifeCycleSupervisor.ChildStarted
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream.Limiter
import peds.akka.supervision.{ OneForOneStrategyFactory, SupervisionStrategyFactory, IsolatedLifeCycleSupervisor }
import lineup.analysis.outlier.algorithm.{ CohortDensityAnalyzer, SeriesCentroidDensityAnalyzer, SeriesDensityAnalyzer }
import lineup.analysis.outlier.{ algorithm, OutlierDetection, DetectionAlgorithmRouter }
import lineup.model.outlier.Outliers
import lineup.model.timeseries.Topic
import lineup.publish.{ LogPublisher, GraphitePublisher }
import lineup.protocol.GraphiteSerializationProtocol


/**
  * Created by rolfsd on 1/6/16.
  */
object OutlierDetectionWorkflow {
  def props( makeWorkflow: => OutlierDetectionWorkflow with OneForOneStrategyFactory with ConfigurationProvider ): Props = {
    Props( makeWorkflow )
  }


  val OutlierMetricPrefix = "lineup.outlier."


  case object GetOutlierDetector
  case object GetPublishRateLimiter
  case object GetPublisher


  type MakeProps = ActorRef => Props

  trait ConfigurationProvider { outer =>
    def sourceAddress: InetSocketAddress
    def maxFrameLength: Int
    def protocol: GraphiteSerializationProtocol
    def windowDuration: FiniteDuration
    def graphiteAddress: Option[InetSocketAddress]
//    def makePlans: OutlierPlan.Creator
    def configuration: Config

    def makePublishRateLimiter()(implicit context: ActorContext ): ActorRef = {
      context.actorOf( Limiter.props(1000, 100.milliseconds, 100), "limiter" )
    }

    def makePublisher( publisherProps: Props )( implicit context: ActorContext ): ActorRef = {
      context.actorOf( publisherProps.withDispatcher("publish-dispatcher"), "publisher" )
    }

    def publisherProps: Props = {
      graphiteAddress map { address =>
        GraphitePublisher.props {
          new GraphitePublisher with GraphitePublisher.PublishProvider {
            override lazy val metricBaseName = MetricName( classOf[GraphitePublisher] )
            override def destinationAddress: InetSocketAddress = address
            override def batchInterval: FiniteDuration = 500.millis
            override def batchSize: Int = 100
            override def createSocket(address: InetSocketAddress): Socket = {
              new Socket( destinationAddress.getAddress, destinationAddress.getPort )
            }
            override def augmentTopic( o: Outliers ): Topic = OutlierMetricPrefix + o.plan.name+"." + o.topic
          }
        }
      } getOrElse {
        LogPublisher.props
      }
    }


    def makePlanRouter()( implicit context: ActorContext ): ActorRef = {
      context.actorOf( DetectionAlgorithmRouter.props.withDispatcher("outlier-detection-dispatcher"), "router" )
    }

    def makeAlgorithmWorkers( router: ActorRef )( implicit context: ActorContext ): Map[Symbol, ActorRef] = {
      Map(
        SeriesDensityAnalyzer.Algorithm -> SeriesDensityAnalyzer.props( router ),
        SeriesCentroidDensityAnalyzer.Algorithm -> SeriesCentroidDensityAnalyzer.props( router ),
        CohortDensityAnalyzer.Algorithm -> CohortDensityAnalyzer.props( router )
      ) map { case (n, p) =>
        n -> context.actorOf( p.withDispatcher("outlier-algorithm-dispatcher"), n.name )
      }
    }

    def makeOutlierDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
      context.actorOf(
        OutlierDetection.props {
          new OutlierDetection with OutlierDetection.ConfigurationProvider {
            override def router: ActorRef = routerRef
            override lazy val metricBaseName = MetricName( classOf[OutlierDetection] )
//            override def makePlans: OutlierDetection.ConfigurationProvider.Creator = outer.makePlans
//            override def refreshInterval: FiniteDuration = 15.minutes
          }
        }.withDispatcher( "outlier-detection-dispatcher" ),
        "outlierDetector"
      )
    }
  }
}

class OutlierDetectionWorkflow(
  maxNrRetries: Int = -1,
  withinTimeRange: Duration = Duration.Inf
) extends IsolatedLifeCycleSupervisor with InstrumentedActor with ActorLogging {
  outer: SupervisionStrategyFactory with OutlierDetectionWorkflow.ConfigurationProvider =>

  import lineup.stream.OutlierDetectionWorkflow._

  var detectorRef: ActorRef = _
  var publishRateLimiterRef: ActorRef = _
  var publisherRef: ActorRef = _


  override lazy val metricBaseName: MetricName = MetricName( classOf[OutlierDetectionWorkflow] )
  val failuresMeter: Meter = metrics.meter( "actor.failures" )

  override def childStarter(): Unit = {
    publishRateLimiterRef = makePublishRateLimiter( )
    publisherRef = makePublisher( publisherProps )
    val router = makePlanRouter()
    makeAlgorithmWorkers( router )
    detectorRef = makeOutlierDetector( router )
    context become around( active )
  }

  override val supervisorStrategy: SupervisorStrategy = makeStrategy( maxNrRetries, withinTimeRange ) {
    case _: ActorInitializationException => Stop

    case _: ActorKilledException => Stop

    case _: Exception => {
      failuresMeter.mark( )
      Restart
    }

    case _ => Escalate
  }

  override def receive: Receive = around( LoggingReceive{ super.receive } )

  val active: Receive = LoggingReceive {
    super.receive orElse {
      case GetOutlierDetector => sender( ) ! ChildStarted( detectorRef )
      case GetPublishRateLimiter => sender( ) ! ChildStarted( publishRateLimiterRef )
      case GetPublisher => sender() ! ChildStarted( publisherRef )
    }
  }
}
