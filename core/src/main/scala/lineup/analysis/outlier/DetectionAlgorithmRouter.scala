package lineup.analysis.outlier

import akka.actor._
import akka.event.LoggingReceive
import nl.grons.metrics.scala.MetricName
import peds.akka.metrics.InstrumentedActor


/**
 * Created by rolfsd on 9/29/15.
 */
object DetectionAlgorithmRouter {
  sealed trait RouterProtocol
  case class RegisterDetectionAlgorithm( algorithm: Symbol, handler: ActorRef ) extends RouterProtocol
  case class AlgorithmRegistered( algorithm: Symbol ) extends RouterProtocol

  val ContextKey = 'DetectionAlgorithmRouter

  def props: Props = Props( new DetectionAlgorithmRouter )
}

class DetectionAlgorithmRouter extends Actor with InstrumentedActor with ActorLogging {
  import DetectionAlgorithmRouter._
  var routingTable: Map[Symbol, ActorRef] = Map()

  override val supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy

  override def receive: Receive = around( registration orElse routing )

  val registration: Receive = LoggingReceive {
    case RegisterDetectionAlgorithm( algorithm, handler ) => {
      routingTable += ( algorithm -> handler )
      sender() ! AlgorithmRegistered( algorithm )
    }
  }

  val routing: Receive = LoggingReceive {
    case m @ DetectUsing( algorithm, _, _, _ ) if routingTable.contains( algorithm ) => routingTable( algorithm ) forward m
  }
}
