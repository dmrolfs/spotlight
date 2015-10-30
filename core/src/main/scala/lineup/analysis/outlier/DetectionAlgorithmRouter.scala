package lineup.analysis.outlier

import akka.actor._
import akka.event.LoggingReceive
import peds.akka.envelope._
import peds.commons.log.Trace


/**
 * Created by rolfsd on 9/29/15.
 */
object DetectionAlgorithmRouter {
  sealed trait RouterProtocol
  case class RegisterDetectionAlgorithm( algorithm: Symbol, handler: ActorRef ) extends RouterProtocol
  case object AlgorithmRegistered extends RouterProtocol

  val ContextKey = 'DetectionAlgorithmRouter

  def props: Props = Props( new DetectionAlgorithmRouter )
}

class DetectionAlgorithmRouter extends EnvelopingActor with ActorLogging {
  import DetectionAlgorithmRouter._
  override def trace: Trace[_] = Trace[DetectionAlgorithmRouter]

  var routingTable: Map[Symbol, ActorRef] = Map()

  override val supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy

  override def receive: Receive = around( registration orElse routing )

  val registration: Receive = LoggingReceive {
    case RegisterDetectionAlgorithm( algorithm, handler ) => {
      routingTable += ( algorithm -> handler )
      sender() ! AlgorithmRegistered
    }
  }

  val routing: Receive = LoggingReceive {
//todo stream enveloping:    case m @ DetectUsing( algorithm, _, _, _ ) if routingTable.contains( algorithm ) => routingTable( algorithm ) sendForward m
    case m @ DetectUsing( algorithm, _, _, _ ) if routingTable.contains( algorithm ) => routingTable( algorithm ) forward m
  }
}
