package spotlight.analysis.outlier

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

  val registration: Receive = LoggingReceive {
    case RegisterDetectionAlgorithm( algorithm, handler ) => {
      routingTable += ( algorithm -> handler )
      sender() ! AlgorithmRegistered( algorithm )
    }
  }

  val routing: Receive = LoggingReceive {
    case m: DetectUsing if routingTable contains m.algorithm => routingTable( m.algorithm ) forward m
  }

  override val receive: Receive = around( registration orElse routing )

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => log.error( s"cannot route unregistered algorithm [${m.algorithm}]" )
      case m => log.error( s"ROUTER UNAWARE OF message: [{}]\nrouting table:{}", m, routingTable.toSeq.mkString("\n",",","\n") ) // super.unhandled( m )
    }
  }
}
