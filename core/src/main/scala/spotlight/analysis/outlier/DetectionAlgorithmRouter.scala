package spotlight.analysis.outlier

import akka.actor._
import akka.event.LoggingReceive
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor


/**
 * Created by rolfsd on 9/29/15.
 */
object DetectionAlgorithmRouter {
  sealed trait RouterProtocol
  case class RegisterDetectionAlgorithm( algorithm: Symbol, handler: ActorRef ) extends RouterProtocol
  case class AlgorithmRegistered( algorithm: Symbol ) extends RouterProtocol

  val ContextKey = 'DetectionAlgorithmRouter

  def props( routingTable: Map[Symbol, ActorRef] ): Props = Props( new Default( routingTable ) )

  private class Default( override val initialRoutingTable: Map[Symbol, ActorRef] ) extends DetectionAlgorithmRouter with Provider


  trait Provider {
    def initialRoutingTable: Map[Symbol, ActorRef]
  }
}

class DetectionAlgorithmRouter extends Actor with EnvelopingActor with InstrumentedActor with ActorLogging {
  provider: DetectionAlgorithmRouter.Provider =>

  var routingTable: Map[Symbol, ActorRef] = provider.initialRoutingTable
  log.debug( "DetectionAlgorithmRouter[{}] created supporting algorithms:[{}]", routingTable.keys.mkString(",") )

  import DetectionAlgorithmRouter.{ RegisterDetectionAlgorithm, AlgorithmRegistered }

  val registration: Receive = {
    case RegisterDetectionAlgorithm( algorithm, handler ) => {
      routingTable += ( algorithm -> handler )
      sender() !+ AlgorithmRegistered( algorithm )
    }
  }

  val routing: Receive = {
    case m: DetectUsing if routingTable contains m.algorithm => routingTable( m.algorithm ) forwardEnvelope m
  }

  override val receive: Receive = LoggingReceive{ around( registration orElse routing ) }

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => log.error( s"cannot route unregistered algorithm [${m.algorithm}]" )
      case m => log.error( s"ROUTER UNAWARE OF message: [{}]\nrouting table:{}", m, routingTable.toSeq.mkString("\n",",","\n") ) // super.unhandled( m )
    }
  }
}
