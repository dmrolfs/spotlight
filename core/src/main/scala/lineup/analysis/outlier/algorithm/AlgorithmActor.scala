package lineup.analysis.outlier.algorithm

import akka.actor.{ Actor, ActorPath, ActorRef, ActorLogging }
import akka.event.LoggingReceive
import lineup.analysis.outlier.OutlierDetection.UnrecognizedTopic
import peds.akka.metrics.InstrumentedActor
import lineup.analysis.outlier.{ UnrecognizedPayload, DetectUsing, DetectionAlgorithmRouter }


trait AlgorithmActor extends Actor with InstrumentedActor with ActorLogging {
  def algorithm: Symbol
  def router: ActorRef

  override def preStart(): Unit = {
    context watch router
    router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm, self )
  }

  override def receive: Receive = around( quiescent )

  def quiescent: Receive = LoggingReceive {
    case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if a == algorithm => {
      log info s"${self.path} registered [${algorithm.name}] with ${sender().path}"
      context become around( detect )
    }

    case m: DetectUsing => throw AlgorithmActor.AlgorithmUsedBeforeRegistrationError( algorithm, self.path )
  }

  def detect: Receive

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => {
        log info s"algorithm [${algorithm}] does not recognize requested payload: [${m}]"
        m.aggregator ! UnrecognizedPayload( algorithm, m )
      }

      case m => super.unhandled( m )
    }

  }
}

object AlgorithmActor {
  case class AlgorithmUsedBeforeRegistrationError( algorithm: Symbol, path: ActorPath )
  extends IllegalStateException( s"actor [${path}] not registered algorithm [${algorithm.name}] before use" )
  with OutlierAlgorithmError
}
