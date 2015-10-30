package lineup.analysis.outlier.algorithm

import akka.actor.{ActorPath, ActorRef, ActorLogging}
import akka.event.LoggingReceive
import lineup.analysis.outlier.{DetectUsing, DetectionAlgorithmRouter}
import peds.akka.envelope.EnvelopingActor


abstract class AlgorithmActor extends EnvelopingActor with ActorLogging {
  def algorithm: Symbol
  def router: ActorRef

  override def preStart(): Unit = {
    context watch router
    router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm, self )
  }

  override def receive: Receive = around( quiescent )

  def quiescent: Receive = LoggingReceive {
    case DetectionAlgorithmRouter.AlgorithmRegistered => context.become( around(detect) )

    case m: DetectUsing => throw AlgorithmActor.AlgorithmUsedBeforeRegistrationError( algorithm, self.path )
  }

  def detect: Receive
}

object AlgorithmActor {
  case class AlgorithmUsedBeforeRegistrationError( algorithm: Symbol, path: ActorPath )
  extends IllegalStateException( s"actor [${path}] not registered algorithm [${algorithm.name}] before use" )
  with OutlierAlgorithmError
}
