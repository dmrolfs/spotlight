package lineup.analysis.outlier.algorithm

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import lineup.model.outlier.OutlierPlan
import lineup.model.timeseries.{DataPoint, TimeSeriesBase, Topic}
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance}
import peds.commons.math.MahalanobisDistance

import scalaz._
import Scalaz._
import scalaz.Kleisli.kleisli
import org.apache.commons.math3.ml.clustering.{Cluster, DoublePoint}
import peds.akka.metrics.InstrumentedActor
import lineup.analysis.outlier._


trait AlgorithmActor extends Actor with InstrumentedActor with ActorLogging {
  import AlgorithmActor._

  def algorithm: Symbol
  def router: ActorRef


  override def preStart(): Unit = {
    context watch router
    router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm, self )
  }

  override def receive: Receive = around( quiescent )

  def quiescent: Receive = LoggingReceive {
    case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if a == algorithm => {
      log.info( "{} registered [{}] with {}", self.path, algorithm.name, sender().path )
      context become around( detect )
    }

    case m: DetectUsing => throw AlgorithmActor.AlgorithmUsedBeforeRegistrationError( algorithm, self.path )
  }

  def detect: Receive

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => {
        log.info( "algorithm [{}] does not recognize requested payload: [{}]", algorithm, m )
        m.aggregator ! UnrecognizedPayload( algorithm, m )
      }

      case m => super.unhandled( m )
    }
  }


  // -- algorithm functional elements --

  def algorithmContext: Op[DetectUsing, AlgorithmContext] = {
    Kleisli[TryV, DetectUsing, AlgorithmContext] { m => AlgorithmContext( m, DataPoint.toDoublePoints( m.source.points ) ).right }
  }

  val tolerance: Op[AlgorithmContext, Option[Double]] = Kleisli[TryV, AlgorithmContext, Option[Double]] {_.tolerance }

  val messageConfig: Op[AlgorithmContext, Config] = kleisli[TryV, AlgorithmContext, Config] {_.messageConfig.right }

}

object AlgorithmActor {
  type TryV[T] = Throwable\/T
  type Op[I, O] = Kleisli[TryV, I, O]

  type Points = Seq[DoublePoint]
  type Point = Array[Double]
  type Point2D = (Double, Double)


  trait AlgorithmContext {
    def message: DetectUsing
    def data: Seq[DoublePoint]
    def algorithm: Symbol
    def topic: Topic
    def plan: OutlierPlan
    def historyKey: HistoryKey
    def history: HistoricalStatistics
    def source: TimeSeriesBase
    def messageConfig: Config
    def distanceMeasure: TryV[DistanceMeasure]
    def tolerance: TryV[Option[Double]]
  }

  object AlgorithmContext {
    def apply( message: DetectUsing, data: Seq[DoublePoint] ): AlgorithmContext = SimpleAlgorithmContext( message, data )


    final case class SimpleAlgorithmContext private[algorithm](
      message: DetectUsing, data: Seq[DoublePoint]
    ) extends AlgorithmContext {
      override val algorithm: Symbol = message.algorithm
      override val topic: Topic = message.topic
      override def plan: OutlierPlan = message.plan
      override val historyKey: HistoryKey = HistoryKey( plan, topic )
      override def history: HistoricalStatistics = message.history
      override def source: TimeSeriesBase = message.source
      override def messageConfig: Config = message.properties

      override def distanceMeasure: TryV[DistanceMeasure] = {
        def makeMahalanobisDistance: TryV[DistanceMeasure] = {
          val mahal = if ( message.history.n > 0 ) {
            MahalanobisDistance.fromCovariance( message.history.covariance )
          } else {
            MahalanobisDistance.fromPoints( MatrixUtils.createRealMatrix( data.toArray map { _.getPoint } ) )
          }

          mahal.disjunction.leftMap{ _.head }
        }

        val distancePath = algorithm.name + ".distance"
        if ( message.properties hasPath distancePath ) {
          message.properties.getString( distancePath ).toLowerCase match {
            case "euclidean" | "euclid" => new EuclideanDistance().right[Throwable]
            case "mahalanobis" | "mahal" | _ => makeMahalanobisDistance
          }
        } else {
          makeMahalanobisDistance
        }
      }

      override def tolerance: TryV[Option[Double]] = {
        val path = algorithm.name+".tolerance"
        \/ fromTryCatchNonFatal {
          if ( messageConfig hasPath path ) Some( messageConfig getDouble path ) else None
        }
      }
    }
  }


  case class AlgorithmUsedBeforeRegistrationError( algorithm: Symbol, path: ActorPath )
    extends IllegalStateException( s"actor [${path}] not registered algorithm [${algorithm.name}] before use" )
    with OutlierAlgorithmError
}

