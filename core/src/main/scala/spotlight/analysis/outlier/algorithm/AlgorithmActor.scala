package spotlight.analysis.outlier.algorithm

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef}
import akka.event.LoggingReceive

import scalaz._
import Scalaz._
import scalaz.Kleisli.kleisli
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Timer
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance}
import org.apache.commons.math3.ml.clustering.DoublePoint
import peds.akka.metrics.InstrumentedActor
import peds.commons.{TryV, KOp}
import peds.commons.math.MahalanobisDistance
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.{ControlBoundary, DataPoint, TimeSeriesBase, Topic}
import spotlight.analysis.outlier._


trait AlgorithmActor extends Actor with InstrumentedActor with ActorLogging {
  import AlgorithmActor._

  def algorithm: Symbol
  def router: ActorRef

  lazy val algorithmTimer: Timer = metrics.timer( "algorithm", algorithm.name )

  override def preStart(): Unit = {
    context watch router
    log.info( "attempting to register [{}] @ [{}] with {}", algorithm.name, self.path, sender().path )
    router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm, self )
  }

  override def receive: Receive = around( quiescent )

  def quiescent: Receive = LoggingReceive {
    case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if a == algorithm => {
      log.info( "registration confirmed for [{}] @ [{}] with {}", algorithm.name, self.path, sender().path )
      context become around( detect )
    }

    case m: DetectUsing => throw AlgorithmActor.AlgorithmUsedBeforeRegistrationError( algorithm, self.path )
  }

  def detect: Receive

  override def unhandled( message: Any ): Unit = {
    message match {
      case m: DetectUsing => {
        log.warning( "algorithm [{}] does not recognize requested payload: [{}]", algorithm, m )
        m.aggregator ! UnrecognizedPayload( algorithm, m )
      }

      case m => super.unhandled( m )
    }
  }


  // -- algorithm functional elements --

  def algorithmContext: KOp[DetectUsing, AlgorithmContext] = {
    Kleisli[TryV, DetectUsing, AlgorithmContext] { m => AlgorithmContext( m, DataPoint.toDoublePoints( m.source.points ) ).right }
  }

  val tolerance: KOp[AlgorithmContext, Option[Double]] = Kleisli[TryV, AlgorithmContext, Option[Double]] { _.tolerance }

  val messageConfig: KOp[AlgorithmContext, Config] = kleisli[TryV, AlgorithmContext, Config] { _.messageConfig.right }

}

object AlgorithmActor {
  trait AlgorithmContext {
    def message: DetectUsing
    def data: Seq[DoublePoint]
    def algorithm: Symbol
    def topic: Topic
    def plan: OutlierPlan
    def historyKey: HistoryKey
    def history: HistoricalStatistics
    def source: TimeSeriesBase
    def controlBoundaries: Seq[ControlBoundary]
    def messageConfig: Config
    def distanceMeasure: TryV[DistanceMeasure]
    def tolerance: TryV[Option[Double]]

    type That <: AlgorithmContext
    def withSource( newSource: TimeSeriesBase ): That
    def addControlBoundary( control: ControlBoundary ): That
  }

  object AlgorithmContext extends LazyLogging {
    def apply( message: DetectUsing, data: Seq[DoublePoint] ): AlgorithmContext = {
      SimpleAlgorithmContext( message, message.source, data )
    }


    final case class SimpleAlgorithmContext private[algorithm](
      override val message: DetectUsing,
      override val source: TimeSeriesBase,
      override val data: Seq[DoublePoint],
      override val controlBoundaries: Seq[ControlBoundary] = Seq.empty[ControlBoundary]
    ) extends AlgorithmContext {
      override val algorithm: Symbol = message.algorithm
      override val topic: Topic = message.topic
      override def plan: OutlierPlan = message.plan
      override val historyKey: HistoryKey = HistoryKey( plan, topic )
      override def history: HistoricalStatistics = message.history
      override def messageConfig: Config = message.properties

      override def distanceMeasure: TryV[DistanceMeasure] = {
        def makeMahalanobisDistance: TryV[DistanceMeasure] = {
          val mahal = if ( message.history.N > 0 ) {
            logger.debug( "DISTANCE_MEASURE message.history.covariance = {}", message.history.covariance)
            MahalanobisDistance.fromCovariance( message.history.covariance )
          } else {
            logger.debug( "DISTANCE_MEASURE point data = {}", data.mkString("[",",","]"))
            MahalanobisDistance.fromPoints( MatrixUtils.createRealMatrix( data.toArray map { _.getPoint } ) )
          }
          logger.debug( "DISTANCE_MEASURE mahal = {}  sample distance={}", mahal, mahal.map{ _.compute(Array(3D, 18.4), Array(2D, 19.2))})
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

      override type That = SimpleAlgorithmContext
      override def withSource( newSource: TimeSeriesBase ): That = copy( source = newSource )
      override def addControlBoundary( control: ControlBoundary ): That = copy( controlBoundaries = controlBoundaries :+ control )
    }
  }


  case class AlgorithmUsedBeforeRegistrationError( algorithm: Symbol, path: ActorPath )
    extends IllegalStateException( s"actor [${path}] not registered algorithm [${algorithm.name}] before use" )
    with OutlierAlgorithmError
}

