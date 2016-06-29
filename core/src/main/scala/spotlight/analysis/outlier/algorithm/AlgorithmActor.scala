package spotlight.analysis.outlier.algorithm

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef}
import akka.event.LoggingReceive

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{MetricName, Timer}
import org.apache.commons.math3.linear.{EigenDecomposition, MatrixUtils}
import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance}
import org.apache.commons.math3.ml.clustering.DoublePoint
import peds.akka.metrics.InstrumentedActor
import peds.commons.{KOp, TryV}
import peds.commons.math.MahalanobisDistance
import spotlight.analysis.outlier.{DetectUsing, DetectionAlgorithmRouter, HistoricalStatistics, UnrecognizedPayload}
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries._


sealed trait AlgorithmProtocolOLD
object AlgorithmProtocolOLD extends {
  case class Register( scopeId: OutlierPlan.Scope, routerRef: ActorRef ) extends AlgorithmProtocolOLD
  case class Registered( scopeId: OutlierPlan.Scope ) extends AlgorithmProtocolOLD
}


trait AlgorithmActor extends Actor with InstrumentedActor with ActorLogging {
  import AlgorithmActor._

  def algorithm: Symbol
  def router: ActorRef

  override lazy val metricBaseName: MetricName = MetricName( "spotlight.analysis.outlier.algorithm" )
  lazy val algorithmTimer: Timer = metrics timer algorithm.name

  override def preStart(): Unit = {
    context watch router
    log.info( "attempting to register [{}] @ [{}] with {}", algorithm.name, self.path, sender().path )
    router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm, self )
  }

  override def receive: Receive = LoggingReceive{ around( quiescent ) }

  def quiescent: Receive = {
    case AlgorithmProtocolOLD.Register( scopeId, routerRef ) => {
      import scala.concurrent.duration._
      import akka.pattern.{ ask, pipe }

      implicit val timeout = akka.util.Timeout( 15.seconds )
      implicit val ec = context.dispatcher

      val resp = ( routerRef ? DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm, self ) )
      val registered = resp map { case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if a == algorithm =>
        AlgorithmProtocolOLD.Registered( scopeId )
      }
      registered pipeTo sender()
    }

    case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if a == algorithm => {
      log.info( "registration confirmed for [{}] @ [{}] with {}", algorithm.name, self.path, sender().path )
      context become LoggingReceive{ around( detect ) }
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
    Kleisli[TryV, DetectUsing, AlgorithmContext] { m => AlgorithmContext( m, m.source.points ).right }
  }

  val tolerance: KOp[AlgorithmContext, Option[Double]] = Kleisli[TryV, AlgorithmContext, Option[Double]] { _.tolerance }

  val messageConfig: KOp[AlgorithmContext, Config] = kleisli[TryV, AlgorithmContext, Config] { _.messageConfig.right }

  /**
    * Some algorithms require a minimum number of points in order to determine an anomaly. To address this circumstance, these
    * algorithms can use fillDataFromHistory to draw from history the points necessary to create an appropriate group.
    * @param minimalSize of the data grouping
    * @return
    */
  def fillDataFromHistory( minimalSize: Int = HistoricalStatistics.LastN ): KOp[AlgorithmContext, Seq[DoublePoint]] = {
    for {
      ctx <- ask[TryV, AlgorithmContext]
    } yield {
      val original = ctx.data
      if ( minimalSize < original.size ) original
      else {
        val inHistory = ctx.history.lastPoints.size
        val needed = minimalSize + 1 - original.size
        val past = ctx.history.lastPoints.drop( inHistory - needed )
        past.toDoublePoints ++ original
      }
    }
  }
}

object AlgorithmActor {

  trait AlgorithmContext {
    def message: DetectUsing
    def data: Seq[DoublePoint]
    def algorithm: Symbol
    def topic: Topic
    def plan: OutlierPlan
    def historyKey: OutlierPlan.Scope
    def history: HistoricalStatistics
    def source: TimeSeriesBase
    def thresholdBoundaries: Seq[ThresholdBoundary]
    def messageConfig: Config
    def distanceMeasure: TryV[DistanceMeasure]
    def tolerance: TryV[Option[Double]]

    type That <: AlgorithmContext
    def withSource( newSource: TimeSeriesBase ): That
    def addThresholdBoundary(control: ThresholdBoundary ): That
  }

  object AlgorithmContext extends LazyLogging {
    def apply( message: DetectUsing, data: Seq[DoublePoint] ): AlgorithmContext = {
      SimpleAlgorithmContext( message, message.source, data )
    }


    final case class SimpleAlgorithmContext private[algorithm](
      override val message: DetectUsing,
      override val source: TimeSeriesBase,
      override val data: Seq[DoublePoint],
      override val thresholdBoundaries: Seq[ThresholdBoundary] = Seq.empty[ThresholdBoundary]
    ) extends AlgorithmContext {
      override val algorithm: Symbol = message.algorithm
      override val topic: Topic = message.topic
      override def plan: OutlierPlan = message.plan
      override val historyKey: OutlierPlan.Scope = OutlierPlan.Scope( plan, topic )
      override def history: HistoricalStatistics = message.history
      override def messageConfig: Config = message.properties

      override def distanceMeasure: TryV[DistanceMeasure] = {
        def makeMahalanobisDistance: TryV[DistanceMeasure] = {
          val mahal = if ( message.history.N > 0 ) {
            logger.debug(
              "DISTANCE_MEASURE message.history.covariance = [{}] determinant:[{}]",
              message.history.covariance,
              new EigenDecomposition(message.history.covariance).getDeterminant.toString
            )
            MahalanobisDistance.fromCovariance( message.history.covariance )
          } else {
            logger.debug( "DISTANCE_MEASURE point data = {}", data.mkString("[",",","]"))
            MahalanobisDistance.fromPoints( MatrixUtils.createRealMatrix( data.toArray map { _.toPointA } ) )
          }
          logger.debug( "DISTANCE_MEASURE mahal = [{}]", mahal )
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
      override def addThresholdBoundary(threshold: ThresholdBoundary ): That = {
        copy( thresholdBoundaries = thresholdBoundaries :+ threshold )
      }
    }
  }


  case class AlgorithmUsedBeforeRegistrationError( algorithm: Symbol, path: ActorPath )
    extends IllegalStateException( s"actor [${path}] not registered algorithm [${algorithm.name}] before use" )
    with OutlierAlgorithmError
}

