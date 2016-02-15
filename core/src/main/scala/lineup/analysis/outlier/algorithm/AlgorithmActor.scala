package lineup.analysis.outlier.algorithm

import akka.actor.{ Actor, ActorPath, ActorRef, ActorLogging }
import akka.event.LoggingReceive
import com.typesafe.config.Config
import lineup.model.outlier.OutlierPlan
import lineup.model.timeseries.{ TimeSeriesBase, TimeSeriesCohort, DataPoint, TimeSeries }
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.ml.distance.{ EuclideanDistance, DistanceMeasure }
import peds.commons.Valid
import peds.commons.math.MahalanobisDistance
import scalaz._, Scalaz._
import scalaz.Kleisli.kleisli
import org.apache.commons.math3.ml.clustering.{ Cluster, DoublePoint }
import peds.akka.metrics.InstrumentedActor
import lineup.analysis.outlier.{ HistoricalStatistics, UnrecognizedPayload, DetectUsing, DetectionAlgorithmRouter }


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

//  def identityK[T]: Op[T, T] = Kleisli[TryV, T, T] { _.right }

//  val plan: Op[AnalyzerContext, OutlierPlan] = Kleisli[TryV, AnalyzerContext, OutlierPlan] { ctx => ctx.message.plan.right }

//  val history: Op[AnalyzerContext, Option[HistoricalStatistics]] = {
//    Kleisli[TryV, AnalyzerContext, Option[HistoricalStatistics]] { _.message.history.right }
//  }

//  val tolerance: Op[Config, Option[Double]] = Kleisli[TryV, Config, Option[Double]] { c =>
//    \/ fromTryCatchNonFatal {
//      if ( c hasPath algorithm.name+".tolerance" ) Some( c getDouble algorithm.name+".tolerance" ) else None
//    }
//  }

  def algorithmContext: Op[DetectUsing, Context] = {
    Kleisli[TryV, DetectUsing, Context] {message =>
      Context( message, DataPoint.toDoublePoints( message.source.points ) ).right
    }
  }
//  val analyzerContext: Op[DetectUsing, AnalyzerContext] = {
//    Kleisli[TryV, DetectUsing, AnalyzerContext] { d =>
//      val points: TryV[Seq[DoublePoint]] = d.payload.source match {
//        case s: TimeSeries => DataPoint.toDoublePoints( s.points ).right[Throwable]
//
//        case c: TimeSeriesCohort => {
//          //          def cohortDistances( cohort: TimeSeriesCohort ): Matrix[DataPoint] = {
//          //            for {
//          //              frame <- cohort.toMatrix
//          //              timestamp = frame.head._1 // all of frame's _1 better be the same!!!
//          //              frameValues = frame map { _._2 }
//          //              stats = new DescriptiveStatistics( frameValues.toArray )
//          //              median = stats.getPercentile( 50 )
//          //            } yield frameValues map { v => DataPoint( timestamp, v - median ) }
//          //          }
//          //
//          //          val outlierMarks: Row[Row[DoublePoint]] = for {
//          //            frameDistances <- cohortDistances( c )
//          //            points = frameDistances map { DataPoint.toDoublePoint }
//          //          } yield points
//
//          //todo: work in progress as part of larger goal to type class b/h around outlier calcs
//          -\/( new UnsupportedOperationException( s"can't support cohorts yet" ) )
//        }
//
//        case x => -\/( new UnsupportedOperationException( s"cannot extract test context from [${x.getClass}]" ) )
//      }
//
//      points map { pts => AnalyzerContext( data = pts, message = d ) }
//    }
//  }

//  val source: Op[AnalyzerContext, TimeSeriesBase] = Kleisli[TryV, AnalyzerContext, TimeSeriesBase] { _.message.source.right }

//  val data: Op[AnalyzerContext, Seq[DoublePoint]] = {
//    Kleisli[TryV, AnalyzerContext, Seq[DoublePoint]] { _.data.right }
//  }

  val messageConfig: Op[Context, Config] = kleisli[TryV, Context, Config] {_.messageConfig.right }

//  val distanceMeasure: Op[AnalyzerContext, DistanceMeasure] = {
//    Kleisli[TryV, AnalyzerContext, DistanceMeasure] { _.distanceMeasure }

//      case AnalyzerContext( message, data ) =>
//      def makeMahalanobisDistance: TryV[DistanceMeasure] = {
//        val mahal = message.history map { h =>
//          MahalanobisDistance.fromCovariance( h.covariance )
//        } getOrElse {
//          MahalanobisDistance.fromPoints( MatrixUtils.createRealMatrix( data.toArray map { _.getPoint } ) )
//        }
//
//        mahal.disjunction.leftMap{ _.head }
//      }
//
//      val distancePath = algorithm.name + ".distance"
//      if ( message.properties hasPath distancePath ) {
//        message.properties.getString( distancePath ).toLowerCase match {
//          case "euclidean" | "euclid" => new EuclideanDistance().right[Throwable]
//          case "mahalanobis" | "mahal" | _ => makeMahalanobisDistance
//        }
//      } else {
//        makeMahalanobisDistance
//      }
//    }
//  }


}

object AlgorithmActor {
  type TryV[T] = Throwable\/T
  type Op[I, O] = Kleisli[TryV, I, O]
  type Clusters = Seq[Cluster[DoublePoint]]


  trait Context {
    def message: DetectUsing
    def data: Seq[DoublePoint]
    def algorithm: Symbol
    def plan: OutlierPlan
    def history: HistoricalStatistics
    def source: TimeSeriesBase
    def messageConfig: Config
    def distanceMeasure: TryV[DistanceMeasure]
    def tolerance: TryV[Option[Double]]
  }

  object Context {
//    //todo remove in favor of message and data?
//    def fromMessage( message: DetectUsing ): Valid[AnalyzerContext] = {
//      checkSource( message.payload.source ) map { pts => SimpleAnalyzerContext( message, data = pts ) }
//    }
//
    def apply( message: DetectUsing, data: Seq[DoublePoint] ): Context = SimpleContext( message, data )

//    def checkSource( source: TimeSeriesBase ): Valid[Seq[DoublePoint]] = {
//      source match {
//        case s: TimeSeries => DataPoint.toDoublePoints( s.points ).successNel
//
//        case c: TimeSeriesCohort => {
//          //          def cohortDistances( cohort: TimeSeriesCohort ): Matrix[DataPoint] = {
//          //            for {
//          //              frame <- cohort.toMatrix
//          //              timestamp = frame.head._1 // all of frame's _1 better be the same!!!
//          //              frameValues = frame map { _._2 }
//          //              stats = new DescriptiveStatistics( frameValues.toArray )
//          //              median = stats.getPercentile( 50 )
//          //            } yield frameValues map { v => DataPoint( timestamp, v - median ) }
//          //          }
//          //
//          //          val outlierMarks: Row[Row[DoublePoint]] = for {
//          //            frameDistances <- cohortDistances( c )
//          //            points = frameDistances map { DataPoint.toDoublePoint }
//          //          } yield points
//
//          //todo: work in progress as part of larger goal to type class b/h around outlier calcs
//          Validation.failureNel( new UnsupportedOperationException( s"can't support cohorts yet" ) )
//        }
//
//        case x => Validation.failureNel( new UnsupportedOperationException(s"cannot extract test context from [${x.getClass}]") )
//      }
//    }


    final case class SimpleContext private[algorithm]( message: DetectUsing, data: Seq[DoublePoint] ) extends Context {
      val algorithm: Symbol = message.algorithm
      def plan: OutlierPlan = message.plan
      def history: HistoricalStatistics = message.history
      def source: TimeSeriesBase = message.source
      def messageConfig: Config = message.properties

      def distanceMeasure: TryV[DistanceMeasure] = {
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

      def tolerance: TryV[Option[Double]] = {
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

