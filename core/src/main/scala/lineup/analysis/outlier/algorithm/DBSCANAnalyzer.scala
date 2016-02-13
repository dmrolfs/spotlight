package lineup.analysis.outlier.algorithm

import scalaz._, Scalaz._
import akka.event.LoggingReceive
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.commons.math3.ml.clustering.{ DBSCANClusterer, Cluster, DoublePoint }
import org.apache.commons.math3.ml.distance.EuclideanDistance
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import peds.commons.math.MahalanobisDistance
import lineup.model.outlier.{ OutlierPlan, Outliers }
import lineup.model.timeseries._
import lineup.analysis.outlier.{ DetectOutliersInSeries, DetectUsing, HistoricalStatistics }


/**
 * Created by rolfsd on 9/29/15.
 */
object DBSCANAnalyzer {
  case class AnalyzerContext( message: DetectUsing, data: Seq[DoublePoint] )
}

trait DBSCANAnalyzer extends AlgorithmActor {
  import DBSCANAnalyzer._

  val trainingLogger = LoggerFactory getLogger "Training"

  type TryV[T] = \/[Throwable, T]
  type Op[I, O] = Kleisli[TryV, I, O]
  type Clusters = Seq[Cluster[DoublePoint]]


  override val detect: Receive = LoggingReceive {
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      ( analyzerContext >==> cluster >==> findOutliers( payload.source ) ).run( s ) match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => {
          log.error(
            ex,
            "failed [{}] analysis on [{}] : [{}]",
            algorithm.name,
            payload.plan.name + "][" + payload.topic,
            payload.source.interval
          )
        }
      }
    }
  }

  def findOutliers( source: TimeSeries ): Op[(AnalyzerContext, Seq[Cluster[DoublePoint]]), Outliers]


  def identityK[T]: Op[T, T] = Kleisli[TryV, T, T] { _.right }

  val plan: Op[AnalyzerContext, OutlierPlan] = Kleisli[TryV, AnalyzerContext, OutlierPlan] { ctx => ctx.message.plan.right }

  val history: Op[AnalyzerContext, Option[HistoricalStatistics]] = {
    Kleisli[TryV, AnalyzerContext, Option[HistoricalStatistics]] { _.message.history.right }
  }

  val tolerance: Op[Config, Option[Double]] = Kleisli[TryV, Config, Option[Double]] { c =>
    \/ fromTryCatchNonFatal {
      if ( c hasPath algorithm.name+".tolerance" ) Some( c getDouble algorithm.name+".tolerance" ) else None
    }
  }

  val seedEps: Op[Config, Double] = Kleisli[TryV, Config, Double] { c =>
    \/ fromTryCatchNonFatal { c getDouble algorithm.name +".seedEps" }
  }

  val minDensityConnectedPoints: Op[Config, Int] = {
    Kleisli[TryV, Config, Int] { c => \/ fromTryCatchNonFatal { c getInt algorithm.name+".minDensityConnectedPoints" } }
  }

  val analyzerContext: Op[DetectUsing, AnalyzerContext] = {
    Kleisli[TryV, DetectUsing, AnalyzerContext] { d =>
      val points: TryV[Seq[DoublePoint]] = d.payload.source match {
        case s: TimeSeries => DataPoint.toDoublePoints( s.points ).right[Throwable]

        case c: TimeSeriesCohort => {
//          def cohortDistances( cohort: TimeSeriesCohort ): Matrix[DataPoint] = {
//            for {
//              frame <- cohort.toMatrix
//              timestamp = frame.head._1 // all of frame's _1 better be the same!!!
//              frameValues = frame map { _._2 }
//              stats = new DescriptiveStatistics( frameValues.toArray )
//              median = stats.getPercentile( 50 )
//            } yield frameValues map { v => DataPoint( timestamp, v - median ) }
//          }
//
//          val outlierMarks: Row[Row[DoublePoint]] = for {
//            frameDistances <- cohortDistances( c )
//            points = frameDistances map { DataPoint.toDoublePoint }
//          } yield points

          //todo: work in progress as part of larger goal to type class b/h around outlier calcs
          -\/( new UnsupportedOperationException( s"can't support cohorts yet" ) )
        }

        case x => -\/( new UnsupportedOperationException( s"cannot extract test context from [${x.getClass}]" ) )
      }

      points map { pts => AnalyzerContext( data = pts, message = d ) }
    }
  }

  val source: Op[AnalyzerContext, TimeSeriesBase] = Kleisli[TryV, AnalyzerContext, TimeSeriesBase] { _.message.source.right }

  val payload: Op[AnalyzerContext, Seq[DoublePoint]] = {
    Kleisli[TryV, AnalyzerContext, Seq[DoublePoint]] { case AnalyzerContext(_, payload ) => payload.right }
  }

  val messageConfig: Op[AnalyzerContext, Config] = {
    Kleisli[TryV, AnalyzerContext, Config] { case AnalyzerContext(message, _ ) => message.properties.right }
  }

  val distanceMeasure: Op[AnalyzerContext, DistanceMeasure] = {
    Kleisli[TryV, AnalyzerContext, DistanceMeasure] { case AnalyzerContext( message, data ) =>
      def makeMahalanobisDistance: TryV[DistanceMeasure] = {
        val mahal = message.history map { h =>
          MahalanobisDistance.fromCovariance( h.covariance )
        } getOrElse {
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
  }

  val eps: Op[AnalyzerContext, Double] = {
    val epsContext = for {
      ctx <- identityK[AnalyzerContext]
      config <- messageConfig
      hist <- history
      tol <- tolerance <=< messageConfig
    } yield ( ctx, config, hist, tol )

    epsContext flatMapK { case (ctx, config, hist, tol) =>
      log.debug( "eps config = {}", config )
      log.debug( "eps history = {}", hist )
      log.debug( "eps tolerance = {}", tol )
      val calculatedEps = for {
        h <- hist
        sd = h.standardDeviation( 1 )
        _ = log.debug( "eps historical std dev = {}", sd )
        stddev <- if ( sd.isNaN ) None else Some( sd )
        t <- tol
      } yield { t * stddev }

      trainingLogger.debug(
        "[{}][{}][{}] eps calculated = {}",
        ctx.message.plan.name,
        ctx.message.topic, hist.map{ _.n },
        calculatedEps
      )

      \/ fromTryCatchNonFatal {
        calculatedEps getOrElse {
          log.debug( "eps seed = {}", config getDouble algorithm.name+".seedEps" )
          config getDouble algorithm.name+".seedEps"
        }
      }
    }
  }

  val cluster: Op[AnalyzerContext, (AnalyzerContext, Seq[Cluster[DoublePoint]])] = {
    for {
      ctx <- identityK[AnalyzerContext]
      payload <- payload
      e <- eps
      minDensityPts <- minDensityConnectedPoints <=< messageConfig
      distance <- distanceMeasure
    } yield {
      log.info( "cluster [{}]: eps = [{}]", ctx.message.topic, e )
      log.debug( "cluster: minDensityConnectedPoints = [{}]", minDensityPts )
      log.debug( "cluster: distanceMeasure = [{}]", distance )
      log.debug( "cluster: payload = [{}]", payload.mkString(",") )

      import scala.collection.JavaConverters._
      ( ctx, new DBSCANClusterer[DoublePoint]( e, minDensityPts, distance ).cluster( payload.asJava ).asScala.toSeq )
    }
  }

  def makeOutlierTest( clusters: Seq[Cluster[DoublePoint]] ): DoublePoint => Boolean = {
    import scala.collection.JavaConverters._

    if ( clusters.isEmpty ) (pt: DoublePoint) => { false }
    else {
      val largestCluster = {
        clusters
        .maxBy { _.getPoints.size }
        .getPoints
        .asScala
        .toSet
      }

      ( pt: DoublePoint ) => { !largestCluster.contains( pt ) }
    }
  }
}
