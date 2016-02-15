package lineup.analysis.outlier.algorithm

import org.apache.commons.math3.ml.distance.DistanceMeasure

import scalaz._, Scalaz._
import scalaz.Kleisli.ask
import scalaz.Kleisli.{ ask, kleisli }
import akka.event.LoggingReceive
import org.apache.commons.math3.ml.clustering.{ DBSCANClusterer, Cluster, DoublePoint }
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import lineup.model.outlier.Outliers
import lineup.model.timeseries._
import lineup.analysis.outlier.{ DetectOutliersInSeries, DetectUsing }


/**
 * Created by rolfsd on 9/29/15.
 */
trait DBSCANAnalyzer extends AlgorithmActor {
  import AlgorithmActor._

  val trainingLogger = LoggerFactory getLogger "Training"


  override val detect: Receive = LoggingReceive {
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      ( algorithmContext >==> cluster >==> findOutliers( payload.source ) ).run( s ) match {
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

  def findOutliers( source: TimeSeries ): Op[(Context, Seq[Cluster[DoublePoint]]), Outliers]


  val seedEps: Op[Config, Double] = Kleisli[TryV, Config, Double] { c =>
    \/ fromTryCatchNonFatal { c getDouble algorithm.name +".seedEps" }
  }

  val minDensityConnectedPoints: Op[Config, Int] = {
    Kleisli[TryV, Config, Int] { c => \/ fromTryCatchNonFatal { c getInt algorithm.name+".minDensityConnectedPoints" } }
  }

  val eps: Op[Context, Double] = {
    val epsContext = for {
      context <- ask[TryV, Context]
      config <- messageConfig
      tol <- kleisli[TryV, Context, Option[Double]] {_.tolerance }
    } yield ( context, config, context.history, tol )

    epsContext flatMapK { case (ctx, config, hist, tol) =>
      log.debug( "eps config = {}", config )
      log.debug( "eps history = {}", hist )
      log.debug( "eps tolerance = {}", tol )

      val sd = hist.standardDeviation( 1 )
      log.debug( "eps historical std dev = {}", sd )

      val calculatedEps = for {
        stddev <- if ( sd.isNaN ) None else Some( sd )
        t <- tol
      } yield { t * stddev }

      trainingLogger.debug(
        "[{}][{}][{}] eps calculated = {}",
        ctx.message.plan.name,
        ctx.message.topic,
        hist.n.toString,
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

  val cluster: Op[Context, (Context, Seq[Cluster[DoublePoint]])] = {
    for {
      ctx <- ask[TryV, Context]
      data = ctx.data
      e <- eps
      minDensityPts <- minDensityConnectedPoints <=< messageConfig
      distance <- kleisli[TryV, Context, DistanceMeasure] {_.distanceMeasure }
    } yield {
      log.info( "cluster [{}]: eps = [{}]", ctx.message.topic, e )
      log.debug( "cluster: minDensityConnectedPoints = [{}]", minDensityPts )
      log.debug( "cluster: distanceMeasure = [{}]", distance )
      log.debug( "cluster: data = [{}]", data.mkString(",") )

      import scala.collection.JavaConverters._
      ( ctx, new DBSCANClusterer[DoublePoint]( e, minDensityPts, distance ).cluster( data.asJava ).asScala.toSeq )
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
