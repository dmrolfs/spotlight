package spotlight.analysis.outlier.algorithm.density

import akka.event.LoggingReceive
import scalaz._, Scalaz._
import scalaz.Kleisli.{ ask, kleisli }
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.{ Cluster, DBSCANClusterer, DoublePoint }
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.slf4j.LoggerFactory
import spotlight.analysis.outlier.algorithm.AlgorithmActor
import spotlight.analysis.outlier.{ DetectOutliersInSeries, DetectUsing }
import spotlight.model.outlier.Outliers



/**
 * Created by rolfsd on 9/29/15.
 */
object DBSCANAnalyzer {
  type Clusters = Seq[Cluster[DoublePoint]]
}

trait DBSCANAnalyzer extends AlgorithmActor {
  import AlgorithmActor._

  val trainingLogger = LoggerFactory getLogger "Training"


  override val detect: Receive = LoggingReceive {
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      ( algorithmContext >=> cluster >=> findOutliers ).run( s ) match {
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

  def findOutliers: Op[(AlgorithmContext, Seq[Cluster[DoublePoint]]), Outliers]


  val minDensityConnectedPoints: Op[Config, Int] = {
    Kleisli[TryV, Config, Int] { c => \/ fromTryCatchNonFatal { c getInt algorithm.name+".minDensityConnectedPoints" } }
  }

  def historyStandardDeviation: Op[AlgorithmContext, Double] = kleisli[TryV, AlgorithmContext, Double] { context => context.history.standardDeviation( 1 ).right }

  val eps: Op[AlgorithmContext, Double] = {
    val epsContext = for {
      context <- ask[TryV, AlgorithmContext]
      config <- messageConfig
      historyStdDev <- historyStandardDeviation
      tol <- kleisli[TryV, AlgorithmContext, Option[Double]] {_.tolerance }
    } yield ( context, config, historyStdDev, tol )

    epsContext flatMapK { case (ctx, config, hsd, tol) =>
      log.debug( "eps config = {}", config )
      log.debug( "eps tolerance = {}", tol )
      log.debug( "eps historical std dev = {}", hsd )

      val calculatedEps = for {
        stddev <- if ( hsd.isNaN ) None else Some( hsd )
        t <- tol
      } yield { t * stddev }

      trainingLogger.debug(
        "[{}][{}] eps calculated = {}",
        ctx.message.plan.name,
        ctx.message.topic,
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

  val cluster: Op[AlgorithmContext, (AlgorithmContext, Seq[Cluster[DoublePoint]])] = {
    for {
      ctx <- ask[TryV, AlgorithmContext]
      data = ctx.data
      e <- eps
      minDensityPts <- minDensityConnectedPoints <=< messageConfig
      distance <- kleisli[TryV, AlgorithmContext, DistanceMeasure] {_.distanceMeasure }
    } yield {
      log.debug( "cluster [{}]: eps = [{}]", ctx.message.topic, e )
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
