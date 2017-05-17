package spotlight.analysis.algorithm.density

import akka.event.LoggingReceive

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.instances.either._
import cats.syntax.either._

import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.{ Cluster, DBSCANClusterer, DoublePoint }
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.slf4j.LoggerFactory
import omnibus.commons.{ KOp, ErrorOr }
import spotlight.analysis.algorithm.AlgorithmActor
import spotlight.analysis.{ DetectOutliersInSeries, DetectUsing }
import spotlight.model.outlier.Outliers

/** Created by rolfsd on 9/29/15.
  */
object DBSCANAnalyzer {
  type Clusters = Seq[Cluster[DoublePoint]]
}

trait DBSCANAnalyzer extends AlgorithmActor {
  import DBSCANAnalyzer.Clusters
  import AlgorithmActor._

  val trainingLogger = LoggerFactory getLogger "Training"

  override val detect: Receive = LoggingReceive {
    case s @ DetectUsing( _, _, payload: DetectOutliersInSeries, _, _ ) ⇒ {
      val aggregator = sender()

      val start = System.currentTimeMillis()

      ( algorithmContext andThen cluster andThen findOutliers ).run( s ) match {
        case Right( r ) ⇒ {
          algorithmTimer.update( System.currentTimeMillis() - start, scala.concurrent.duration.MILLISECONDS )
          aggregator ! r
        }

        case Left( ex ) ⇒ {
          log.error(
            ex,
            "failed [{}] analysis on [{}] : [{}]",
            algorithm,
            payload.plan.name + "][" + payload.topic,
            payload.source.interval
          )
        }
      }
    }
  }

  def findOutliers: KOp[( AlgorithmContext, Clusters ), Outliers]

  val minDensityConnectedPoints: KOp[Config, Int] = {
    Kleisli[ErrorOr, Config, Int] { c ⇒ Either catchNonFatal { c getInt algorithm + ".minDensityConnectedPoints" } }
  }

  def historyMean: KOp[AlgorithmContext, Double] = Kleisli[ErrorOr, AlgorithmContext, Double] { context ⇒
    context.history.mean( 1 ).asRight
  }

  def historyStandardDeviation: KOp[AlgorithmContext, Double] = Kleisli[ErrorOr, AlgorithmContext, Double] { context ⇒
    context.history.standardDeviation( 1 ).asRight
  }

  val eps: KOp[AlgorithmContext, Double] = {
    val epsContext = for {
      context ← ask[ErrorOr, AlgorithmContext]
      config ← messageConfig
      historyMean ← historyMean
      historyStdDev ← historyStandardDeviation
      tol ← tolerance
    } yield ( context, config, historyMean, historyStdDev, tol )

    epsContext flatMapF {
      case ( ctx, config, hsm, hsd, tol ) ⇒ {
        log.debug( "eps config = {}", config )
        log.debug( "eps historical mean=[{}] stddev=[{}] tolerance=[{}]", hsm, hsd, tol )

        val calculatedEps = for {
          distanceMean ← if ( hsm.isNaN ) None else Some( hsm )
          distanceStddev ← if ( hsd.isNaN ) None else Some( hsd )
          t = tol getOrElse 1.0
        } yield {
          log.debug( "dist-mean=[{}] dist-stddev=[{}] tolerance=[{}] calc-eps=[{}]", distanceMean, distanceStddev, t, ( distanceMean + t * distanceStddev ) )
          distanceMean + t * distanceStddev
        }

        trainingLogger.debug(
          "[{}][{}] eps calculated = {}",
          ctx.message.plan.name,
          ctx.message.topic,
          calculatedEps
        )

        Either catchNonFatal {
          calculatedEps getOrElse {
            log.debug( "eps seed = {}", config getDouble algorithm + ".seedEps" )
            config getDouble algorithm + ".seedEps"
          }
        }
      }
    }
  }

  val cluster: KOp[AlgorithmContext, ( AlgorithmContext, Clusters )] = {
    for {
      ctx ← ask[ErrorOr, AlgorithmContext]
      e ← eps
      minDensityPts ← messageConfig andThen minDensityConnectedPoints
      distance ← Kleisli[ErrorOr, AlgorithmContext, DistanceMeasure] { _.distanceMeasure }
    } yield {
      val data = ctx.data

      log.debug( "cluster [{}]: eps = [{}]", ctx.message.topic, e )
      log.debug( "cluster: minDensityConnectedPoints = [{}]", minDensityPts )
      log.debug( "cluster: distanceMeasure = [{}]", distance )
      log.debug( "cluster: data[{}] = [{}]", data.size, data.mkString( "," ) )

      import scala.collection.JavaConverters._
      val clustersD = Either catchNonFatal {
        new DBSCANClusterer[DoublePoint]( e, minDensityPts, distance ).cluster( data.asJava ).asScala.toSeq
      }

      if ( log.isDebugEnabled ) {
        val sizeClusters = clustersD.map { clusters ⇒
          clusters map { c ⇒ ( c.getPoints.size, c.getPoints.asScala.mkString( "[", ", ", "]" ) ) }
        }
        log.debug( "cluster: clusters = [{}]", sizeClusters.map { _.mkString( "\n + [", ", ", "]\n" ) } )
      }

      ( ctx, clustersD valueOr { _ ⇒ Seq.empty[Cluster[DoublePoint]] } )
    }
  }

  def makeOutlierTest( clusters: Clusters ): DoublePoint ⇒ Boolean = {
    import scala.collection.JavaConverters._

    if ( clusters.isEmpty ) ( pt: DoublePoint ) ⇒ { false }
    else {
      val largestCluster = {
        clusters
          .maxBy { _.getPoints.size }
          .getPoints
          .asScala
          .toSet
      }

      ( pt: DoublePoint ) ⇒ { !largestCluster.contains( pt ) }
    }
  }
}
