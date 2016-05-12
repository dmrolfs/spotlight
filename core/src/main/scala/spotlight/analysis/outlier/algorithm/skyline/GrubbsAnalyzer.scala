package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.{KOp, TryV, Valid}
import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 2/25/16.
  */
object GrubbsAnalyzer {
  val Algorithm = 'grubbs

  def props( router: ActorRef ): Props = Props { new GrubbsAnalyzer( router ) }
}

class GrubbsAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[CommonAnalyzer.SimpleWrappingContext] {
  import CommonAnalyzer.SimpleWrappingContext

  type Context = SimpleWrappingContext

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = GrubbsAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = ( SimpleWrappingContext( c ) ).successNel


  /**
    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
    */
  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    def criticalValue( data: Seq[PointT] ) = kleisli[TryV, AlgorithmContext, Double] { ctx =>
      val Alpha = 0.05  //todo drive from context's algoConfig
      val degreesOfFreedom = math.max( data.size - 2, 1 ) //todo: not a great idea but for now avoiding error if size <= 2
      \/ fromTryCatchNonFatal {
        new TDistribution( degreesOfFreedom ).inverseCumulativeProbability( Alpha / (2D * data.size) )
      }
    }

    // background: http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
    // background: http://graphpad.com/support/faqid/1598/
    val outliers = for {
      ctx <- toConcreteContextK
      filled <- fillDataFromHistory( 6 * 60 ) // pts in last hour
      taverages <- tailAverage( ctx.data )
      filledAverages <- tailAverage( filled )
      cv <- criticalValue( filledAverages )
      tolerance <- tolerance
    } yield {
      val tol = tolerance getOrElse 3D

      val statsData = filledAverages map { _.value }
      val stats = new DescriptiveStatistics( statsData.toArray )
      val mean = stats.getMean
      val stddev = stats.getStandardDeviation
      // zscore calculation considered in threshold expected and distance formula
//      val zScores = taverages map { case (ts, v) => ( ts, math.abs(v - mean) / stddev ) }
//      log.debug( "Skyline[Grubbs]: mean:[{}] stddev:[{}] zScores:[{}]", mean, stddev, zScores.mkString(",") )

      val thresholdSquared = math.pow( cv, 2 )
      log.debug( "Skyline[Grubbs]: threshold^2:[{}]", thresholdSquared )
      val grubbsScore = {
        ((statsData.size - 1) / math.sqrt(statsData.size)) * math.sqrt( thresholdSquared / (statsData.size - 2 + thresholdSquared) )
      }
      log.debug( "Skyline[Grubbs]: Grubbs Score:[{}] tolerance:[{}]", grubbsScore, tol )

      collectOutlierPoints(
        points = taverages,
        analysisContext = ctx,
        evaluateOutlier = (p: PointT, c: Context) => {
          val threshold = ThresholdBoundary.fromExpectedAndDistance(
            timestamp = p.timestamp.toLong,
            expected = mean,
            distance = tol * grubbsScore * stddev
          )

//          logDebug( ctx.plan, ctx.source, filledAverages, taverages, grubbsScore, stats, cv, tol, threshold.isOutlier(p.value), threshold )

          ( threshold isOutlier p.value, threshold )
        },
        update = (c: Context, p: PointT) => { c }
      )
    }

    makeOutliersK( outliers )
  }

  private def logDebug(
    plan: spotlight.model.outlier.OutlierPlan,
    source: TimeSeriesBase,
    statsData: Seq[PointT],
    assessed: Seq[PointT],
    grubbsScore: Double,
    stats: DescriptiveStatistics,
    thresholdV: Double,
    tolerance: Double,
    isOutlier: Boolean,
    threshold: ThresholdBoundary
  ): Unit = {
    val WatchedTopic = "prod.em.authz-proxy.1.proxy.p95"
    def acknowledge( t: Topic ): Boolean = t.name == WatchedTopic

    if ( acknowledge(source.topic) ) {
      import org.slf4j.LoggerFactory
      import com.typesafe.scalalogging.Logger

      val debugLogger = Logger( LoggerFactory getLogger "Debug" )

      debugLogger.info(
        """
          |GRUBBS:[{}] [{}] original points & [{}] filled points:
          |    GRUBBS: assessed-values:[{}]
          |    GRUBBS: grubbsScore:[{}] mean:[{}] stddev:[{}] threshold:[{}] tolerance:[{}]
          |    GRUBBS: outlier:[{}] threshold: [{}]
        """.stripMargin,
        plan.name + ":" + WatchedTopic, source.points.size.toString, statsData.size.toString,
        assessed.mkString(","),
        grubbsScore.toString, stats.getMean.toString, stats.getStandardDeviation.toString, thresholdV.toString, tolerance.toString,
        isOutlier.toString, threshold
      )
    }
  }
}
