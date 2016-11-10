//package spotlight.analysis.outlier.algorithm.skyline
//
//import scala.reflect.ClassTag
//import akka.actor.{ActorRef, Props}
//import scalaz._
//import Scalaz._
//import scalaz.Kleisli.kleisli
//import org.apache.commons.math3.distribution.TDistribution
//import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
//import peds.commons.{KOp, TryV, Valid}
//import peds.commons.util._
//import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
//import spotlight.analysis.outlier.algorithm.CommonAnalyzer
//import CommonAnalyzer.WrappingContext
//import spotlight.analysis.outlier.RecentHistory
//import spotlight.model.outlier.Outliers
//import spotlight.model.timeseries._
//
//
///**
//  * Created by rolfsd on 2/25/16.
//  */
//object GrubbsAnalyzer {
//  val Algorithm = 'grubbs
//
//  def props( router: ActorRef ): Props = Props { new GrubbsAnalyzer( router ) }
//
//
//  final case class Context private[skyline](
//    override val underlying: AlgorithmContext,
//    movingStatistics: DescriptiveStatistics
//  ) extends WrappingContext {
//    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel
//
//    override type That = Context
//    override def withSource( newSource: TimeSeriesBase ): That = {
//      val updated = underlying withSource newSource
//      copy( underlying = updated )
//    }
//
//    override def addThresholdBoundary( threshold: ThresholdBoundary ): That = {
//      copy( underlying = underlying.addThresholdBoundary( threshold ) )
//    }
//
//    override def toString: String = {
//      s"""${getClass.safeSimpleName}(moving-statistics:[${movingStatistics}])"""
//    }
//  }
//}
//
//class GrubbsAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[GrubbsAnalyzer.Context] {
//  import GrubbsAnalyzer.Context
//
//  type Context = GrubbsAnalyzer.Context
//
//  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )
//
//  override def algorithm: Symbol = GrubbsAnalyzer.Algorithm
//
//  override def wrapContext( ctx: AlgorithmContext ): Valid[WrappingContext] = {
//    makeStatistics( ctx ) map { movingStats => Context( underlying = ctx, movingStatistics = movingStats ) }
//  }
//
//  def makeStatistics( ctx: AlgorithmContext ): Valid[DescriptiveStatistics] = {
//    new DescriptiveStatistics( RecentHistory.LastN ).successNel
//  }
//
//  /**
//    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
//    */
//  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
//    def criticalValue( size: Long ): KOp[AlgorithmContext, Double] = {
//      kleisli[TryV, AlgorithmContext, Double] { ctx =>
//        val Alpha = 0.05  //todo drive from context's algoConfig
//      val degreesOfFreedom = math.max( size - 2, 1 ) //todo: not a great idea but for now avoiding error if size <= 2
//        \/ fromTryCatchNonFatal { new TDistribution( degreesOfFreedom ).inverseCumulativeProbability( Alpha / (2D * size) ) }
//      }
//    }
//
//    def updateContextStatistics( ctx: Context, values: Seq[Double] ): Context = {
//      log.debug( "GRUBBS: updateStats: values:[{}]", values.mkString(",") )
//      values.foldLeft( ctx ) { (c, v) => c.movingStatistics addValue v; c }
//    }
//
//    // background: http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
//    // background: http://graphpad.com/support/faqid/1598/
//    val outliers = for {
//      cctx <- toConcreteContextK
////      filled <- fillDataFromHistory( 6 * 60 ) // pts in last hour
//      taverages <- tailAverage( cctx.data )
//      ctx = updateContextStatistics( cctx, taverages.map{ _.value } )
////      filledAverages <- tailAverage( filled )
//      size = ctx.movingStatistics.getN
//      cv <- criticalValue( size )
//      tolerance <- tolerance
//    } yield {
//      val tol = tolerance getOrElse 3D
//
////      val statsData = filledAverages map { _.value }
////      val statistics = new DescriptiveStatistics( statsData.toArray
//
//      val mean = ctx.movingStatistics.getMean
//      val stddev = ctx.movingStatistics.getStandardDeviation
//      log.debug( "Skyline[Grubbs]: statistics-N:[{}] group-size:[{}] mean:[{}] stddev:[{}]", ctx.movingStatistics.getN.toString, size.toString, mean.toString, stddev.toString )
//      // zscore calculation considered in threshold expected and distance formula
////      val zScores = taverages map { case (ts, v) => ( ts, math.abs(v - mean) / stddev ) }
////      log.debug( "Skyline[Grubbs]: mean:[{}] stddev:[{}] zScores:[{}]", mean, stddev, zScores.mkString(",") )
//
//      val thresholdSquared = math.pow( cv, 2 )
//      log.debug( "Skyline[Grubbs]: threshold^2:[{}]", thresholdSquared )
//      val grubbsScore = ((size - 1) / math.sqrt(size)) * math.sqrt( thresholdSquared / (size - 2 + thresholdSquared) )
//      log.debug( "Skyline[Grubbs]: Grubbs Score:[{}] tolerance:[{}]", grubbsScore, tol )
//
//      collectOutlierPoints(
//        points = taverages,
//        analysisContext = ctx,
//        evaluateOutlier = (p: PointT, c: Context) => {
//          val threshold = ThresholdBoundary.fromExpectedAndDistance(
//            timestamp = p.timestamp.toLong,
//            expected = mean,
//            distance = tol * grubbsScore * stddev
//          )
//
////          logDebug( ctx.plan, ctx.source, filledAverages, taverages, grubbsScore, statistics, cv, tol, threshold.isOutlier(p.value), threshold )
//
//          ( threshold isOutlier p.value, threshold )
//        },
//        update = (c: Context, p: PointT) => { c }
//      )
//    }
//
//    makeOutliersK( outliers )
//  }
//
//  private def logDebug(
//    plan: spotlight.model.outlier.OutlierPlan,
//    source: TimeSeriesBase,
//    statsData: Seq[PointT],
//    assessed: Seq[PointT],
//    grubbsScore: Double,
//    stats: DescriptiveStatistics,
//    thresholdV: Double,
//    tolerance: Double,
//    isOutlier: Boolean,
//    threshold: ThresholdBoundary
//  ): Unit = {
//    val WatchedTopic = "prod-las.em.authz-proxy.1.proxy.p95"
//    def acknowledge( t: Topic ): Boolean = t.name == WatchedTopic
//
//    if ( acknowledge(source.topic) ) {
//      import org.slf4j.LoggerFactory
//      import com.typesafe.scalalogging.Logger
//
//      val debugLogger = Logger( LoggerFactory getLogger "Debug" )
//
//      debugLogger.info(
//        """
//          |GRUBBS:[{}] [{}] original points & [{}] filled points:
//          |    GRUBBS: assessed-values:[{}]
//          |    GRUBBS: grubbsScore:[{}] mean:[{}] stddev:[{}] threshold:[{}] tolerance:[{}]
//          |    GRUBBS: outlier:[{}] threshold: [{}]
//        """.stripMargin,
//        plan.name + ":" + WatchedTopic, source.points.size.toString, statsData.size.toString,
//        assessed.mkString(","),
//        grubbsScore.toString, stats.getMean.toString, stats.getStandardDeviation.toString, thresholdV.toString, tolerance.toString,
//        isOutlier.toString, threshold
//      )
//    }
//  }
//}
