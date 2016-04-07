//package spotlight.analysis.outlier.algorithm.density
//
//import scala.reflect.ClassTag
//import akka.actor.{ActorRef, Props}
//
//import scalaz._
//import Scalaz._
//import scalaz.Kleisli.ask
//import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
//import peds.commons.Valid
//import peds.commons.util._
//import spotlight.analysis.outlier.Moment
//import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, TryV}
//import spotlight.analysis.outlier.algorithm.CommonAnalyzer
//import CommonAnalyzer.WrappingContext
//import org.apache.commons.math3.ml.clustering.{Cluster, DoublePoint}
//import spotlight.model.outlier.Outliers
//import spotlight.model.timeseries.{ControlBoundary, Point2D, TimeSeriesBase}
//
//
///**
//  * Created by rolfsd on 2/25/16.
//  */
//object SeriesDensityAnalyzer2 {
//  val Algorithm = 'dbscanSeries
//
//  def props( router: ActorRef, alphaValue: Double = 0.05 ): Props = {
//    Props {
//      new SeriesDensityAnalyzer2( router ) with HistoryProvider {
//        override val alpha: Double = alphaValue
//      }
//    }
//  }
//
//
//  final case class Context private[density](
//    override val underlying: AlgorithmContext,
//    movingStatistics: Moment
//  ) extends WrappingContext {
//    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel
//
//    override type That = Context
//    override def withSource( newSource: TimeSeriesBase ): That = {
//      val updated = underlying withSource newSource
//      copy( underlying = updated )
//    }
//
//    override def addControlBoundary( control: ControlBoundary ): That = copy(underlying = underlying.addControlBoundary(control))
//
//    override def toString: String = {
//      s"""${getClass.safeSimpleName}(moving-stats:[${movingStatistics}])"""
//    }
//  }
//
//
//  trait HistoryProvider {
//    def alpha: Double
//  }
//}
//
//class SeriesDensityAnalyzer2( override val router: ActorRef ) extends CommonAnalyzer[SeriesDensityAnalyzer2.Context] {
//  outer: SeriesDensityAnalyzer2.HistoryProvider =>
//
//  import SeriesDensityAnalyzer2.Context
//
//  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )
//
//  override val algorithm: Symbol = SeriesDensityAnalyzer2.Algorithm
//
//  override def makeSkylineContext( c: AlgorithmContext ): Valid[WrappingContext] = {
//    makeMovingStatistics( c ) map { movingStats => Context( underlying = c, movingStatistics = movingStats ) }
//  }
//
//  def makeMovingStatistics( context: AlgorithmContext ): Valid[Moment] = {
//    Moment.withAlpha( id = context.historyKey.toString, alpha = outer.alpha )
//  }
//
//
//  type Clusters = Seq[Cluster[DoublePoint]]
//  val cluster: Op[AlgorithmContext, (Clusters, AlgorithmContext)] = ???
//
//  val filterOutliers: Op[(Clusters, AlgorithmContext), (Outliers, AlgorithmContext)] = {
//    val outliers = for {
//      context <- toConcreteContextK <=< ask[TryV, AlgorithmContext]
//      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
//    } yield {
//      val tol = tolerance getOrElse 3D
//
//      collectOutlierPoints(
//        points = taverages,
//        context = context,
//        evaluateOutlier = (p: Point2D, ctx: Context) => {
//          val (ts, v) = p
//          val mean = ctx.movingStatistics.getMean
//          val stddev = ctx.movingStatistics.getStandardDeviation
//          log.debug(
//            "Stddev from simple moving Average N[{}]: mean[{}]\tstdev[{}]\ttolerance[{}]",
//            ctx.movingStatistics.getN, mean, stddev, tol
//          )
//          val control = ControlBoundary.fromExpectedAndDistance(
//            timestamp = ts.toLong,
//            expected = mean,
//            distance = tol * stddev
//          )
//          ( control isOutlier v, control )
//          //          math.abs( v - mean ) > ( tol * stddev)
//        },
//        update = (ctx: Context, pt: Point2D) => {
//          val (_, v) = pt
//          ctx.movingStatistics addValue v
//          ctx
//        }
//      )
//    }
//
//    makeOutliersK( algorithm, outliers )
//  }
//
//}
//
//  /**
//    */
//  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = cluster >=> filterOutliers
//
//}
