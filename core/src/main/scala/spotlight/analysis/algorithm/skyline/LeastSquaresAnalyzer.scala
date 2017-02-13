package spotlight.analysis.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ ActorRef, Props }

import scalaz._
import Scalaz._
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.regression.MillerUpdatingRegression
import peds.commons.{ KOp, Valid }
import peds.commons.util._
import spotlight.analysis.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._

/** Created by rolfsd on 2/25/16.
  */
object LeastSquaresAnalyzer {
  val Algorithm: String = "least-squares"

  def props( router: ActorRef ): Props = Props { new LeastSquaresAnalyzer( router ) }

  final case class Context private[skyline] (
      override val underlying: AlgorithmContext,
      regression: MillerUpdatingRegression
  ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addThresholdBoundary( threshold: ThresholdBoundary ): That = {
      copy( underlying = underlying.addThresholdBoundary( threshold ) )
    }

    override def toString: String = s"""${getClass.safeSimpleName}(regression:[${regression}])"""
  }
}

class LeastSquaresAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[LeastSquaresAnalyzer.Context] {
  import LeastSquaresAnalyzer.Context

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: String = LeastSquaresAnalyzer.Algorithm

  override def wrapContext( c: AlgorithmContext ): Valid[WrappingContext] = {
    makeRegression( c ) map { rm ⇒ Context( underlying = c, regression = rm ) }
  }

  def makeRegression( context: AlgorithmContext ): Valid[MillerUpdatingRegression] = {
    val config = context.messageConfig
    val k = 1 // one coefficient: ts is x-axis
    new MillerUpdatingRegression( k, true ).successNel
  }

  /** A timeseries is anomalous if the average of the last three datapoints
    * on a projected least squares model is greater than three sigma.
    */

  override val findOutliers: KOp[AlgorithmContext, ( Outliers, AlgorithmContext )] = {
    val outliers = for {
      ctx ← toConcreteContextK
      tolerance ← tolerance
    } yield {
      val tol = tolerance getOrElse 3D

      val allByTimestamp = Map( groupWithLast( ctx.source.points, ctx ): _* )

      //todo: this approach seems very wrong and not working out.
      collectOutlierPoints(
        points = ctx.data,
        analysisContext = ctx,
        evaluateOutlier = ( p: PointT, cx: Context ) ⇒ {
        val ( ts, v ) = p
        val result = \/ fromTryCatchNonFatal {
          cx.regression.regress.getParameterEstimates
        } map {
          case Array( c, m ) ⇒
            val projected = m * ts + c
            val errors = allByTimestamp( ts ) map { _ - projected }
            val errorsStddev = new DescriptiveStatistics( errors.toArray ).getStandardDeviation
            val meanError = errors.sum / errors.size

            val threshold = ThresholdBoundary.fromExpectedAndDistance(
              timestamp = ts.toLong,
              expected = v + meanError,
              distance = math.abs( tol * errorsStddev )
            )

            val isOutlier = {
              ( math.abs( meanError ) > errorsStddev * tol ) &&
                ( math.round( errorsStddev ) != 0D ) &&
                ( math.round( meanError ) != 0 )
            }

            //            logDebug( cx.plan, cx.source, p.toDataPoint, meanError, errors, errorsStddev, threshold, isOutlier )

            ( isOutlier, threshold )
        }

        //          logDebug( cx, result, ctx.source.points.map{ _.timestamp }.contains(p.dateTime) )

        log.debug( "least squares [{}] = {}", p, result )
        result getOrElse ( false, ThresholdBoundary.empty( ts.toLong ) )
      },
        update = ( c: Context, p: PointT ) ⇒ {
        val ( ts, v ) = p
        c.regression.addObservation( Array( ts ), v )
        //          logDebug( c, p )
        c
      }
      )
    }

    makeOutliersK( outliers )
  }

  //todo: DRY wrt tailaverage logic?
  def groupWithLast( points: Seq[( Double, Double )], ctx: Context ): Seq[( Double, Seq[Double] )] = {
    val data = points map { _.value }
    val last = ctx.history.lastPoints.drop( ctx.history.lastPoints.size - 2 ) map { _.value }
    log.debug( "groupWithLast: last=[{}]", last.mkString( "," ) )

    val TailLength = 3

    points
      .map { _.timestamp }
      .zipWithIndex
      .map {
        case ( ts, i ) ⇒
          val groups = if ( i < TailLength ) {
            val all = last ++ data.take( i + 1 )
            all.drop( all.size - TailLength )
          } else {
            data.drop( i - TailLength + 1 ).take( TailLength )
          }

          ( ts, groups )
      }
  }

  //  private def logDebug( ctx: Context, p: PointT ): Unit = {
  //    val WatchedTopic = "prod.em.authz-proxy.1.proxy.p95"
  //    def acknowledge( t: Topic ): Boolean = t.name == WatchedTopic
  //
  //    if ( acknowledge(ctx.source.topic) ) {
  //      import org.slf4j.LoggerFactory
  //      import com.typesafe.scalalogging.Logger
  //
  //      val debugLogger = Logger( LoggerFactory getLogger "Debug" )
  //
  //      debugLogger.info(
  //        """
  //          |LEASTSQUARES:PT-UPDATE: [{}] algorithm:[{}] source:[{}] ctx-threshold:[{}]
  //        """.stripMargin,
  //        ctx.plan.name + ":" + WatchedTopic, algorithm, ctx.source.points.mkString(","), ctx.thresholdBoundaries
  //      )
  //    }
  //  }
  //
  //  private def logDebug( ctx: Context, result: \/[Throwable, (Boolean, ThresholdBoundary)], isAssessed: Boolean ): Unit = {
  //    val WatchedTopic = "prod.em.authz-proxy.1.proxy.p95"
  //    def acknowledge( t: Topic ): Boolean = t.name == WatchedTopic
  //
  //    if ( acknowledge(ctx.source.topic) ) {
  //      import org.slf4j.LoggerFactory
  //      import com.typesafe.scalalogging.Logger
  //
  //      val debugLogger = Logger( LoggerFactory getLogger "Debug" )
  //
  //      debugLogger.info(
  //        """
  //          |LEASTSQUARES:PT-RESULT: [{}] algorithm:[{}] is-assessed:[{}] source:[{}] result:[{}]
  //          |    LEASTSQUARES:PT-RESULT: threshold:[{}]
  //        """.stripMargin,
  //        ctx.plan.name + ":" + WatchedTopic, algorithm, isAssessed.toString, ctx.source.points.mkString(","), result,
  //        ctx.thresholdBoundaries
  //      )
  //    }
  //  }
  //
  //  private def logDebug(
  //    plan: spotlight.model.outlier.AnalysisPlan,
  //    source: TimeSeriesBase,
  //    pt: DataPoint,
  //    meanError: Double,
  //    errors: Seq[Double],
  //    stddevError: Double,
  //    threshold: ThresholdBoundary,
  //    isOutlier: Boolean
  //  ): Unit = {
  //    val WatchedTopic = "prod.em.authz-proxy.1.proxy.p95"
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
  //          |LEASTSQUARES:[{}] point:[{}] is-outlier:[{}] source:[{}]:
  //          |    LEASTSQUARES: mean-error:[{}] stddev-error: [{}] errors:[{}]
  //          |    LEASTSQUARES: threshold: [{}]
  //        """.stripMargin,
  //        plan.name + ":" + WatchedTopic, pt, isOutlier.toString, source.points.mkString(","),
  //        meanError.toString, stddevError.toString, errors.mkString(","),
  //        threshold
  //      )
  //    }
  //  }
}

// alternative where regression is found for immediate series only

//override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
//  val outliers = for {
//  context <- toConcreteContextK <=< ask[TryV, AlgorithmContext]
//  tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
//} yield {
//  val tol = tolerance getOrElse 3D
//
//  val points2D = context.data.map {_.getPoint}.map { case Array( ts, v ) => (ts, v) }
//  val allByTimestamp = Map( groupWithLast( points2D, context ): _* )
//  val regression = new MillerUpdatingRegression( 1, true )
//  val last2D = context.history.lastPoints.map { case Array( ts, v ) => (ts, v) }
//  ( last2D ++ points2D ) foreach { case (ts, v) => regression.addObservation( Array(ts), v ) }
//  val regress = \/ fromTryCatchNonFatal { regression.regress }
//  regress.foreach{ r =>
//  log.debug( "regression-adj r sq=[{}]", r.getAdjustedRSquared )
//  log.debug( "regression-error of sum squared=[{}]", r.getErrorSumSquares )
//  log.debug( "regression-mean sq error=[{}]", r.getMeanSquareError )
//  log.debug( "regression-N=[{}]", r.getN )
//  log.debug( "regression-# of params=[{}]", r.getNumberOfParameters )
//  log.debug( "regression-regression sum squares=[{}]", r.getRegressionSumSquares )
//  log.debug( "regression-r squared=[{}]", r.getRSquared )
//  log.debug( "regression-std error of estimates=[{}]", r.getStdErrorOfEstimates.mkString(",") )
//  log.debug( "regression-total sum squared=[{}]", r.getTotalSumSquares )
//  log.debug( "regression-has intercept=[{}]", r.hasIntercept )
//}
//
//  collectOutlierPoints(
//  points = points2D,
//  context = context,
//  isOutlier = (p: PointT, ctx: Context) => {
//  val (ts, v) = p
//  val result = regress map { r =>
//  val Array( c, m ) = r.getParameterEstimates
//  val projected = m * ts + c
//  log.debug( "least squares projected:[{}] values:[{}]", projected, allByTimestamp( ts ).mkString( "," ) )
//  val errors = allByTimestamp( ts ) map {_ - projected}
//  val errorsStddev = new DescriptiveStatistics( errors.toArray ).getStandardDeviation
//  val t = errors.sum / errors.size
//  log.debug( "least squares error[{}] errorStdDev[{}] t[{}]", errors.mkString( "," ), errorsStddev, t )
//  log.debug( "least squares 1: {} > {} = {}", math.abs( t ), errorsStddev * tol, ( math.abs( t ) > errorsStddev * tol ) )
//  log.debug( "least squares 2: {} != {} = {}", math.round( errorsStddev ), 0D, ( math.round( errorsStddev ) != 0D ) )
//  log.debug( "least squares 3: {} != {} = {}", math.round( t ), 0D, ( math.round( t ) != 0D ) )
//  ( math.abs( t ) > errorsStddev * tol ) && ( math.round( errorsStddev ) != 0D ) && ( math.round( t ) != 0 )
//}
//
//  result getOrElse false
//},
//  update = (ctx: Context, pt: DataPoint) => ctx
//  )
//}
//
//  makeOutliersK( algorithm, outliers )
//}
//
