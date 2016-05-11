package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.ask
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.regression.MillerUpdatingRegression
import peds.commons.{KOp, TryV, Valid}
import peds.commons.util._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 2/25/16.
  */
object LeastSquaresAnalyzer {
  val Algorithm = Symbol( "least-squares" )

  def props( router: ActorRef ): Props = Props { new LeastSquaresAnalyzer( router ) }


  final case class Context private[skyline](
    override val underlying: AlgorithmContext,
    regression: MillerUpdatingRegression
  ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addControlBoundary( control: ControlBoundary ): That = copy(underlying = underlying.addControlBoundary(control))

    override def toString: String = s"""${getClass.safeSimpleName}(regression:[${regression}])"""
  }
}

class LeastSquaresAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[LeastSquaresAnalyzer.Context] {
  import LeastSquaresAnalyzer.Context

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = LeastSquaresAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
    makeRegression( c ) map { rm => Context( underlying = c, regression = rm ) }
  }

  def makeRegression( context: AlgorithmContext ): Valid[MillerUpdatingRegression] = {
    val config = context.messageConfig
    val k = 1 // one coefficient: ts is x-axis
    new MillerUpdatingRegression( k, true ).successNel
  }


  /**
    * A timeseries is anomalous if the average of the last three datapoints
    * on a projected least squares model is greater than three sigma.
    */

    override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      ctx <- toConcreteContextK
      tolerance <- tolerance
    } yield {
      val tol = tolerance getOrElse 3D

      val allByTimestamp = Map( groupWithLast( ctx.source.points, ctx ):_* )

      //todo: this approach seems very wrong and not working out.
      collectOutlierPoints(
        points = ctx.source.points,
        context = ctx,
        evaluateOutlier = (p: PointT, cx: Context) => {
          val (ts, v) = p
          val result = \/ fromTryCatchNonFatal {
            cx.regression.regress.getParameterEstimates
          } map { case Array(c, m) =>
            val projected = m * ts + c
            log.debug( "least squares projected:[{}] values:[{}]", projected, allByTimestamp(ts).mkString(",") )
            val errors = allByTimestamp( ts ) map { _ - projected }
            val errorsStddev = new DescriptiveStatistics( errors.toArray ).getStandardDeviation
            val t = errors.sum / errors.size
            log.debug( "least squares error[{}] errorStdDev[{}] t[{}]", errors.mkString(","), errorsStddev, t )
            log.debug( "least squares 1 avg error > tolerance: {} > {} = {}", math.abs(t), errorsStddev * tol, ( math.abs(t) > errorsStddev * tol ) )
            log.debug( "least squares 2 non-zero errors stddev: {} != {} = {}", math.round( errorsStddev ), 0D, ( math.round( errorsStddev ) != 0D ) )
            log.debug( "least squares 3 non-zero avg error: {} != {} = {}", math.round( t ), 0D, ( math.round( t ) != 0D ) )
            val control = ControlBoundary.fromExpectedAndDistance(
              timestamp = ts.toLong,
              expected = v + t,
              distance = math.abs( tol * errorsStddev )
            )
            val isOutlier = ( math.abs(t) > errorsStddev * tol ) && ( math.round( errorsStddev ) != 0D ) && ( math.round( t ) != 0 )
            ( isOutlier, control )
          }

          log.debug( "least squares [{}] = {}", p, result )
          result getOrElse ( false, ControlBoundary.empty(ts.toLong) )
        },
        update = (c: Context, p: PointT) => {
          val (ts, v) = p
          ctx.regression.addObservation( Array(ts), v )
          log.debug( "after update [{}] regression-adj r sq=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getAdjustedRSquared) )
          log.debug( "after update [{}] regression-error of sum squared=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getErrorSumSquares) )
          log.debug( "after update [{}] regression-mean sq error=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getMeanSquareError) )
          log.debug( "after update [{}] regression-N=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getN) )
          log.debug( "after update [{}] regression-# of params=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getNumberOfParameters) )
          log.debug( "after update [{}] regression-regression sum squares=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getRegressionSumSquares) )
          log.debug( "after update [{}] regression-r squared=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getRSquared) )
          log.debug( "after update [{}] regression-std error of estimates=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getStdErrorOfEstimates.mkString(",")) )
          log.debug( "after update [{}] regression-total sum squared=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.getTotalSumSquares) )
          log.debug( "after update [{}] regression-has intercept=[{}]", (ts.toLong, v), \/.fromTryCatchNonFatal(ctx.regression.regress.hasIntercept) )
          ctx
        }
      )
    }

    makeOutliersK( algorithm, outliers )
  }

  //todo: DRY wrt tailaverage logic?
  def groupWithLast( points: Seq[(Double, Double)], ctx: Context ): Seq[(Double, Seq[Double])] = {
    val data = points map { _.value }
    val last = ctx.history.lastPoints.drop( ctx.history.lastPoints.size - 2 ) map { _.value }
    log.debug( "groupWithLast: last=[{}]", last.mkString(",") )

    val TailLength = 3

    points
    .map { _.timestamp }
    .zipWithIndex
    .map { case (ts, i) =>
      val groups = if ( i < TailLength ) {
        val all = last ++ data.take( i + 1 )
        all.drop( all.size - TailLength )
      } else {
        data.drop( i - TailLength + 1 ).take( TailLength )
      }

      ( ts, groups )
    }
  }
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
