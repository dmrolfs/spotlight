package spotlight.analysis.outlier.algorithm.skyline.adf

import scala.annotation.tailrec
import org.apache.commons.math3.linear.{ MatrixUtils, RealMatrix, SingularValueDecomposition }
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics


abstract class RPCA {
  def L: RealMatrix
  def S: RealMatrix
  def E: RealMatrix
}

object RPCA {
  def apply( data: RealMatrix, lPenalty: Double, sPenalty: Double ): RPCA = computeRSVD( data, lPenalty, sPenalty )

  def apply( data: Array[Array[Double]], lPenalty: Double, sPenalty: Double ): RPCA = {
    apply( MatrixUtils.createRealMatrix( data ), lPenalty, sPenalty )
  }

  def empty( rowDimension: Int, columnDimension: Int ): RPCA = {
    val l = MatrixUtils.createRealMatrix( rowDimension, columnDimension )
    val s = MatrixUtils.createRealMatrix( rowDimension, columnDimension )
    val e = MatrixUtils.createRealMatrix( rowDimension, columnDimension )
    SimpleRPCA( l, s, e )
  }


  private def computeRSVD( xm: RealMatrix, lPenalty: Double, sPenalty: Double ): RPCA = {
    val mu = xm.getColumnDimension * xm.getRowDimension / ( 4D * l1norm(xm.getData) )
    val objPrev = 0.5 * math.pow( xm.getFrobeniusNorm, 2 )
    val obj = objPrev
    val tol = 1e-8 * objPrev
    val diff = 2D * tol

    val MaxIters: Int = 228

    @tailrec def loop( iter: Int, mu: Double, obj: Double, objPrev: Double, diff: Double, acc: RPCA ): RPCA = {
      if ( diff <= tol || iter >= MaxIters ) acc
      else {
        val (sm, nuclearNorm) = computeS( xm, acc.L, sPenalty )( mu )
        val (lm, l1Norm) = computeL( xm, sm, lPenalty )( mu )
        val (em, l2Norm) = computeE( xm, lm, sm )
        loop(
          iter = iter + 1,
          mu = computeDynamicMu( em ),
          obj = computeObjective( nuclearNorm, l1Norm, l2Norm ),
          objPrev = obj,
          diff = math.abs( objPrev - obj ),
          acc = SimpleRPCA( lm, sm, em )
        )
      }
    }

    loop(
      iter = 0,
      mu = mu,
      obj = obj,
      objPrev = objPrev,
      diff = diff,
      acc = empty( xm.getRowDimension, xm.getColumnDimension )
    )
  }

  private def l1norm( x: Array[Array[Double]] ): Double = x.flatten.foldLeft( 0D ){ _ + math.abs( _ ) }

  private def softThreshold( xs: Array[Double], penalty: Double ): Array[Double] = {
    xs map { x => math.signum( x ) * math.max( math.abs(x) - penalty, 0 ) }
  }

  private def softThreshold( xxs: Array[Array[Double]], penalty: Double ): Array[Array[Double]] = {
    xxs map { softThreshold( _, penalty ) }
  }

  private def sum( xs: Array[Double] ): Double = xs.sum

  private def computeL( xm: RealMatrix, sm: RealMatrix, lpenalty: Double )( mu: Double ): (RealMatrix, Double) = {
    val LPenalty = lpenalty * mu
    val svd = new SingularValueDecomposition( xm subtract sm )
    val penalizedD = softThreshold( svd.getSingularValues, LPenalty )
    val D_matrix = MatrixUtils createRealDiagonalMatrix penalizedD
    val l = svd.getU.multiply( D_matrix ).multiply( svd.getVT )
    ( l, penalizedD.sum * LPenalty )
  }

  private def computeS( xm: RealMatrix, lm: RealMatrix, spenalty: Double )( mu: Double ): (RealMatrix, Double) = {
    val SPenalty = spenalty * mu
    val penalizedS = softThreshold( xm.subtract(lm).getData, SPenalty )
    val s = MatrixUtils createRealMatrix penalizedS
    ( s, l1norm(penalizedS) * SPenalty )
  }

  private def computeE( xm: RealMatrix, lm: RealMatrix, sm: RealMatrix ): (RealMatrix, Double) = {
    val e = xm.subtract( lm ).subtract( sm )
    val norm = e.getFrobeniusNorm
    ( e, math.pow(norm, 2) )
  }

  private def computeObjective( nuclearNorm: Double, l1norm: Double, l2norm: Double ): Double = {
    0.5 * l2norm + nuclearNorm + l1norm
  }

  private def computeDynamicMu( em: RealMatrix ): Double = {
    val m = em.getRowDimension
    val n = em.getColumnDimension
    val E_sd = standardDeviation( em.getData )
    val mu = E_sd * math.sqrt( 2 * math.max(m, n) )
    math.max( 0.01, mu )
  }

  private def standardDeviation( xs: Array[Array[Double]] ): Double = {
    val stats = xs.flatten.foldLeft( new DescriptiveStatistics ){ (s, v) => s.addValue( v ); s }
    stats.getStandardDeviation
  }


  final case class SimpleRPCA private[adf](
    override val L: RealMatrix,
    override val S: RealMatrix,
    override val E: RealMatrix
  ) extends RPCA
}
