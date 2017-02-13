package spotlight.analysis.algorithm.skyline.adf

import org.apache.commons.math3.linear.{ MatrixUtils, RealMatrix, SingularValueDecomposition }

abstract class RidgeRegression {
  def l2Penalty: Double
  def coefficients: Array[Double]
  def standardErrors: Array[Double]

  def withL2Penalty( l2Penalty: Double ): RidgeRegression
}

object RidgeRegression {
  def apply( x: RealMatrix, ys: Array[Double] ): RidgeRegression = {
    SimpleRidgeRegression(
      X = x,
      Y = ys,
      l2Penalty = 0D,
      coefficients = Array.empty[Double],
      standardErrors = Array.empty[Double]
    )
  }

  def apply( xm: Array[Array[Double]], ys: Array[Double] ): RidgeRegression = apply( MatrixUtils.createRealMatrix( xm ), ys )

  private def diagonal( xm: RealMatrix ): Array[Double] = ( 0 until xm.getColumnDimension ).map { i ⇒ xm.getEntry( i, i ) }.toArray

  final case class SimpleRidgeRegression private[adf] (
      override val l2Penalty: Double,
      override val coefficients: Array[Double],
      override val standardErrors: Array[Double],
      X: RealMatrix,
      Y: Array[Double],
      fitted: Array[Double] = Array.empty[Double],
      residuals: Array[Double] = Array.empty[Double]
  ) extends RidgeRegression {
    val X_svd = new SingularValueDecomposition( X )

    override def withL2Penalty( l2Penalty: Double ): RidgeRegression = {
      val V = X_svd.getV
      val s = X_svd.getSingularValues map { v ⇒ v / ( v * v + l2Penalty ) }
      val U = X_svd.getU
      val S = MatrixUtils.createRealDiagonalMatrix( s )
      val Z = V.multiply( S ).multiply( U.transpose )
      val newCoefficients = Z operate Y

      val newFitted = X operate newCoefficients
      val ( newResiduals, errorVariancePrep ) = Y.zip( newFitted ).foldLeft( ( Array.empty[Double], 0D ) ) {
        case ( ( r, ev ), ( y, f ) ) ⇒
          val residual = y - f
          ( r :+ residual, ev + math.pow( residual, 2 ) )
      }
      val errorVariance = errorVariancePrep / ( X.getRowDimension - X.getColumnDimension )
      val errorVarianceMatrix = MatrixUtils.createRealIdentityMatrix( Y.size ).scalarMultiply( errorVariance )
      val coefficientsCovarianceMatrix = Z.multiply( errorVarianceMatrix ).multiply( Z.transpose )
      val newStandardErrors = diagonal( coefficientsCovarianceMatrix )
      copy(
        coefficients = newCoefficients,
        fitted = newFitted,
        residuals = newResiduals,
        standardErrors = newStandardErrors
      )
    }
  }
}
