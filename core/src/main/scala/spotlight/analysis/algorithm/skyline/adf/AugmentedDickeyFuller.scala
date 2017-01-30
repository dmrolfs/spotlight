package spotlight.analysis.algorithm.skyline.adf

import org.apache.commons.math3.linear.{MatrixUtils, RealMatrix, RealVector}

/**
  * Created by rolfsd on 3/6/16.
  */
abstract class AugmentedDickeyFuller {
  def statistic: Double
  def needsDiff: Boolean
  def zeroPaddedDiff: Array[Double]
}

object AugmentedDickeyFuller {
  def apply( ts: Array[Double], lag: Int ): AugmentedDickeyFuller = computeADFStatistics( ts, lag )
  def apply( ts: Array[Double] ): AugmentedDickeyFuller = apply( ts = ts, lag = math.floor( math.cbrt(ts.size - 1) ).toInt )


  val PvalueThreshold = -3.45

  private def computeADFStatistics( ts: Array[Double], lag: Int ): AugmentedDickeyFuller = {
    val y = diff( ts )
    val zeroPaddedDiff = 0D +: y
    val k = lag + 1
    val n = ts.size - 1

    val z = MatrixUtils.createRealMatrix( laggedMatrix(y, k) ) // has rows length(ts) - 1 - k + 1
    val zcol1 = z.getColumnVector( 0 ) // has length(ts) - 1 - k + 1
    val xt1 = subsetArray( ts, k - 1, n - 1 ) // ts[k:(length(ts) - 1], has length(ts) - 1 - k + 1
    val trend = sequence( k, n ) // trend k:n, has length length(ts) - 1 - k + 1

    val designMatrix = {
      if ( k > 1 ) {
        val yt1 = z.getSubMatrix( 0, ts.size - 1 - k, 1, k - 1 ) // same as z but skips first column
        // build design matrix as cbind( xt1, 1, trend, yt1 )
        val dm = MatrixUtils.createRealMatrix( ts.size - 1 - k + 1, 3 + k - 1 )
        dm.setColumn( 0, xt1 )
        dm.setColumn( 1, ones(ts.size - 1 - k + 1) )
        dm.setColumn( 2, trend )
        dm.setSubMatrix( yt1.getData, 0, 3 )
        dm
      } else {
        // build design matrix as cbind(xt1, 1, tt)
        val dm = MatrixUtils.createRealMatrix( ts.size - 1 - k + 1, 3 )
        dm.setColumn( 0, xt1 )
        dm.setColumn( 1, ones(ts.size - 1 - k + 1) )
        dm.setColumn( 2, trend )
        dm
      }
    }

    val regression = RidgeRegression( designMatrix, zcol1.toArray ).withL2Penalty( 0.0001 )
    val beta = regression.coefficients
    val sd = regression.standardErrors
    val t = beta.head / sd.head

    SimpleAugmentedDickeyFuller( statistic = t, zeroPaddedDiff = zeroPaddedDiff )
  }

  /**
    * Takes finite differences of x
    *
    * @param xs
    * @return Returns an array of length x.length-1 of
    * the first differences of x
    */
  private def diff( xs: Array[Double] ): Array[Double] = {
    xs
    .zip( xs.drop(1) )
    .map { case (x, x1) => x1 - x }
  }


  /**
    * Equivalent to matlab and python ones
    *
    * @param n
    * @return an array of doubles of length n that are
    * initialized to 1
    */
  private def ones( n: Int ): Array[Double] = Array.fill( n ){ 1D }

  /**
    * Equivalent to R's embed function
    *
    * @param x time series vector
    * @param lag number of lags, where lag=1 is the same as no lags
    * @return a matrix that has x.length - lag + 1 rows by lag columns.
    */
  private def laggedMatrix( x: Array[Double], lag: Int ): Array[Array[Double]] = {
    ( 0 until x.size - lag + 1 )
    .map { i =>
      ( 0 until lag )
      .map { j =>
        x( lag - j - 1 + i )
      }
      .toArray
    }
    .toArray
  }

  /**
    * Takes x[start] through x[end - 1]
    *
    * @param x
    * @param start
    * @param end
    * @return
    */
  private def subsetArray( x: Array[Double], start: Int, end: Int ): Array[Double] = x.take( end ).drop( start - 1 )

  /**
    * Generates a sequence of ints [start, end]
    *
    * @param start
    * @param end
    * @return
    */
  private def sequence( start: Int, end: Int ): Array[Double] = ( start to end ).map{ _.toDouble }.toArray


  final case class SimpleAugmentedDickeyFuller private[adf](
    override val statistic: Double,
    override val zeroPaddedDiff: Array[Double]
  ) extends AugmentedDickeyFuller {
    override def needsDiff: Boolean = { statistic <= AugmentedDickeyFuller.PvalueThreshold }
  }
}
