package lineup.analysis.outlier

import java.io.Serializable
import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.MultivariateSummaryStatistics


/**
  * Created by rolfsd on 1/26/16.
  */
trait HistoricalStatistics extends Serializable {
  def add( point: Array[Double] ): HistoricalStatistics
  def covariance: RealMatrix
  def dimension: Int
  def geometricMean: Array[Double]
  def max: Array[Double]
  def mean: Array[Double]
  def min: Array[Double]
  def n: Long
  def standardDeviation: Array[Double]
  def sum: Array[Double]
  def sumLog: Array[Double]
  def sumOfSquares: Array[Double]
  def hashCode: Int
}


object HistoricalStatistics {
  def apply( k: Int, isCovarianceBiasCorrected: Boolean ): HistoricalStatistics = {
    ApacheMath3HistoricalStatistics( new MultivariateSummaryStatistics(k, isCovarianceBiasCorrected) )
  }

  def fromActivePoints( points: Array[DoublePoint], isCovarianceBiasCorrected: Boolean ): HistoricalStatistics = {
    points.foldLeft( HistoricalStatistics( k = 2, isCovarianceBiasCorrected ) ) { (h, p) => h add p.getPoint }
  }


  final case class ApacheMath3HistoricalStatistics private[outlier](
    underlying: MultivariateSummaryStatistics
  ) extends HistoricalStatistics {
    override def add( point: Array[Double] ): HistoricalStatistics = {
      underlying addValue point
      this
    }

    override def n: Long = underlying.getN
    override def mean: Array[Double] = underlying.getMean
    override def sumOfSquares: Array[Double] = underlying.getSumSq
    override def max: Array[Double] = underlying.getMax
    override def standardDeviation: Array[Double] = underlying.getStandardDeviation
    override def geometricMean: Array[Double] = underlying.getGeometricMean
    override def min: Array[Double] = underlying.getMin
    override def sum: Array[Double] = underlying.getSum
    override def covariance: RealMatrix = underlying.getCovariance
    override def sumLog: Array[Double] = underlying.getSumLog
    override def dimension: Int = underlying.getDimension

    //todo underlying serializable ops
  }

}