package lineup.analysis.outlier

import java.io.Serializable
import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.MultivariateSummaryStatistics


/**
  * Created by rolfsd on 1/26/16.
  */
trait HistoricalStatistics extends Serializable {
  import HistoricalStatistics.Point

  def add( point: Point ): HistoricalStatistics
  def covariance: RealMatrix
  def dimension: Int
  def geometricMean: Point
  def max: Point
  def mean: Point
  def min: Point
  def n: Long
  def standardDeviation: Point
  def sum: Point
  def sumLog: Point
  def sumOfSquares: Point
  def lastPoints: Seq[Point]
//  def hashCode: Int
}


object HistoricalStatistics {
  type Point = Array[Double]
  val LastN: Int = 3

  def apply( k: Int, isCovarianceBiasCorrected: Boolean ): HistoricalStatistics = {
    ApacheMath3HistoricalStatistics( new MultivariateSummaryStatistics(k, isCovarianceBiasCorrected) )
  }

  def fromActivePoints( points: Array[DoublePoint], isCovarianceBiasCorrected: Boolean ): HistoricalStatistics = {
    points.foldLeft( HistoricalStatistics( k = 2, isCovarianceBiasCorrected ) ) { (h, p) => h add p.getPoint }
  }


  final case class ApacheMath3HistoricalStatistics private[outlier](
    underlying: MultivariateSummaryStatistics,
    override val lastPoints: Seq[Point] = Seq.empty[Point]
  ) extends HistoricalStatistics {
    override def add( point: Point ): HistoricalStatistics = {
      underlying addValue point
      this.copy( lastPoints = this.lastPoints.drop(this.lastPoints.size - LastN + 1) :+ point )
    }

    override def n: Long = underlying.getN
    override def mean: Point = underlying.getMean
    override def sumOfSquares: Point = underlying.getSumSq
    override def max: Point = underlying.getMax
    override def standardDeviation: Point = underlying.getStandardDeviation
    override def geometricMean: Point = underlying.getGeometricMean
    override def min: Point = underlying.getMin
    override def sum: Point = underlying.getSum
    override def covariance: RealMatrix = underlying.getCovariance
    override def sumLog: Point = underlying.getSumLog
    override def dimension: Int = underlying.getDimension

    override def toString: String = {
      s"""
         |${underlying.toString}
         |lastPoints = [${lastPoints.map{_.mkString("(",",",")")}.mkString(",")}]
       """.stripMargin
    }

    //todo underlying serializable ops
  }

}