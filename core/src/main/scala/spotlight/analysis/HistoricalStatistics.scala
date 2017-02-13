package spotlight.analysis

import java.io.Serializable

import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.MultivariateSummaryStatistics
import spotlight.model.timeseries._

/** Created by rolfsd on 1/26/16.
  */
trait HistoricalStatistics extends Serializable {
  def :+( point: PointA ): HistoricalStatistics
  @deprecated( "replace with RecentHistory", "20161004" ) def recordLastPoints( points: Seq[PointA] ): HistoricalStatistics

  def covariance: RealMatrix
  def dimension: Int
  def geometricMean: PointA
  def max: PointA
  def mean: PointA
  def min: PointA
  def N: Long
  def standardDeviation: PointA
  def sum: PointA
  def sumLog: PointA
  def sumOfSquares: PointA
  @deprecated( "replace with RecentHistory", "20161004" ) def lastPoints: Seq[PointA]
  override def toString: String = {
    s"HistoricalStatistics(" +
      s"N:${N} " +
      s"""m:${mean.mkString( ", " )} """ +
      s"""sd:${standardDeviation.mkString( ", " )} """ +
      s"""min-max:[${min.zip( max ).mkString( ", " )}]""" +
      ")"
  }
}

object HistoricalStatistics {
  //  val LastN: Int = 6 * 60 * 24 // 6pts / sec for 1-day    // 3

  def apply( k: Int, isCovarianceBiasCorrected: Boolean ): HistoricalStatistics = {
    ApacheMath3HistoricalStatistics( new MultivariateSummaryStatistics( k, isCovarianceBiasCorrected ), RecentHistory.apply() )
  }

  def fromActivePoints( points: Seq[DoublePoint], isCovarianceBiasCorrected: Boolean ): HistoricalStatistics = {
    points.foldLeft( HistoricalStatistics( k = 2, isCovarianceBiasCorrected ) ) { ( h, p ) ⇒ h :+ p }
  }

  final case class ApacheMath3HistoricalStatistics private[analysis] (
      all: MultivariateSummaryStatistics,
      recent: RecentHistory
  ) extends HistoricalStatistics {
    override def :+( point: PointA ): HistoricalStatistics = {
      all addValue point
      this
    }

    @deprecated( "replace with RecentHistory", "20161004" ) override def recordLastPoints( points: Seq[PointA] ): HistoricalStatistics = this.copy( recent = recent withPoints points )
    @deprecated( "replace with RecentHistory", "20161004" ) override def lastPoints: Seq[PointA] = recent.points

    override def N: Long = all.getN
    override def mean: PointA = all.getMean
    override def sumOfSquares: PointA = all.getSumSq
    override def max: PointA = all.getMax
    override def standardDeviation: PointA = all.getStandardDeviation
    override def geometricMean: PointA = all.getGeometricMean
    override def min: PointA = all.getMin
    override def sum: PointA = all.getSum
    override def covariance: RealMatrix = all.getCovariance
    override def sumLog: PointA = all.getSumLog
    override def dimension: Int = all.getDimension
    //todo underlying serializable ops
  }
}

/*
http://stackoverflow.com/questions/9200874/implementing-exponential-moving-average-in-java
public LinkedList EMA(int dperiods, double alpha)
            throws IOException {
        String line;
        int i = 0;
        DescriptiveStatistics statistics = new SynchronizedDescriptiveStatistics();
        statistics.setWindowSize(dperiods);
        File f = new File("");
        BufferedReader in = new BufferedReader(new FileReader(f));
        LinkedList<Double> ema1 = new LinkedList<Double>();
        // Compute some statistics
        while ((line = in.readLine()) != null) {
            double sum = 0;
            double den = 0;
            System.out.println("line: " + " " + line);
            statistics.addValue(Double.parseDouble(line.trim()));
            i++;
            if (i > dperiods)
                for (int j = 0; j < dperiods; j++) {
                    double var = Math.pow((1 - alpha), j);
                    den += var;
                    sum += statistics.getElement(j) * var;
                    System.out.println("elements:"+statistics.getElement(j));
                    System.out.println("sum:"+sum);
                }
            else
                for (int j = 0; j < i; j++) {
                    double var = Math.pow((1 - alpha), j);
                    den += var;
                    sum += statistics.getElement(j) * var;
                }
            ema1.add(sum / den);
            System.out.println("EMA: " + sum / den);
        }
        return ema1;
    }
 */
