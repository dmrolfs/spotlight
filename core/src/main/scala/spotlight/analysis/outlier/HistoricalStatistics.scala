package spotlight.analysis.outlier

import java.io.Serializable
import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.MultivariateSummaryStatistics
import spotlight.model.timeseries.DataPoint


/**
  * Created by rolfsd on 1/26/16.
  */
trait HistoricalStatistics extends Serializable {
  def :+( point: Point ): HistoricalStatistics
  def recordLastPoints( points: Seq[Point] ): HistoricalStatistics
  def recordLastDataPoints( datapoints: Seq[DataPoint] ): HistoricalStatistics = {
    val points = DataPoint.toDoublePoints( datapoints ).map{ _.getPoint }
    recordLastPoints( points )
  }

  def covariance: RealMatrix
  def dimension: Int
  def geometricMean: Point
  def max: Point
  def mean: Point
  def min: Point
  def N: Long
  def standardDeviation: Point
  def sum: Point
  def sumLog: Point
  def sumOfSquares: Point
  def lastPoints: Seq[Point]
}


object HistoricalStatistics {
  val LastN: Int = 3

  def apply( k: Int, isCovarianceBiasCorrected: Boolean ): HistoricalStatistics = {
    ApacheMath3HistoricalStatistics( new MultivariateSummaryStatistics(k, isCovarianceBiasCorrected) )
  }

  def fromActivePoints( points: Array[DoublePoint], isCovarianceBiasCorrected: Boolean ): HistoricalStatistics = {
    points.foldLeft( HistoricalStatistics(k = 2, isCovarianceBiasCorrected) ) { (h, p) => h :+ p.getPoint }
  }


  final case class ApacheMath3HistoricalStatistics private[outlier](
    all: MultivariateSummaryStatistics,
    override val lastPoints: Seq[Point] = Seq.empty[Point]
  ) extends HistoricalStatistics {
    override def :+( point: Point ): HistoricalStatistics = {
      all addValue point
      this
    }

    override def recordLastPoints( points: Seq[Point] ): HistoricalStatistics = {
      val recorded = points drop ( points.size - LastN )
      this.copy( lastPoints = this.lastPoints.drop(this.lastPoints.size - LastN + recorded.size) ++ recorded )
    }

    override def N: Long = all.getN
    override def mean: Point = all.getMean
    override def sumOfSquares: Point = all.getSumSq
    override def max: Point = all.getMax
    override def standardDeviation: Point = all.getStandardDeviation
    override def geometricMean: Point = all.getGeometricMean
    override def min: Point = all.getMin
    override def sum: Point = all.getSum
    override def covariance: RealMatrix = all.getCovariance
    override def sumLog: Point = all.getSumLog
    override def dimension: Int = all.getDimension

    override def toString: String = {
      s"""
         |allStatistics: [${all.toString}]
         |lastPoints = [${lastPoints.map{_.mkString("(",",",")")}.mkString(",")}]
       """.stripMargin
    }

    //todo underlying serializable ops
  }
}


/*
http://stackoverflow.com/questions/9200874/implementing-exponential-moving-average-in-java
public LinkedList EMA(int dperiods, double alpha)
            throws IOException {
        String line;
        int i = 0;
        DescriptiveStatistics stats = new SynchronizedDescriptiveStatistics();
        stats.setWindowSize(dperiods);
        File f = new File("");
        BufferedReader in = new BufferedReader(new FileReader(f));
        LinkedList<Double> ema1 = new LinkedList<Double>();
        // Compute some statistics
        while ((line = in.readLine()) != null) {
            double sum = 0;
            double den = 0;
            System.out.println("line: " + " " + line);
            stats.addValue(Double.parseDouble(line.trim()));
            i++;
            if (i > dperiods)
                for (int j = 0; j < dperiods; j++) {
                    double var = Math.pow((1 - alpha), j);
                    den += var;
                    sum += stats.getElement(j) * var;
                    System.out.println("elements:"+stats.getElement(j));
                    System.out.println("sum:"+sum);
                }
            else
                for (int j = 0; j < i; j++) {
                    double var = Math.pow((1 - alpha), j);
                    den += var;
                    sum += stats.getElement(j) * var;
                }
            ema1.add(sum / den);
            System.out.println("EMA: " + sum / den);
        }
        return ema1;
    }
 */
