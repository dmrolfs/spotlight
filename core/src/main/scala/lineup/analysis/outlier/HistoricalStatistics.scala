package lineup.analysis.outlier

import java.io.Serializable
import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.{ DescriptiveStatistics, MultivariateSummaryStatistics }

import scala.annotation.tailrec
import scala.collection.immutable.Queue


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

  def movingPercentile( p: Double ): Double
  def movingAverage: Double
  def movingStandardDeviation: Double
  def movingVariance: Double
  def movingMin: Double
  def movingMax: Double
  def movingExponentialWeightedMovingAverage( centerOfMass: Int ): Double
}


object HistoricalStatistics {
  type Point = Array[Double]
  val LastN: Int = 3
  val OneHourForOnePointPerTenSeconds: Int = 360

  def apply(
    k: Int,
    isCovarianceBiasCorrected: Boolean,
    windowSize: Int = OneHourForOnePointPerTenSeconds
  ): HistoricalStatistics = {
    ApacheMath3HistoricalStatistics(
      all = new MultivariateSummaryStatistics(k, isCovarianceBiasCorrected),
      windowSize = windowSize,
      movingStatistics = new DescriptiveStatistics( windowSize )
    )
  }

  def fromActivePoints(
    points: Array[DoublePoint],
    isCovarianceBiasCorrected: Boolean,
    windowSize: Int = OneHourForOnePointPerTenSeconds
  ): HistoricalStatistics = {
    points.foldLeft( HistoricalStatistics(k = 2, isCovarianceBiasCorrected, windowSize) ) { (h, p) => h add p.getPoint }
  }


  final case class ApacheMath3HistoricalStatistics private[outlier](
    all: MultivariateSummaryStatistics,
    windowSize: Int,
    movingStatistics: DescriptiveStatistics,
    movingTimestamps: Queue[Double] = Queue.empty[Double],
    override val lastPoints: Seq[Point] = Seq.empty[Point]
  ) extends HistoricalStatistics {
    import ApacheMath3HistoricalStatistics._

    override def add( point: Point ): HistoricalStatistics = {
      all addValue point
      val ts = movingTimestamps.enqueueFinite( point(0), windowSize )
      movingStatistics addValue point( 1 )
      this.copy( movingTimestamps = ts, lastPoints = this.lastPoints.drop(this.lastPoints.size - LastN + 1) :+ point )
    }

    override def n: Long = all.getN
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

    override def movingPercentile( p: Double ): Double = movingStatistics.getPercentile( p )
    override def movingAverage: Double = movingStatistics.getMean
    override def movingStandardDeviation: Double = movingStatistics.getStandardDeviation
    override def movingVariance: Double = movingStatistics.getVariance
    override def movingMin: Double = movingStatistics.getMin
    override def movingMax: Double = movingStatistics.getMax

    override def movingExponentialWeightedMovingAverage( centerOfMass: Int ): Double = {
      val alpha = 1D / ( 1D + centerOfMass.toDouble )

      @tailrec def loop( i: Int, n: Int, acc: Vector[(Double, Double)] = Vector.empty[(Double, Double)]): Seq[Double] = {
        if ( n <= i ) acc.map{ case (den, sum) => sum / den }.toSeq
        else {
          val v = math.pow( (1D - alpha), i )
          val (lastDen, lastSum) = acc.lastOption getOrElse (0D, 0D)
          val den = lastDen + v
          val sum = lastSum + movingStatistics.getElement( i ) * v
          loop( i + 1, n, acc :+ (den, sum) )
        }
      }

//      loop( 0, movingStatistics.getN.toInt ).zipWithIndex.map { case (ewma, i) => Array( movingTimestamps(i), ewma ) }
      loop( 0, movingStatistics.getN.toInt ).last
    }

    override def toString: String = {
      s"""
         |allStatistics: [${all.toString}]
         |movingTimestamps: [${movingTimestamps.mkString(",")}]
         |movingStatistics[$windowSize]: [${movingStatistics.toString}]
         |lastPoints = [${lastPoints.map{_.mkString("(",",",")")}.mkString(",")}]
       """.stripMargin
    }

    //todo underlying serializable ops
  }

  object ApacheMath3HistoricalStatistics {
    implicit class FiniteQueue[A]( val q: Queue[A] ) extends AnyVal {
      def enqueueFinite[B >: A]( elem: B, maxSize: Int ): Queue[B] = {
        var result = q enqueue elem
        while ( result.size > maxSize ) { result = result.dequeue._2 }
        result
      }
    }
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
