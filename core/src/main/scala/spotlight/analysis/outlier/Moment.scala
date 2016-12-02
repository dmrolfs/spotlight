package spotlight.analysis.outlier

import scalaz._
import Scalaz._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.stat.descriptive.StatisticalSummary
import peds.commons.Valid
import peds.commons.util._


/**
  * Created by rolfsd on 1/26/16.
  */
trait Moment {
  def alpha: Double
  def centerOfMass: Double
  //    def halfLife: Double
  def statistics: Option[Moment.Statistics]

  def :+( value: Double ): Moment
}

object Moment extends LazyLogging {
  def withAlpha( alpha: Double ): Valid[Moment] = {
    checkAlpha( alpha ) map { a => SimpleMoment( alpha ) }
  }

  def withCenterOfMass( com: Double ): Valid[Moment] = withAlpha( 1D / ( 1D + com / 100D ) )

  def withHalfLife( halfLife: Double ): Valid[Moment] = withAlpha( 1D - math.exp( math.log(0.5 ) / halfLife ) )

  def checkAlpha( alpha: Double ): Valid[Double] = {
    if ( alpha < 0D || 1D < alpha ) Validation.failureNel( InvalidMomentAlphaError( alpha ) )
    else alpha.successNel
  }


  object Statistics {
    def apply( alpha: Double, values: Double* ): Statistics = {
      values.foldLeft(
        Statistics(alpha = alpha, N = 0, sum = 0D, movingMin = 0D, movingMax = 0D, ewma = 0D, ewmsd = 0D)
      ) {
        _ :+ _
      }
    }
  }

  final case class Statistics private[outlier](
    N: Long = 1,
    alpha: Double,
    sum: Double,
    movingMax: Double,
    movingMin: Double,
    ewma: Double,
    ewmsd: Double
  ) extends StatisticalSummary {
    override def getSum: Double = N * ewma
    override def getMin: Double = movingMax
    override def getStandardDeviation: Double = ewmsd
    override def getMean: Double = ewma
    override def getMax: Double = movingMax
    override def getN: Long = N
    override def getVariance: Double = ewmsd * ewmsd

    def :+( value: Double ): Statistics = {
      val newSum = this.sum + value
      val newMax = math.max( this.movingMax, value )
      val newMin = math.min( this.movingMin, value )
      val newEWMA = (this.alpha * value) + (1 - this.alpha) * this.ewma
      val newEWMSD = math.sqrt( this.alpha * math.pow(this.ewmsd, 2) + (1 - this.alpha) * math.pow(value - this.ewma, 2) )
      this.copy( N = this.N + 1, sum = newSum, movingMax = newMax, movingMin = newMin, ewma = newEWMA, ewmsd = newEWMSD )
    }

    override def toString: String = {
      s"${getClass.safeSimpleName}[${N}](max:[${movingMax}] min:[${movingMin}] ewma:[${ewma}] ewmsd:[${ewmsd}] alpha:[${alpha}])"
    }
//      def movingVariance: Double = movingStandardDeviation * movingStandardDeviation
  }


  final case class SimpleMoment private[outlier](
    override val alpha: Double,
    override val statistics: Option[Moment.Statistics] = None
  ) extends Moment {
    override def centerOfMass: Double = ( 1D / alpha ) - 1D
    override def :+( value: Double ): Moment = {
      val newStatistics = statistics map { _ :+ value } getOrElse { Statistics( alpha, value ) }
      copy( statistics = Option(newStatistics) )
    }
  }


  final case class InvalidMomentAlphaError private[outlier]( alpha: Double )
  extends IllegalArgumentException( s"cannot create MomentStatistics with alpha [${alpha}] outside [0, 1]" )
}





//    override def movingExponentialWeightedMovingAverage( centerOfMass: Int ): Double = {
//      val alpha = 1D / ( 1D + centerOfMass.toDouble )
//
//      @tailrec def loop( i: Int, n: Int, acc: Vector[(Double, Double)] = Vector.empty[(Double, Double)]): Seq[Double] = {
//        if ( n <= i ) acc.map{ case (den, sum) => sum / den }.toSeq
//        else {
//          val v = math.pow( (1D - alpha), i )
//          val (lastDen, lastSum) = acc.lastOption getOrElse (0D, 0D)
//          val den = lastDen + v
//          val sum = lastSum + movingStatistics.getElement( i ) * v
//          loop( i + 1, n, acc :+ (den, sum) )
//        }
//      }
//
////      loop( 0, movingStatistics.getN.toInt ).zipWithIndex.map { case (ewma, i) => Array( movingTimestamps(i), ewma ) }
//      loop( 0, movingStatistics.getN.toInt ).last
//    }

//  object ApacheMath3HistoricalStatistics {
//    implicit class FiniteQueue[A]( val q: Queue[A] ) extends AnyVal {
//      def enqueueFinite[B >: A]( elem: B, maxSize: Int ): Queue[B] = {
//        var result = q enqueue elem
//        while ( result.size > maxSize ) { result = result.dequeue._2 }
//        result
//      }
//    }
//  }



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
