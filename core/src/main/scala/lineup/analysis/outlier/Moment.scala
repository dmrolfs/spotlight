package lineup.analysis.outlier

import scalaz._, Scalaz._
import org.joda.{ time => joda }
import peds.commons.Valid



/**
  * Created by rolfsd on 1/26/16.
  */
object Moment {
  type ID = String

  def withAlpha( id: ID, alpha: Double ): Valid[Moment] = checkAlpha( alpha ) map { a => Moment( id, alpha = a ) }
  def withCenterOfMass( id: ID, com: Double ): Valid[Moment] = withAlpha( id, 1D / ( 1D + com / 100D ) )
  def withHalfLife( id: ID, halfLife: Double ): Valid[Moment] = withAlpha( id, 1D - math.exp( math.log(0.5 ) / halfLife ) )

  def checkAlpha( alpha: Double ): Valid[Double] = {
    if ( alpha < 0D || 1D < alpha ) Validation.failureNel( InvalidMomentAlphaError( alpha ) )
    else alpha.successNel
  }


  final case class Statistics private[outlier](
    movingMax: Double = Double.NaN,
    movingMin: Double = Double.NaN,
//      movingAverage: Double = Double.NaN,
//      movingStandardDeviation: Double = Double.NaN,
    ewma: Double = Double.NaN,
    ewmsd: Double = Double.NaN
  ) {
//      def movingVariance: Double = movingStandardDeviation * movingStandardDeviation
  }


  final case class InvalidMomentAlphaError private[outlier]( alpha: Double )
  extends IllegalArgumentException( s"cannot create MomentStatistics with alpha [${alpha}] outside [0, 1]" )
}

final case class Moment private[outlier](
  id: Moment.ID,
  alpha: Double,
  statistics: Moment.Statistics = Moment.Statistics()
) {
  def centerOfMass: Double = ( 1D / alpha ) - 1D
//    def halfLife: Double

  def :+( value: Double ): Moment = {
    val updatedEWMA = (alpha * value) + (1 - alpha) * statistics.ewma
    val updatedEWMSD = math.sqrt( alpha * math.pow(statistics.ewmsd, 2) + (1 - alpha) * math.pow(value - statistics.ewma, 2) )
    copy( statistics = Moment.Statistics( ewma = updatedEWMA, ewmsd = updatedEWMSD ) )
  }
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
