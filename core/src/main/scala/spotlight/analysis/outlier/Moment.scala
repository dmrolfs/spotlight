package spotlight.analysis.outlier

import scalaz._, Scalaz._
import com.typesafe.scalalogging.LazyLogging
import peds.commons.Valid
import peds.commons.util._


/**
  * Created by rolfsd on 1/26/16.
  */
trait Moment {
  def id: String
  def alpha: Double
  def centerOfMass: Double
  //    def halfLife: Double
  def statistics: Option[Moment.Statistics]

  def :+( value: Double ): Moment
}

object Moment extends LazyLogging {
  type ID = String

  def withAlpha( id: ID, alpha: Double ): Valid[Moment] = {
    checkAlpha( alpha ) map { a => SimpleMoment( id, alpha ) }
  }

  def withCenterOfMass( id: ID, com: Double ): Valid[Moment] = withAlpha( id, 1D / ( 1D + com / 100D ) )

  def withHalfLife( id: ID, halfLife: Double ): Valid[Moment] = withAlpha( id, 1D - math.exp( math.log(0.5 ) / halfLife ) )

  def checkAlpha( alpha: Double ): Valid[Double] = {
    if ( alpha < 0D || 1D < alpha ) Validation.failureNel( InvalidMomentAlphaError( alpha ) )
    else alpha.successNel
  }


  final case class Statistics private[outlier](
    n: Int = 1,
    alpha: Double,
    movingMax: Double,
    movingMin: Double,
//      movingAverage: Double = Double.NaN,
//      movingStandardDeviation: Double = Double.NaN,
    ewma: Double,
    ewmsd: Double
  ) {
    def :+( value: Double ): Statistics = {
      val newMax = math.max( movingMax, value )
      val newMin = math.min( movingMin, value )
      val newEWMA = (alpha * value) + (1 - alpha) * ewma
      val newEWMSD = math.sqrt( alpha * math.pow(ewmsd, 2) + (1 - alpha) * math.pow(value - ewma, 2) )
      this.copy( n = n+1, movingMax = newMax, movingMin = newMin, ewma = newEWMA, ewmsd = newEWMSD )
    }

    override def toString: String = {
      s"${getClass.safeSimpleName}[${n}](max:[${movingMax}] min:[${movingMin}] ewma:[${ewma}] ewmsd:[${ewmsd}] alpha:[${alpha}])"
    }
//      def movingVariance: Double = movingStandardDeviation * movingStandardDeviation
  }


  final case class SimpleMoment private[outlier](
    override val id: Moment.ID,
    override val alpha: Double,
    override val statistics: Option[Moment.Statistics] = None
  ) extends Moment {
    override def centerOfMass: Double = ( 1D / alpha ) - 1D
    override def :+( value: Double ): Moment = {
      val newStatistics = statistics map { _ :+ value } getOrElse {
        Statistics( alpha = alpha, movingMax = value, movingMin = value, ewma = value, ewmsd = 0D )
      }
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
