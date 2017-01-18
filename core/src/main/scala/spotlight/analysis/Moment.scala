package spotlight.analysis

import scalaz._
import Scalaz._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.stat.descriptive.StatisticalSummary
import peds.commons.Valid
import peds.commons.util._


/**
  * Created by rolfsd on 1/26/16.
  */
trait Moment extends Serializable {
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

  final case class Statistics private[analysis](
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
  }


  final case class SimpleMoment private[analysis](
    override val alpha: Double,
    override val statistics: Option[Moment.Statistics] = None
  ) extends Moment {
    override def centerOfMass: Double = ( 1D / alpha ) - 1D
    override def :+( value: Double ): Moment = {
      val newStatistics = statistics map { _ :+ value } getOrElse { Statistics( alpha, value ) }
      copy( statistics = Option(newStatistics) )
    }
  }


  final case class InvalidMomentAlphaError private[analysis]( alpha: Double )
  extends IllegalArgumentException( s"cannot create MomentStatistics with alpha [${alpha}] outside [0, 1]" )
}
