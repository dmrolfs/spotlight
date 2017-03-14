package spotlight.analysis

import scalaz._
import Scalaz._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import com.persist.logging._
import org.apache.commons.math3.stat.descriptive.StatisticalSummary
import omnibus.commons.Valid
import omnibus.commons.util._
import spotlight.analysis.algorithm.Advancing
import spotlight.analysis.algorithm.AlgorithmProtocol.Advanced

/** Created by rolfsd on 1/26/16.
  */
sealed trait Moment extends Serializable {
  def alpha: Double
  def centerOfMass: Double
  //    def halfLife: Double
  def statistics: Option[Moment.Statistics]

  def :+( value: Double ): Moment
}

object Moment extends ClassLogging {
  def withAlpha( alpha: Double ): Valid[Moment] = {
    checkAlpha( alpha ) map { a ⇒ SimpleMoment( alpha ) }
  }

  def withCenterOfMass( com: Double ): Valid[Moment] = withAlpha( 1D / ( 1D + com / 100D ) )

  def withHalfLife( halfLife: Double ): Valid[Moment] = withAlpha( 1D - math.exp( math.log( 0.5 ) / halfLife ) )

  def checkAlpha( alpha: Double ): Valid[Double] = {
    if ( alpha < 0D || 1D < alpha ) Validation.failureNel( InvalidMomentAlphaError( alpha ) )
    else alpha.successNel
  }

  implicit val advancing: Advancing[Moment] = new Advancing[Moment] {
    val AlphaPath = "alpha"

    override def zero( configuration: Option[Config] ): Moment = {
      val alpha = getFromOrElse[Double]( configuration, AlphaPath, 0.05 )
      Moment.withAlpha( alpha ).disjunction match {
        case \/-( m ) ⇒ m
        case -\/( exs ) ⇒ {
          exs foreach { ex ⇒ log.error( "failed to create moment shape", ex ) }
          throw exs.head
        }
      }
    }

    override def N( shape: Moment ): Long = shape.statistics.map { _.N } getOrElse 0L

    override def advance( original: Moment, advanced: Advanced ): Moment = original :+ advanced.point.value

    override def copy( shape: Moment ): Moment = shape
  }

  object Statistics {
    def apply( alpha: Double, values: Double* ): Statistics = {
      values.foldLeft(
        Statistics( alpha = alpha, N = 0, sum = 0.0, movingMin = 0.0, movingMax = 0.0, ewma = 0.0, ewmsd = 0.0 )
      ) {
          _ :+ _
        }
    }
  }

  final case class Statistics private[analysis] (
      N: Long = 1L,
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
      val newEWMA = ( this.alpha * value ) + ( 1 - this.alpha ) * this.ewma
      val newEWMSD = math.sqrt( this.alpha * math.pow( this.ewmsd, 2 ) + ( 1 - this.alpha ) * math.pow( value - this.ewma, 2 ) )
      this.copy( N = this.N + 1, sum = newSum, movingMax = newMax, movingMin = newMin, ewma = newEWMA, ewmsd = newEWMSD )
    }

    override def toString: String = {
      s"${getClass.safeSimpleName}[${N}](max:[${movingMax}] min:[${movingMin}] ewma:[${ewma}] ewmsd:[${ewmsd}] alpha:[${alpha}])"
    }
  }

  final case class SimpleMoment private[analysis] (
      override val alpha: Double,
      override val statistics: Option[Moment.Statistics] = None
  ) extends Moment {
    override def centerOfMass: Double = ( 1D / alpha ) - 1D
    override def :+( value: Double ): Moment = {
      val newStatistics = statistics map { _ :+ value } getOrElse { Statistics( alpha, value ) }
      copy( statistics = Option( newStatistics ) )
    }
  }

  final case class InvalidMomentAlphaError private[analysis] ( alpha: Double )
    extends IllegalArgumentException( s"cannot create MomentStatistics with alpha [${alpha}] outside [0, 1]" )
}
