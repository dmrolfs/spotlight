package spotlight.analysis.algorithm.mutable

import com.google.common.collect.EvictingQueue
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.stat.descriptive.StatisticalSummary


object MovingStatistics {
  def apply( capacity: Int ): MovingStatistics = new MovingStatistics( EvictingQueue.create[Double]( capacity ) )
}

/** Computes moving statistics for a stream of data values added using the :+ operator. The class calculates statistics over a
  * window of data defined by sampleSize property.
  * Created by rolfsd on 2/27/17.
  */
class MovingStatistics( val window: EvictingQueue[Double] ) extends StatisticalSummary with Serializable with Equals {
  import scala.collection.JavaConverters._

  /** add a value to the moving statistics. If the number of elements exceeds sampleSize, then the oldest value is dropped.
    * @param value
    * @return moving statistics
    */
  def :+( value: Double ): MovingStatistics = { window.add( value ); this }

  /** add a sequence of values to the moving statistics. If the number of elements exceed sampleSize, then oldest value(s) are
    * dropped.
    */
  //  def :++( values: Double* ): MovingStatistics = { values.foreach { window.add }; this }

  def copy(): MovingStatistics = {
    val newWindow = EvictingQueue.create[Double]( capacity )
    newWindow.addAll( window )
    new MovingStatistics( newWindow )
  }

  def capacity: Int = window.size + window.remainingCapacity

  /** Returns the number of available values
    * @return number of available values
    */
  def N: Long = window.size

  /** StatisticalSummary interface. Returns the number of available values
    * @return number of available values
    */
  override def getN: Long = N

  /** Returns the sum of the currently in the window
    * @return the sum or Double.NaN is no values have been added
    */
  def sum: Double = window.iterator.asScala.sum

  /** StatisticalSummary interface. Returns the sum of the currently in the window
    * @return the sum or Double.NaN is no values have been added
    */
  override def getSum: Double = sum

  /** Returns the mean of the values that have been added.
    * Double.NaN is returned if no values have been added.
    * @return mean
    */
  def mean: Double = if ( 0L < N ) sum / N else Double.NaN

  /** StatisticalSummary interface. Returns the mean of the values that have been added.
    * Double.NaN is returned if no values have been added.
    * @return mean
    */
  override def getMean: Double = mean

  /** Returns the standard deviation of the values that have been added.
    * Double.NaN is returned if no values have been added.
    * @return standard deviation
    */
  def standardDeviation: Double = math.sqrt( variance ) * ( 1 - biasCorrection )

  //todo: need to work this through more... current impl mimics DescriptiveStatistics.
  private def biasCorrection: Double = 0D // math.sqrt( 2.0 / ( N - 1.0 ) ) * Gamma.gamma( N / 2.0 ) / Gamma.gamma( ( N - 1.0 ) / 2.0 )

  /** StatisticalSummary interface. Returns the standard deviation of the values that have been added.
    * Double.NaN is returned if no values have been added.
    * @return standard deviation
    */
  override def getStandardDeviation: Double = standardDeviation

  /** Returns the (sample) variance of the available values. This method returns the bias-corrected sample variance
    * (using n - 1 in the denominator).
    * Double.NaN is returned if no values have been added.
    * @return variance
    */
  def variance: Double = {
    if ( 0L < N ) {
      if ( 1L < N ) {
        val mu = mean
        val parts = window.iterator().asScala.map { n ⇒ math.pow( ( n - mu ), 2.0 ) }
        1.0 / ( N - 1.0 ) * parts.sum
      } else {
        0.0
      }
    } else {
      Double.NaN
    }
  }

  /** StatisticalSummary interface. Returns the (sample) variance of the available values. This method returns the bias-corrected
    * sample variance (using n - 1 in the denominator).
    * Double.NaN is returned if no values have been added.
    * @return variance
    */
  override def getVariance: Double = variance

  /** Returns the minimum of the values that have been added.
    * Double.NaN is returned if no values have been added.
    * @return minimun
    */
  def minimum: Double = if ( 0L < N ) window.iterator.asScala.min else Double.NaN

  /** StatisticalSummary interface. Returns the minimum of the values that have been added.
    * Double.NaN is returned if no values have been added.
    * @return minimun
    */
  override def getMin: Double = minimum

  /** Returns the maximum of the values that have been added.
    * Double.NaN is returned if no values have been added.
    * @return maximum
    */
  def maximum: Double = if ( 0L < N ) window.iterator.asScala.max else Double.NaN

  /** StatisticalSummary interface. Returns the maximum of the values that have been added.
    * Double.NaN is returned if no values have been added.
    * @return maximum
    */
  override def getMax: Double = maximum

  def values: Array[Double] = window.iterator.asScala.toArray

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[MovingStatistics]

  override def equals( rhs: Any ): Boolean = {
    def optionalNaN( d: Double ): Option[Double] = if ( d.isNaN ) None else Some( d )

    rhs match {
      case that: MovingStatistics ⇒ {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.capacity == that.capacity ) &&
            ( this.window == that.window )
        }
      }

      case _ ⇒ false
    }
  }

  override def hashCode: Int = {
    41 * (
      41 + capacity.##
    ) + window.##
  }

  override def toString: String = {
    s"${ClassUtils.getAbbreviatedName( getClass, 15 )}(" +
      s"capacity:[${capacity}] " +
      s"N:${N}" +
      ( if ( 0L < N ) s" mean:${mean} stddev:${standardDeviation} range:[${minimum} - ${maximum}]] " else "" ) +
      ")"
  }
}
