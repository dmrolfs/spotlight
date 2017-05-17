package spotlight.model.timeseries

import org.joda.{ time ⇒ joda }

/** Created by rolfsd on 3/17/16.
  */
object ThresholdBoundary {
  def fromExpectedAndDistance( timestamp: Long, expected: Double, distance: Double ): ThresholdBoundary = {
    fromExpectedAndDistance( new joda.DateTime( timestamp ), expected, distance )
  }

  def fromExpectedAndDistance( timestamp: joda.DateTime, expected: Double, distance: Double ): ThresholdBoundary = {
    @inline def optional( v: Double ): Option[Double] = if ( v.isNaN ) None else Some( v )

    val checked = for {
      e ← optional( expected )
      d ← optional( distance )
    } yield ( e - d, e + d )

    val ( f, c ) = checked.unzip // Unzip[Option] unzip checked
    ThresholdBoundary( timestamp = timestamp, floor = f.headOption, expected = optional( expected ), ceiling = c.headOption )
  }

  def empty( timestamp: Long ): ThresholdBoundary = empty( new joda.DateTime( timestamp ) )

  def empty( timestamp: joda.DateTime ): ThresholdBoundary = {
    ThresholdBoundary( timestamp = timestamp, floor = None, expected = None, ceiling = None )
  }
}

case class ThresholdBoundary(
    timestamp: joda.DateTime,
    floor: Option[Double] = None,
    expected: Option[Double] = None,
    ceiling: Option[Double] = None
) {
  def isOutlier( value: Double ): Boolean = !contains( value )
  def contains( value: Double ): Boolean = {
    val equalOrAboveFloor = floor map { _ <= value } getOrElse true
    val equalOrBelowCeiling = ceiling map { value <= _ } getOrElse true
    equalOrAboveFloor && equalOrBelowCeiling
  }

  override def toString: String = s"ThresholdBoundary( ${timestamp}[${timestamp.getMillis}] [f:${floor}, e:${expected}, c:${ceiling}] )"
}
