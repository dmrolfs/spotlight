package spotlight.model.timeseries

import org.joda.{time => joda}


/**
  * Created by rolfsd on 3/17/16.
  */
object ControlBoundary {
  def fromExpectedAndDistance( timestamp: Long, expected: Double, distance: Double ): ControlBoundary = {
    fromExpectedAndDistance( new joda.DateTime(timestamp), expected, distance )
  }

  def fromExpectedAndDistance( timestamp: joda.DateTime, expected: Double, distance: Double ): ControlBoundary = {
    val checkedExpected = if ( expected.isNaN ) None else Some( expected )

    val checked = for {
      e <- checkedExpected
      d <- if ( distance.isNaN ) None else Some( math.abs(distance) )
    } yield ( e - d, e + d )

    ControlBoundary(
      timestamp = timestamp,
      floor = checked map { _._1 },
      expected = checkedExpected,
      ceiling = checked map { _._2 }
    )
  }

  def empty( timestamp: Long ): ControlBoundary = empty( new joda.DateTime(timestamp) )

  def empty( timestamp: joda.DateTime ): ControlBoundary = {
    ControlBoundary( timestamp = timestamp, floor = None, expected = None, ceiling = None )
  }
}

case class ControlBoundary(
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

  override def toString: String = s"ControlBoundary( ${timestamp}[${timestamp.getMillis}]f:${floor}, e:${expected}, c:${ceiling} )"
}
