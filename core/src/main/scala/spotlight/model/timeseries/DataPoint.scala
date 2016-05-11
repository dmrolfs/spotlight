package spotlight.model.timeseries

import org.joda.{ time => joda }
import org.apache.commons.math3.ml.{ clustering => ml }


case class DataPoint( timestamp: joda.DateTime, value: Double ) {
  override def toString: String = s"(${timestamp}[${timestamp.getMillis}], ${value})"
}

object DataPoint {
  def fromPoint2D( pt: PointT ): DataPoint = {
    val (ts, v) = pt
    DataPoint( timestamp = new joda.DateTime(ts.toLong), value = v )
  }

  implicit def toDoublePoint( dp: DataPoint ): ml.DoublePoint = {
    new ml.DoublePoint( Array(dp.timestamp.getMillis.toDouble, dp.value) )
  }

  implicit def toPointA( dp: DataPoint ): PointA = toDoublePoint( dp ).getPoint
  implicit def toPointT( dp: DataPoint ): PointT = ( dp.timestamp.getMillis.toDouble, dp.value )

  implicit def toDoublePoints( dps: Seq[DataPoint] ): Seq[ml.DoublePoint] = dps map { toDoublePoint }
  implicit def toPointAs( dps: Seq[DataPoint] ): Seq[PointA] = dps map { toPointA }
  implicit def toPointTs( dps: Seq[DataPoint] ): Seq[PointT] = dps map { toPointT }
}
