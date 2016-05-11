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

  implicit def toPoint( dp: DataPoint ): PointA = toDoublePoint( dp ).getPoint

  implicit def toDoublePoints( dps: Seq[DataPoint] ): Seq[ml.DoublePoint] = dps map { toDoublePoint }
}
