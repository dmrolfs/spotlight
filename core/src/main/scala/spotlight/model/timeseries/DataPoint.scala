package spotlight.model.timeseries

import org.joda.{ time => joda }
import org.apache.commons.math3.ml.{ clustering => ml }


case class ControlBoundary( floor: Double, expected: Double, ceiling: Double ) {
  override def toString: String = s"[${floor}, ${expected}, ${ceiling}] )"
}

case class DataPoint( timestamp: joda.DateTime, value: Double, control: Option[ControlBoundary] = None ) {
  override def toString: String = s"(${timestamp}[${timestamp.getMillis}], ${value})"
}

object DataPoint {
  def fromPoint2D( pt: Point2D ): DataPoint = {
    val (ts, v) = pt
    DataPoint( timestamp = new joda.DateTime(ts.toLong), value = v )
  }

  implicit def toDoublePoint( dp: DataPoint ): ml.DoublePoint = {
    new ml.DoublePoint( Array(dp.timestamp.getMillis.toDouble, dp.value) )
  }

  implicit def toDoublePoints( dps: Seq[DataPoint] ): Seq[ml.DoublePoint] = dps map { toDoublePoint }
}
