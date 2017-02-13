package spotlight.model.timeseries

import org.apache.commons.math3.ml.clustering.DoublePoint
import org.joda.{ time ⇒ joda }

case class DataPoint( timestamp: joda.DateTime, value: Double ) {
  def toPointA: PointA = Array( timestamp.getMillis.toDouble, value )
  def toPointT: PointT = ( timestamp.getMillis.toDouble, value )
  def toDoublePoint: DoublePoint = new DoublePoint( toPointA )

  override def toString: String = s"(${timestamp}[${timestamp.getMillis}], ${value})"
}

object DataPoint {
  def fromPointA( pt: PointA ): DataPoint = {
    val Array( ts, v ) = pt
    DataPoint( timestamp = new joda.DateTime( ts.toLong ), value = v )
  }

  def fromPointT( pt: PointT ): DataPoint = {
    val ( ts, v ) = pt
    DataPoint( timestamp = new joda.DateTime( ts.toLong ), value = v )
  }

  def fromDoublePoint( pt: DoublePoint ): DataPoint = {
    val Array( ts, v ) = pt.getPoint
    DataPoint( timestamp = new joda.DateTime( ts.toLong ), value = v )
  }

  implicit class SeqDataPointWrapper( val underlying: Seq[DataPoint] ) extends AnyVal {
    def toPointAs: Seq[PointA] = underlying map { _.toPointA }
    def toPointTs: Seq[PointT] = underlying map { _.toPointT }
    def toDoublePoints: Seq[DoublePoint] = underlying map { _.toDoublePoint }
  }

  implicit def datapoint2doublepoint( dp: DataPoint ): DoublePoint = dp.toDoublePoint
  implicit def datapoint2pointa( dp: DataPoint ): PointA = dp.toPointA
  implicit def datapoint2pointt( dp: DataPoint ): PointT = dp.toPointT

  implicit def datapoints2pointAs( dps: Seq[DataPoint] ): Seq[PointA] = dps map { _.toPointA }
  implicit def datapoints2pointts( dps: Seq[DataPoint] ): Seq[PointT] = dps map { _.toPointT }
  implicit def datapoints2doublepoints( dps: Seq[DataPoint] ): Seq[DoublePoint] = {
    dps map { _.toDoublePoint }
  }
}
