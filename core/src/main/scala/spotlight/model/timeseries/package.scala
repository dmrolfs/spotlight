package spotlight.model

import bloomfilter.CanGenerateHashFrom
import bloomfilter.CanGenerateHashFrom.CanGenerateHashFromString
import org.joda.{ time ⇒ joda }
import org.apache.commons.math3.ml.clustering.DoublePoint
import omnibus.akka.envelope.WorkId

package object timeseries {
  //  type IdentifiedTimeSeries = (TimeSeries, Set[WorkId])

  type PointA = Array[Double]
  type PointT = ( Double, Double )

  implicit class PointAWrapper( val underlying: PointA ) extends AnyVal {
    def timestamp: Double = toPointT._1
    def dateTime: joda.DateTime = new joda.DateTime( timestamp.toLong )
    def value: Double = toPointT._2

    def toPointT: PointT = ( underlying( 0 ), underlying( 1 ) )
    def toDoublePoint: DoublePoint = new DoublePoint( underlying )
    def toDataPoint: DataPoint = DataPoint( timestamp = new joda.DateTime( underlying( 0 ).toLong ), value = underlying( 1 ) )
  }

  implicit class PointTWrapper( val underlying: PointT ) extends AnyVal {
    def timestamp: Double = underlying._1
    def dateTime: joda.DateTime = new joda.DateTime( timestamp.toLong )
    def value: Double = underlying._2

    def toPointA: PointA = Array( underlying._1, underlying._2 )
    def toDoublePoint: DoublePoint = new DoublePoint( toPointA )
    def toDataPoint: DataPoint = DataPoint( timestamp = new joda.DateTime( underlying._1.toLong ), value = underlying._2 )
  }

  implicit class DoublePointWrapper( val underlying: DoublePoint ) extends AnyVal {
    def timestamp: Double = toPointT._1
    def dateTime: joda.DateTime = new joda.DateTime( timestamp.toLong )
    def value: Double = toPointT._2

    def toPointA: PointA = underlying.getPoint
    def toPointT: PointT = {
      val Array( ts, v ) = underlying.getPoint
      ( ts, v )
    }
    def toDataPoint: DataPoint = {
      val Array( ts, v ) = underlying.getPoint
      DataPoint( timestamp = new joda.DateTime( ts.toLong ), value = v )
    }
  }

  implicit class SeqPointAWrapper( val underlying: Seq[PointA] ) extends AnyVal {
    def toPointTs: Seq[PointT] = underlying map { _.toPointT }
    def toDoublePoints: Seq[DoublePoint] = underlying map { _.toDoublePoint }
    def toDataPoints: Seq[DataPoint] = underlying map { _.toDataPoint }
  }

  implicit class SeqPointTWrapper( val underlying: Seq[PointT] ) extends AnyVal {
    def toPointAs: Seq[PointA] = underlying map { _.toPointA }
    def toDoublePoints: Seq[DoublePoint] = underlying map { _.toDoublePoint }
    def toDataPoints: Seq[DataPoint] = underlying map { _.toDataPoint }
  }

  implicit class SeqDoublePointWrapper( val underlying: Seq[DoublePoint] ) extends AnyVal {
    def toPointAs: Seq[PointA] = underlying map { _.toPointA }
    def toPointTs: Seq[PointT] = underlying map { _.toPointT }
    def toDataPoints: Seq[DataPoint] = underlying map { _.toDataPoint }
  }

  implicit def pointa2pointt( pt: PointA ): PointT = pt.toPointT
  implicit def pointa2doublepoint( pt: PointA ): DoublePoint = pt.toDoublePoint
  implicit def pointt2pointa( pt: PointT ): PointA = pt.toPointA
  implicit def pointt2doublepoint( pt: PointT ): DoublePoint = pt.toDoublePoint
  implicit def doublepoint2pointa( dp: DoublePoint ): PointA = dp.toPointA
  implicit def doublepoint2pointt( dp: DoublePoint ): PointT = dp.toPointT

  implicit def pointas2pointts( pts: Seq[PointA] ): Seq[PointT] = pts map { pointa2pointt }
  implicit def pointas2doublepoints( pts: Seq[PointA] ): Seq[DoublePoint] = pts map { pointa2doublepoint }
  implicit def pointts2pointas( pts: Seq[PointT] ): Seq[PointA] = pts map { pointt2pointa }
  implicit def pointts2doublepoints( pts: Seq[PointT] ): Seq[DoublePoint] = pts map { pointt2doublepoint }
  implicit def doublepoints2pointas( dps: Seq[DoublePoint] ): Seq[PointA] = dps map { doublepoint2pointa }
  implicit def doublepoints2pointts( dps: Seq[DoublePoint] ): Seq[PointT] = dps map { doublepoint2pointt }

  type Matrix[T] = IndexedSeq[IndexedSeq[T]]

  case class Topic( name: String ) {

    override def hashCode(): Int = scala.util.hashing.MurmurHash3.stringHash( name )

    override def toString: String = name
  }

  object Topic {
    implicit object CanGenerateTopicHash extends CanGenerateHashFrom[Topic] {
      override def generateHash( from: Topic ): Long = CanGenerateHashFromString generateHash from.name
    }

    implicit def fromString( topic: String ): Topic = Topic( name = topic )

    def findAncestor( topics: Topic* ): Topic = {
      def trim( topic: String ): String = {
        val Delim = """[._+\:\-\/\\]+""".r
        Delim.findPrefixOf( topic.reverse ) map { t ⇒ topic.slice( 0, topic.length - t.length ) } getOrElse topic
      }

      trim(
        if ( topics.isEmpty ) ""
        else {
          var i = 0
          topics( 0 ).name.takeWhile( ch ⇒ topics.forall( _.name( i ) == ch ) && { i += 1; true } ).mkString
        }
      )
    }
  }

  implicit class TopicString( val underlying: String ) extends AnyVal {
    def toTopic: Topic = Topic fromString underlying
  }

  trait TimeSeriesError
}
