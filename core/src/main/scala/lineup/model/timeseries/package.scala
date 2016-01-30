package lineup.model

import scala.collection.immutable
import scalaz._, Scalaz._
import org.joda.{ time => joda }
import com.github.nscala_time.time.Imports._
import org.apache.commons.math3.ml.{ clustering => ml }
import peds.commons.Valid


package object timeseries {
  case class DataPoint( timestamp: joda.DateTime, value: Double ) {
    override def toString: String = s"(${timestamp}[${timestamp.getMillis}], ${value})"
  }

  object DataPoint {
    implicit def toDoublePoint( dp: DataPoint ): ml.DoublePoint = {
      new ml.DoublePoint( Array(dp.timestamp.getMillis.toDouble, dp.value) )
    }
  }

  type Row[T] = immutable.IndexedSeq[T]
  object Row {
    def apply[T]( data: T* ): Row[T] = immutable.IndexedSeq( data:_* )
    def empty[T]: Row[T] = immutable.IndexedSeq.empty[T]
  }

  implicit class ConvertSeqToRow[T]( val seq: Seq[T] ) extends AnyVal {
    def toRow: Row[T] = Row( seq:_* )
  }

  type Matrix[T] = Row[Row[T]]


  trait TimeSeriesBase {
    def topic: Topic
    def size: Int
    def start: Option[joda.DateTime]
    def end: Option[joda.DateTime]
    def interval: Option[joda.Interval] = {
      for {
        s <- start
        e <- end
      } yield new joda.Interval( s, e + 1.millis )
    }
  }

  object TimeSeriesBase {
    final case class IncompatibleTopicsError private[timeseries]( originalTopic: Topic, newTopic: Topic )
    extends IllegalArgumentException( s"cannot merge time series topics: original [${originalTopic}] amd mew topic [${newTopic}]" )
    with TimeSeriesError


    trait Merging[T <: TimeSeriesBase] {
      def merge( lhs: T, rhs: T ): Valid[T]
      def zero( topic: Topic ): T

      protected def checkTopic( lhs: Topic, rhs: Topic ): Valid[Topic] = {
        if ( lhs == rhs ) lhs.successNel[Throwable]
        else Validation.failureNel( IncompatibleTopicsError(originalTopic = lhs, newTopic = rhs) )
      }
    }
  }


  case class Topic( name: String ) {
    override def toString: String = name
  }

  object Topic {
    implicit def fromString( topic: String ): Topic = Topic( name = topic )

    def findAncestor( topics: Topic* ): Topic = {
      def trim( topic: String ): String = {
        val Delim = """[._+\:\-\/\\]+""".r
        Delim.findPrefixOf( topic.reverse ) map { t => topic.slice( 0, topic.length - t.length ) } getOrElse topic
      }

      trim(
        if ( topics.isEmpty ) ""
        else {
          var i = 0
          topics( 0 ).name.takeWhile( ch => topics.forall( _.name(i) == ch ) && { i += 1; true } ).mkString
        }
      )
    }
  }


  trait TimeSeriesError
}
