package spotlight.model.timeseries

import scala.concurrent.duration.{ SECONDS, TimeUnit }
import cats.syntax.cartesian._
import cats.syntax.validated._
import cats.instances.list._
import cats.syntax.traverse._
import shapeless.Lens
import org.joda.{ time ⇒ joda }
import com.github.nscala_time.time.Imports._
import omnibus.commons.AllIssuesOr
import omnibus.commons.math.Interpolator

trait TimeSeriesCohort extends TimeSeriesBase {
  override def size: Int = data.size
  def data: IndexedSeq[TimeSeries]
  def precision: TimeUnit
  def toMatrix: Matrix[( joda.DateTime, Double )]
}

object TimeSeriesCohort {
  def apply( topic: Topic, data: IndexedSeq[TimeSeries], precision: TimeUnit ): TimeSeriesCohort = {
    SimpleTimeSeriesCohort( topic, data, precision )
  }

  def apply( topic: Topic, data: IndexedSeq[TimeSeries] = IndexedSeq.empty[TimeSeries] ): TimeSeriesCohort = {
    apply( topic, data, SECONDS )
  }

  def apply( data: IndexedSeq[TimeSeries], precision: TimeUnit ): TimeSeriesCohort = {
    val prefix = Topic.findAncestor( data.map( _.topic ): _* )
    SimpleTimeSeriesCohort( prefix, data, precision )
  }

  def apply( data: TimeSeries* ): TimeSeriesCohort = apply( data.toIndexedSeq, SECONDS )

  val topicLens: Lens[TimeSeriesCohort, Topic] = new Lens[TimeSeriesCohort, Topic] {
    override def get( c: TimeSeriesCohort ): Topic = c.topic
    override def set( c: TimeSeriesCohort )( t: Topic ): TimeSeriesCohort = {
      TimeSeriesCohort( topic = t, data = c.data, precision = c.precision )
    }
  }

  val dataLens: Lens[TimeSeriesCohort, IndexedSeq[TimeSeries]] = new Lens[TimeSeriesCohort, IndexedSeq[TimeSeries]] {
    override def get( c: TimeSeriesCohort ): IndexedSeq[TimeSeries] = c.data
    override def set( c: TimeSeriesCohort )( d: IndexedSeq[TimeSeries] ): TimeSeriesCohort = {
      TimeSeriesCohort( topic = c.topic, data = d, precision = c.precision )
    }
  }

  val precisionLens: Lens[TimeSeriesCohort, TimeUnit] = new Lens[TimeSeriesCohort, TimeUnit] {
    override def get( c: TimeSeriesCohort ): TimeUnit = c.precision
    override def set( c: TimeSeriesCohort )( p: TimeUnit ): TimeSeriesCohort = {
      TimeSeriesCohort( topic = c.topic, data = c.data, precision = p )
    }
  }

  case class TimeFrame( bounds: joda.Interval, mid: joda.DateTime )

  def timeframes( start: joda.DateTime, end: joda.DateTime, millisUnit: Long ): Stream[TimeFrame] = {
    if ( end < start ) Stream.empty
    else {
      val unitEnd = start plus millisUnit
      val frameEnd = implicitly[Ordering[joda.DateTime]].min( end + 1.milli, unitEnd )
      val midMillis = ( ( start.getMillis.toDouble + frameEnd.getMillis.toDouble ) / 2.0 ).toLong
      val mid = new joda.DateTime( midMillis )
      val interval = start to frameEnd
      TimeFrame( interval, mid ) #:: timeframes( frameEnd, end, millisUnit )
    }
  }

  import TimeSeriesBase.Merging

  implicit val cohortMerging: Merging[TimeSeriesCohort] = new Merging[TimeSeriesCohort] {
    override def zero( topic: Topic ): TimeSeriesCohort = TimeSeriesCohort( topic )

    override def merge( lhs: TimeSeriesCohort, rhs: TimeSeriesCohort ): AllIssuesOr[TimeSeriesCohort] = {
      ( checkTopic( lhs.topic, rhs.topic ) |@| combineSeries( lhs.data, rhs.data ) ) map { ( _, merged ) ⇒
        dataLens.set( lhs )( merged )
      }
    }

    private def combineSeries(
      lhs: IndexedSeq[TimeSeries],
      rhs: IndexedSeq[TimeSeries]
    )(
      implicit
      seriesMerge: Merging[TimeSeries]
    ): AllIssuesOr[IndexedSeq[TimeSeries]] = {
      val merged = lhs ++ rhs
      val ( uniques, dups ) = merged groupBy { _.topic } map { _._2 } partition { _.size == 1 }
      val dupsMerged = for {
        d ← dups
      } yield {
        val zero: AllIssuesOr[TimeSeries] = TimeSeries( d.head.topic, IndexedSeq.empty[DataPoint] ).validNel
        import cats.syntax.either._

        d.foldLeft( zero ) { ( acc: AllIssuesOr[TimeSeries], c: TimeSeries ) ⇒
          val eacc = acc.toEither
          val merged = eacc flatMap { ts ⇒ seriesMerge.merge( ts, c ).toEither }
          merged.toValidated
        }
      }

      dupsMerged.toList.sequence map { uniques.toIndexedSeq.flatten ++ _.toIndexedSeq }
    }
  }

  final case class SimpleTimeSeriesCohort private[timeseries] (
      override val topic: Topic,
      override val data: IndexedSeq[TimeSeries],
      override val precision: TimeUnit
  ) extends TimeSeriesCohort {
    override def points: Seq[DataPoint] = data flatMap { _.points }

    override val start: Option[joda.DateTime] = {
      val result = for {
        d ← data
        h ← d.points.headOption
      } yield h.timestamp

      if ( result.nonEmpty ) Some( result.min ) else None
      //      data.map( _.points.head.timestamp ).min
    }

    override val end: Option[joda.DateTime] = {
      val result = for {
        d ← data
        h ← d.points.lastOption
      } yield h.timestamp

      if ( result.nonEmpty ) Some( result.max ) else None
      //      data.map( _.points.last.timestamp ).max
    }

    private val unit: Long = precision toMillis 1

    //todo: refactor fp / kleisli arrows
    /** organizes data set into frames of interpolated values. Frames are defined by grouping time axis into time frames
      * according to precision; e.g., SECONDS precision results in time frames of 1-second.
      * There is a row for each time frame from the cohort's start and including the cohort's end; i.e., the last frame may be
      * shorter depending on where the cohort's end lands within the potential frame.
      * Within each frame the value is either:
      * - if no points exists within the frame, it is empty
      * - if one point exists within the frame, then it is the point,
      * - if multiple points exist within the frame, then the x-value is the frame's midpoint and the y-value is the
      * interpolated at the mid-point.
      *
      * @return a sequence of interpolated values per time frame.
      */
    override lazy val toMatrix: Matrix[( joda.DateTime, Double )] = {
      val result = for {
        s ← start
        e ← end
      } yield {
        for {
          frame ← timeframes( s, e, unit ).toIndexedSeq
        } yield {
          val frameGroup = for {
            d ← data
            points = d.points.filter { frame.bounds contains _.timestamp }.toList
          } yield {
            points match {
              case Nil ⇒ None
              case p :: Nil ⇒ Some( ( p.timestamp, p.value ) )
              case pts ⇒ {
                val xs = pts map { _.timestamp.getMillis.toDouble }
                val ys = pts map { _.value }
                val result = Interpolator( xs.toArray, ys.toArray ) map { interpolate ⇒
                  ( frame.mid, interpolate( frame.mid.getMillis.toDouble ) )
                }

                result.toOption
              }
            }
          }
          frameGroup.flatten
        }
      }

      result getOrElse IndexedSeq.empty[IndexedSeq[( joda.DateTime, Double )]]
    }
  }
}

//todo      for each series disperse elems into frames
//todo      for each frame interpolate y value based on xs and ys by mid point

//      for {
//        (s, mid, e) <- frames( start, end + 1.milli, precision toMillis 1 ).toIndexedSeq
//        i = new joda.Interval( s, e )
//      } yield {
//        for {
//          d <- data
//          points = d.points filter { i contains _.timestamp } map { xy => ( xy.timestamp.getMillis.toDouble, xy.value ) }
////          if points.nonEmpty
//          xs = points map { _._1 }
//          ys = points map { _._2 }
//        } yield trace.block( s"framing[$i]" ) {
////val partitions = d.points.partition( i contains _.timestamp )
////val included = partitions._1.map( _.timestamp )
////val remainder = partitions._2.map( _.timestamp ).filter( i.getEnd <= _ )
////val ignored = partitions._2.map( _.timestamp ).filter( i => !included.contains(i) && !remainder.contains(i) )
////trace( s"""INCLUDED = ${included.mkString(",")}""")
////trace( s"""REMAINDER = ${remainder.mkString(",")}""")
////trace( s"""IGNORED = ${ignored.mkString(",")}""")
////trace( s"""ALL POINTS = ${d.points.mkString(",")}""")
//
//          if ( points.isEmpty ) trace.briefBlock("EMPTY POINTS"){ (new joda.DateTime(0L), 0D)}
//          else if ( points.size == 1 ) trace.briefBlock( s"single-pt: $points" ) { (new joda.DateTime(xs.head.toLong), ys.head ) }
//          else trace.block( s"""mulitple-pts: ${points.mkString(",")}""" ){
//            val result = Interpolator( xs.toArray, ys.toArray ) map { interpolate =>
//              trace( s"s=${s.getMillis}   e=${e.getMillis}")
//              trace( s"mid x= $mid   millis:${mid.getMillis}" )
//              trace( s"""xs = ${xs.mkString("[",",","]")}""" )
//              trace( s"""ys = ${xs.mkString("[",",","]")}""" )
//              trace( s"interpolated y= ${interpolate(mid.getMillis.toDouble)}" )
//              ( mid, interpolate( mid.getMillis.toDouble ) )
//            }
//
//            result.valueOr( exs => throw exs.head )
//          }
//        }
//      }
