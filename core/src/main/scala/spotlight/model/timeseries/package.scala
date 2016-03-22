package spotlight.model

import org.apache.commons.math3.ml.clustering.DoublePoint


package object timeseries {
  type Points = Seq[DoublePoint]
  type Point = Array[Double]
  type Point2D = (Double, Double)


  type Matrix[T] = IndexedSeq[IndexedSeq[T]]


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


  implicit class TopicString( val underlying: String ) extends AnyVal {
    def toTopic: Topic = Topic fromString underlying
  }


  trait TimeSeriesError
}
