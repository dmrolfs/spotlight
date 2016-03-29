package spotlight.model

import org.joda.{ time => joda }


/**
 * Created by rolfsd on 10/4/15.
 */
package object outlier {

  type OutlierAlgorithmResults = Map[Symbol, Outliers]

  object OutlierAlgorithmResults {
    val empty: OutlierAlgorithmResults = Map.empty[Symbol, Outliers]

    def tallyMax( results: OutlierAlgorithmResults ): Int = {
      val t = tally( results ).toSeq.map{ _._2 }
      if ( t.nonEmpty ) t.max else 0
    }

    def tally( results: OutlierAlgorithmResults ): Map[joda.DateTime, Int] = {
      results
      .toSeq
      .flatMap { case (algorithm, outliers) =>
        outliers match {
          case _: NoOutliers => Seq.empty[(joda.DateTime, Int)]
          case s: SeriesOutliers => s.outliers map { dp => ( dp.timestamp, 1 ) }
        }
      }
      .groupBy { case (ts, c) => ts }
      .mapValues { _.foldLeft( 0 ) { _ + _._2 } }
    }
  }


  trait OutlierError

  final case class InvalidIdError[ID] private[outlier]( id: ID )
  extends IllegalArgumentException( s"id [$id] is not in valid format" ) with OutlierError

  final case class InvalidAlgorithmError private[outlier]( algorithms: Set[String] )
  extends IllegalArgumentException( s"algorithms [$algorithms] are empty or invalid" ) with OutlierError
}
