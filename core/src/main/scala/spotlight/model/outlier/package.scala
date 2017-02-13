package spotlight.model

import org.joda.{ time ⇒ joda }
import spotlight.model.timeseries.TimeSeries

/** Created by rolfsd on 10/4/15.
  */
package object outlier {
  type CorrelatedSeries = CorrelatedData[TimeSeries]
  type OutlierAlgorithmResults = Map[String, Outliers]

  object OutlierAlgorithmResults {
    val empty: OutlierAlgorithmResults = Map.empty[String, Outliers]

    def tallyMax( results: OutlierAlgorithmResults ): Int = {
      val t = tally( results ).toSeq.map { _._2.size }
      if ( t.nonEmpty ) t.max else 0
    }

    def tally( results: OutlierAlgorithmResults ): Map[joda.DateTime, Set[String]] = {
      results
        .toSeq
        .flatMap {
          case ( algorithm, outliers ) ⇒
            outliers match {
              case _: NoOutliers ⇒ Seq.empty[( joda.DateTime, Option[String] )]
              case s: SeriesOutliers ⇒ s.outliers map { dp ⇒ ( dp.timestamp, Some( algorithm ) ) }
            }
        }
        .groupBy { case ( ts, c ) ⇒ ts }
        .mapValues { v: Seq[( joda.DateTime, Option[String] )] ⇒ v.foldLeft( Set.empty[Option[String]] ) { _ + _._2 }.flatten }
    }
  }

  trait OutlierError

  final case class InvalidIdError[ID] private[outlier] ( id: ID )
    extends IllegalArgumentException( s"id [$id] is not in valid format" ) with OutlierError

  final case class InvalidAlgorithmError private[outlier] ( algorithms: Set[String] )
    extends IllegalArgumentException( s"algorithms [$algorithms] are empty or invalid" ) with OutlierError
}
