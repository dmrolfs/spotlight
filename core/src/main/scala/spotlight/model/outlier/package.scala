package spotlight.model


/**
 * Created by rolfsd on 10/4/15.
 */
package object outlier {

  type OutlierAlgorithmResults = Map[Symbol, Outliers]


  trait OutlierError

  final case class InvalidIdError[ID] private[outlier]( id: ID )
  extends IllegalArgumentException( s"id [$id] is not in valid format" ) with OutlierError

  final case class InvalidAlgorithmError private[outlier]( algorithms: Set[String] )
  extends IllegalArgumentException( s"algorithms [$algorithms] are empty or invalid" ) with OutlierError
}
