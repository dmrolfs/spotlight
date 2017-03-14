package spotlight.analysis.algorithm

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.commons.math3.ml.clustering.DoublePoint
import spotlight.analysis.{ DetectUsing, RecentHistory }
import spotlight.model.outlier.AnalysisPlan
import spotlight.model.timeseries._

/** Created by rolfsd on 2/17/17.
  */
trait AlgorithmContext {
  def message: DetectUsing
  def topic: Topic = message.topic
  def data: Seq[DoublePoint]
  def tolerance: Double
  def recent: RecentHistory

  def plan: AnalysisPlan = message.payload.plan
  def source: TimeSeriesBase = message.source

  /** Properties supplied for with the currently processing request, such as Algorithm-specific configuration group found in
    * the algorithm definition.
    * Some common algorithm properties include "tolerance" and "tail-average"
    * @return algorithm-specific config
    */
  def properties: Config = message.properties

  def fillData( minimalSize: Int = RecentHistory.LastN ): ( Seq[DoublePoint] ) ⇒ Seq[DoublePoint] = { original ⇒
    if ( minimalSize <= original.size ) original
    else {
      val historicalSize = recent.points.size
      val needed = minimalSize + 1 - original.size
      val historical = recent.points.drop( historicalSize - needed )
      historical.toDoublePoints ++ original
    }
  }

  def tailAverage( tailLength: Int = AlgorithmContext.DefaultTailAverageLength ): ( Seq[DoublePoint] ) ⇒ Seq[DoublePoint] = {
    points ⇒
      {
        val values = points map { _.value }
        val lastPos = {
          points.headOption
            .map { h ⇒ recent.points indexWhere { _.timestamp == h.timestamp } }
            .getOrElse { recent.points.size }
        }

        val last = recent.points.drop( lastPos - tailLength + 1 ) map { _.value }

        points
          .map { _.timestamp }
          .zipWithIndex
          .map {
            case ( ts, i ) ⇒
              val pointsToAverage = {
                if ( i < tailLength ) {
                  val all = last ++ values.take( i + 1 )
                  all.drop( all.size - tailLength )
                } else {
                  values.slice( i - tailLength + 1, i + 1 )
                }
              }

              ( ts, pointsToAverage )
          }
          .map {
            case ( ts, pts ) ⇒
              val average = pts.sum / pts.size
              //        logger.debug( "points to tail average ({}, [{}]) = {}", ts.toLong.toString, pts.mkString(","), average.toString )
              ( ts, average ).toDoublePoint
          }
      }
  }
}

object AlgorithmContext {
  val TolerancePath = "tolerance"
  val DefaultTailAverageLength: Int = 3
}

class CommonContext( override val message: DetectUsing ) extends AlgorithmContext {
  override def data: Seq[DoublePoint] = message.payload.source.points
  override def recent: RecentHistory = message.recent
  override def tolerance: Double = message.properties.as[Option[Double]]( AlgorithmContext.TolerancePath ) getOrElse 3.0
}
