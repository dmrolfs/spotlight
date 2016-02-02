package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import scalaz.{ -\/, \/- }
import lineup.model.timeseries.DataPoint
import lineup.model.outlier.{ NoOutliers, SeriesOutliers }
import lineup.analysis.outlier.{ DetectUsing, DetectOutliersInSeries }


/**
 * Created by rolfsd on 9/29/15.
 */
object SeriesDensityAnalyzer {
  val Algorithm: Symbol = 'dbscanSeries
  def props( router: ActorRef ): Props = Props { new SeriesDensityAnalyzer( router ) }
}

class SeriesDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  override val algorithm: Symbol = SeriesDensityAnalyzer.Algorithm

  override val detect: Receive = LoggingReceive {
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      val points = payload.data.points map {DataPoint.toDoublePoint}

      val result = cluster.run( TestContext( points, algorithmConfig, history ) ) map {clusters =>
        val isOutlier = makeOutlierTest( clusters )
        val outliers = payload.data.points collect { case dp if isOutlier( dp ) => dp }
        if ( outliers.nonEmpty ) SeriesOutliers( algorithms = Set( algorithm ), source = payload.data, outliers = outliers )
        else NoOutliers( algorithms = Set( algorithm ), source = payload.data )
      }

      result match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => log.error( ex, s"failed ${algorithm.name} analysis on ${payload.topic}[${payload.source.interval}]" )
      }
    }
  }
}
