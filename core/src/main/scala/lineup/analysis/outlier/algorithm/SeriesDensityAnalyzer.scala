package lineup.analysis.outlier.algorithm

import akka.actor.{ ActorRef, Props }
import akka.event.LoggingReceive
import org.apache.commons.math3.ml.clustering.{ Cluster, DoublePoint }
import scalaz._, Scalaz._
import lineup.model.timeseries.TimeSeries
import lineup.model.outlier.{ OutlierPlan, Outliers, NoOutliers, SeriesOutliers }
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
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, plan, history, algorithmConfig ) => {
      ( extractTestContext >==> cluster >==> findOutliers( payload.source, plan ) ).run( s ) match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => log.error( ex, "failed {} analysis on {}[{}]", algorithm.name, payload.topic, payload.source.interval )
      }
    }
  }

  def findOutliers( series: TimeSeries, plan: OutlierPlan ): Op[Seq[Cluster[DoublePoint]], Outliers] = {
    Kleisli[TryV, Seq[Cluster[DoublePoint]], Outliers] { clusters =>
      val isOutlier = makeOutlierTest( clusters )
      val outliers = series.points collect { case dp if isOutlier( dp ) => dp }

      if ( outliers.nonEmpty ) {
        SeriesOutliers( algorithms = Set( algorithm ), source = series, outliers = outliers, plan = plan ).right
      }
      else {
        NoOutliers( algorithms = Set( algorithm ), source = series, plan = plan ).right
      }
    }
  }
}
