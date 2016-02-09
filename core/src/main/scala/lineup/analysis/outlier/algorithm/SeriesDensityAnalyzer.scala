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
    case s @ DetectUsing( _, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      ( extractTestContext >==> cluster >==> findOutliers( payload.source ) ).run( s ) match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => log.error( ex, "failed {} analysis on {}[{}]", algorithm.name, payload.topic, payload.source.interval )
      }
    }
  }

  def findOutliers( source: TimeSeries ): Op[(TestContext, Seq[Cluster[DoublePoint]]), Outliers] = {
    Kleisli[TryV, (TestContext, Seq[Cluster[DoublePoint]]), Outliers] { case (ctx, clusters) =>
      val isOutlier = makeOutlierTest( clusters )
      val outliers = for {
        dp <- ctx.data if isOutlier( dp )
        o <- source.points if o.timestamp.getMillis == dp.getPoint.head.toLong
      } yield o

      if ( outliers.nonEmpty ) {
        SeriesOutliers(
          algorithms = Set( algorithm ),
          source = source,
          outliers = outliers.toIndexedSeq,
          plan = ctx.message.plan
        ).right
      }
      else {
        NoOutliers( algorithms = Set( algorithm ), source = source, plan = ctx.message.plan ).right
      }
    }
  }
}
