package spotlight.analysis.algorithm.density

import scala.annotation.tailrec
import scala.reflect.ClassTag
import akka.actor.{ ActorRef, Props }
import nl.grons.metrics.scala.{ Histogram, Timer }
import org.apache.commons.math3.linear.EigenDecomposition

import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.instances.either._
import cats.syntax.either._
import cats.syntax.validated._

import omnibus.commons.{ KOp, ErrorOr, AllIssuesOr }
import omnibus.commons.util._
import org.apache.commons.math3.ml.clustering.{ Cluster, DBSCANClusterer, DoublePoint }
import org.apache.commons.math3.ml.distance.{ DistanceMeasure, EuclideanDistance }
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import omnibus.commons.math.MahalanobisDistance
import spotlight.analysis.HistoricalStatistics
import spotlight.analysis.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.algorithm.CommonAnalyzer
import spotlight.analysis.algorithm.CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._

/** Created by rolfsd on 2/25/16.
  */
object SeriesDensityAnalyzer {
  val Algorithm: String = "dbscanSeries"

  def props( router: ActorRef, alphaValue: Double = 0.05 ): Props = {
    Props {
      new SeriesDensityAnalyzer( router ) with HistoryProvider {
        override val alpha: Double = alphaValue
      }
    }
  }

  final case class Context private[density] (
      override val underlying: AlgorithmContext,
      distanceStatistics: DescriptiveStatistics
  ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): AllIssuesOr[WrappingContext] = copy( underlying = ctx ).validNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addThresholdBoundary( threshold: ThresholdBoundary ): That = {
      copy( underlying = underlying.addThresholdBoundary( threshold ) )
    }

    override def toString: String = {
      s"""${getClass.safeSimpleName}(distance-statistics:[${distanceStatistics}])"""
    }
  }

  trait HistoryProvider {
    def alpha: Double
  }
}

class SeriesDensityAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[SeriesDensityAnalyzer.Context] {
  outer: SeriesDensityAnalyzer.HistoryProvider ⇒

  lazy val clusterTimer: Timer = metrics.timer( algorithm, "cluster" )
  lazy val sourceSizeHistogram: Histogram = metrics.histogram( algorithm, "source-size" )
  lazy val filledSizeHistogram: Histogram = metrics.histogram( algorithm, "filled-size" )

  import SeriesDensityAnalyzer.Context

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override val algorithm: String = SeriesDensityAnalyzer.Algorithm

  override def wrapContext( c: AlgorithmContext ): AllIssuesOr[WrappingContext] = {
    makeMovingStatistics( c ) map { movingStats ⇒ Context( underlying = c, distanceStatistics = movingStats ) }
  }

  def makeMovingStatistics( context: AlgorithmContext ): AllIssuesOr[DescriptiveStatistics] = {
    new DescriptiveStatistics( CommonAnalyzer.ApproximateDayWindow ).validNel
  }

  val distanceMeasure: KOp[AlgorithmContext, DistanceMeasure] = Kleisli { _.distanceMeasure }

  val updateDistanceMoment: KOp[AlgorithmContext, AlgorithmContext] = {
    import spotlight.analysis._

    def distanceIsValid( d: DistanceMeasure, h: HistoricalStatistics ): Boolean = {
      // stinks have to resort to this match. Type class is muted due to instantiating distance measure from configuration.
      d match {
        case m: MahalanobisDistance ⇒ mahalanobisValidity.isApplicable( m, h )
        case e: EuclideanDistance ⇒ euclideanValidity.isApplicable( e, h )
        case _ ⇒ true
      }
    }

    for {
      ctx ← toConcreteContextK
      distance ← distanceMeasure
    } yield {
      if ( !distanceIsValid( distance, ctx.message.history ) ) {
        log.debug( "updateDistanceMoment: distance covariance matrix has ZERO DETERMINANT topic:[{}]", ctx.message.topic )
        ctx
      } else {
        val distances = contiguousPairs( ctx ) map {
          case ( cur, prev ) ⇒
            val d = distance.compute( cur, prev )
            log.debug( "distance:[{}] for contiguous pairs: [{}, {}]", d, prev.getPoint.mkString( "(", ",", ")" ), cur.getPoint.mkString( "(", ",", ")" ) )
            d
        }
        val updatedStats = distances.foldLeft( ctx.distanceStatistics ) { ( stats, d ) ⇒
          if ( !d.isNaN ) stats addValue d
          stats
        }
        log.debug( "updated distance m:[{}] sd:[{}] statistics: [{}]", updatedStats.getMean, updatedStats.getStandardDeviation, updatedStats )
        ctx.copy( distanceStatistics = updatedStats ).asInstanceOf[AlgorithmContext]
      }
    }
  }

  val minDensityPoints: KOp[AlgorithmContext, Int] = {
    Kleisli[ErrorOr, AlgorithmContext, Int] { ctx ⇒
      Either catchNonFatal { ctx.messageConfig getInt algorithm + ".minDensityConnectedPoints" }
    }
  }

  type Clusters = Seq[Cluster[DoublePoint]]
  val cluster: KOp[AlgorithmContext, ( Clusters, AlgorithmContext )] = {
    for {
      ctx ← toConcreteContextK
      filled ← fillDataFromHistory( 6 * 5 ) // fill up to 5 minutes @ 1pt / 10s
      e ← toConcreteContextK andThen eps
      distance ← distanceMeasure
      minPoints ← minDensityPoints
    } yield {
      log.debug( "DBSCAN eps = [{}]", e )
      log.debug( "DBSCAN filled orig:[{}] past:[{}] points=[{}]", ctx.data.size, filled.size - ctx.data.size, filled.mkString( "," ) )
      //      log.debug( "cluster: context dist-statistics=[{}]", ctx.distanceStatistics )
      import scala.collection.JavaConverters._

      sourceSizeHistogram += ctx.data.size
      filledSizeHistogram += filled.size

      val clustersD = clusterTimer.time {
        Either catchNonFatal {
          new DBSCANClusterer[DoublePoint]( e, minPoints, distance ).cluster( filled.asJava ).asScala.toSeq
        }
      }

      if ( log.isDebugEnabled ) {
        val sizeClusters = clustersD map { clusters ⇒
          clusters map { c ⇒ ( c.getPoints.size, c.getPoints.asScala.mkString( "[", ", ", "]" ) ) }
        }
        log.debug( "dbscan cluster: clusters = [{}]", sizeClusters map { _.mkString( "\n + [", "; ", "]\n" ) } )
      }

      ( clustersD valueOr { _ ⇒ Seq.empty[Cluster[DoublePoint]] }, ctx )
    }
  }

  val eps: KOp[Context, Double] = {
    Kleisli[ErrorOr, Context, Double] { ctx ⇒
      val config = ctx.messageConfig
      val distanceStatistcs = ctx.distanceStatistics
      log.debug( "distance-statistics=[{}]", distanceStatistcs )

      val calculatedEps = {
        for {
          mean ← if ( distanceStatistcs.getMean.isNaN ) None else Some( distanceStatistcs.getMean )
          stddev ← if ( distanceStatistcs.getStandardDeviation.isNaN ) None else Some( distanceStatistcs.getStandardDeviation )
        } yield {
          ctx.tolerance map { tolerance ⇒
            val t = tolerance getOrElse 3.0
            log.debug( "dist-mean=[{}] dist-stddev=[{}] tolerance=[{}] calc-eps=[{}]", mean, stddev, t, ( mean + t * stddev ) )
            mean + t * stddev
          }
        }
      }

      calculatedEps getOrElse {
        Either catchNonFatal {
          log.debug( "eps seed = {}", config getDouble algorithm + ".seedEps" )
          config getDouble algorithm + ".seedEps"
        }
      }
    }
  }

  def makeOutlierTest( cs: Clusters ): DoublePoint ⇒ Boolean = {
    import scala.collection.JavaConverters._

    if ( cs.isEmpty ) ( pt: DoublePoint ) ⇒ { false }
    else {
      val largestCluster = cs.maxBy { _.getPoints.size }.getPoints.asScala.toSet
      ( pt: DoublePoint ) ⇒ { !largestCluster.contains( pt ) }
    }
  }

  val filterOutliers: KOp[( Clusters, AlgorithmContext ), ( Seq[DataPoint], AlgorithmContext )] = {
    for {
      clustersAndContext ← ask[ErrorOr, ( Clusters, AlgorithmContext )]
      ( clusters, ctx ) = clustersAndContext
    } yield {
      val isOutlier = makeOutlierTest( clusters )
      // since ctx.data is basis only current points are considered to report as anomalies
      val outlyingTestPoints = ctx.data.filter { isOutlier }.map { _.timestamp.toLong }.toSet
      val outliers = ctx.source.points filter { dp ⇒ outlyingTestPoints contains dp.timestamp.getMillis }
      ( outliers, ctx )
    }
  }

  val makeOutliers: KOp[( Seq[DataPoint], AlgorithmContext ), ( Outliers, AlgorithmContext )] = {
    val toContext = Kleisli[ErrorOr, ( Seq[DataPoint], AlgorithmContext ), AlgorithmContext] { case ( _, ctx ) ⇒ ctx.asRight }

    for {
      anomaliesAndContext ← ask[ErrorOr, ( Seq[DataPoint], AlgorithmContext )]
      ( anomalies, ctx ) = anomaliesAndContext
      ts ← toContext andThen thresholds
      ctxWithThresholds = ts.foldLeft( ctx ) { _ addThresholdBoundary _ }
      result ← toContext andThen makeOutliers( anomalies, ctxWithThresholds )
    } yield {
      ( result, ctxWithThresholds )
    }
  }

  val thresholds: KOp[AlgorithmContext, Seq[ThresholdBoundary]] = {
    for {
      ctx ← toConcreteContextK
      e ← toConcreteContextK andThen eps
      distance ← distanceMeasure
      tol ← tolerance
    } yield {
      val distanceStatistics = ctx.distanceStatistics

      if ( !shouldCreateThreshold( ctx ) ) Seq.empty[ThresholdBoundary]
      else {
        val t = tol getOrElse 3.0
        contiguousPairs( ctx ) map {
          case ( p2, p1 ) ⇒ // later point is paired first since primary
            //          val Array( ts2, v2 ) = p2.getPoint

            def extrapolate( label: String, target: Double ): Double ⇒ Double = ( v: Double ) ⇒ {
              distance.compute( p1.toPointA, Array( p2.timestamp, v ) ) - target
            }

            val expectedDistance = distanceStatistics.getMean
            val farthestDistance = distanceStatistics.getMean + t * distanceStatistics.getStandardDeviation
            log.debug( "expectedDistance=[{}]  farthest=[{}]", expectedDistance, farthestDistance )

            val expected = {
              valueSeek( precision = 0.001, maximumSteps = 20, start = p2.value )( extrapolate( "expected", expectedDistance ) )
            }
            val farthest = {
              valueSeek( precision = 0.001, maximumSteps = 20, start = expected )( extrapolate( "farthest", farthestDistance ) )
            }
            log.debug( "actual-value=[{}] expected-value=[{}]  farthest-value=[{}]", p2.value, expected, farthest )
            val result = ThresholdBoundary.fromExpectedAndDistance( timestamp = p2.timestamp.toLong, expected = expected, distance = farthest - expected )
            log.debug(
              "dist-to-expected=[{}] dist-to-floor=[{}] dist-to-ceiling=[{}]",
              result.expected map { e ⇒ extrapolate( "expected", expectedDistance )( e ) },
              result.floor map { f ⇒ extrapolate( "floor", expectedDistance )( f ) },
              result.ceiling map { c ⇒ extrapolate( "ceiling", expectedDistance )( c ) }
            )

            log.debug( "pt1[{}] ~> pt2[{}] threshold:[{}] height:[{}]", p1, p2, result, ( farthest - expected ) )
            result
        }
      }
    }
  }

  /** @param ctx
    * @return pairs of contiguous points including possible last point of previous timeseries. Later point is listed first since
    * it is the primary from interated over the immediate context source.
    */
  def contiguousPairs( ctx: AlgorithmContext ): Seq[( DoublePoint, DoublePoint )] = {
    val points = ctx.source.points.toDoublePoints
    val last = ctx.history.lastPoints.lastOption map { _.toDoublePoint }
    last map { l ⇒ points.zip( l +: points ) } getOrElse { points.drop( 1 ) zip points }
  }

  def shouldCreateThreshold( ctx: AlgorithmContext ): Boolean = {
    val PublishThresholdPath = s"${algorithm}.publish-threshold"
    if ( ctx.messageConfig hasPath PublishThresholdPath ) ctx.messageConfig.getBoolean( PublishThresholdPath ) else false
  }

  def valueSeek( precision: Double, maximumSteps: Int, start: Double )( fn: Double ⇒ Double ): Double = {
    @tailrec def loop( current: Double, last: Double, step: Int = 0 ): Double = {
      if ( maximumSteps <= step ) current
      else if ( math.abs( current - last ) <= precision ) current
      else {
        val next = current - fn( current ) * ( current - last ) / ( fn( current ) - fn( last ) )
        log.debug( "valueSeek next=[{}]", next )
        loop( next, current, step + 1 )
      }
    }

    loop( fn( start ), fn( start + 1 ) )
  }

  /**
    */
  override val findOutliers: KOp[AlgorithmContext, ( Outliers, AlgorithmContext )] = {
    updateDistanceMoment andThen cluster andThen filterOutliers andThen makeOutliers
  }

}
