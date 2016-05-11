package spotlight.analysis.outlier.algorithm.density

import scala.annotation.tailrec
import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}
import org.apache.commons.math3.linear.EigenDecomposition

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import peds.commons.{KOp, TryV, Valid}
import peds.commons.util._
import org.apache.commons.math3.ml.clustering.{Cluster, DBSCANClusterer, DoublePoint}
import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance}
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.math.MahalanobisDistance
import spotlight.analysis.outlier.HistoricalStatistics
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import spotlight.analysis.outlier.algorithm.CommonAnalyzer.WrappingContext


/**
  * Created by rolfsd on 2/25/16.
  */
object SeriesDensityAnalyzer {
  val Algorithm = 'dbscanSeries

  def props( router: ActorRef, alphaValue: Double = 0.05 ): Props = {
    Props {
      new SeriesDensityAnalyzer( router ) with HistoryProvider {
        override val alpha: Double = alphaValue
      }
    }
  }


  final case class Context private[density](
    override val underlying: AlgorithmContext,
    distanceStatistics: DescriptiveStatistics
  ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addControlBoundary( control: ControlBoundary ): That = copy(underlying = underlying.addControlBoundary(control))

    override def toString: String = {
      s"""${getClass.safeSimpleName}(distance-stats:[${distanceStatistics}])"""
    }
  }


  trait HistoryProvider {
    def alpha: Double
  }
}

class SeriesDensityAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[SeriesDensityAnalyzer.Context] {
  outer: SeriesDensityAnalyzer.HistoryProvider =>

  import SeriesDensityAnalyzer.Context

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override val algorithm: Symbol = SeriesDensityAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
    makeMovingStatistics( c ) map { movingStats => Context( underlying = c, distanceStatistics = movingStats ) }
  }

  def makeMovingStatistics( context: AlgorithmContext ): Valid[DescriptiveStatistics] = {
    new DescriptiveStatistics( CommonAnalyzer.ApproximateDayWindow ).successNel
  }


  val distanceMeasure: KOp[AlgorithmContext, DistanceMeasure] = kleisli { _.distanceMeasure }

  val updateDistanceMoment: KOp[AlgorithmContext, AlgorithmContext] = {
    import spotlight.analysis.outlier._

    def distanceIsValid( d: DistanceMeasure, h: HistoricalStatistics ): Boolean = {
      // stinks have to resort to this match. Type class is muted due to instantiating distance measure from configuration.
      d match {
        case m: MahalanobisDistance => mahalanobisValidity.isApplicable(m, h)
        case e: EuclideanDistance => euclideanValidity.isApplicable(e, h)
        case _ => true
      }
    }

    for {
      ctx <- toConcreteContextK
      distance <- distanceMeasure
    } yield {
      if ( !distanceIsValid(distance, ctx.message.history) ) {
        log.info( "updateDistanceMoment: distance covariance matrix has ZERO DETERMINANT topic:[{}]", ctx.message.topic )
        ctx
      } else {
        val distances = contiguousPairs( ctx ) map { case (cur, prev) =>
          val d = distance.compute( cur, prev )
          log.debug( "distance:[{}] for contiguous pairs: [{}, {}]", d, prev.getPoint.mkString("(", ",",")"), cur.getPoint.mkString("(",",",")") )
          d
        }
        val updatedStats = distances.foldLeft( ctx.distanceStatistics ) { (stats, d) =>
          if ( !d.isNaN ) stats addValue d
          stats
        }
        log.debug( "updated distance m:[{}] sd:[{}] stats: [{}]", updatedStats.getMean, updatedStats.getStandardDeviation, updatedStats )
        ctx.copy( distanceStatistics = updatedStats ).asInstanceOf[AlgorithmContext]
      }
    }
  }

  val minDensityPoints: KOp[AlgorithmContext, Int] = {
    kleisli[TryV, AlgorithmContext, Int] { ctx =>
      \/ fromTryCatchNonFatal { ctx.messageConfig getInt algorithm.name + ".minDensityConnectedPoints" }
    }
  }


  type Clusters = Seq[Cluster[DoublePoint]]
  val cluster: KOp[AlgorithmContext, (Clusters, AlgorithmContext)] = {
    for {
      ctx <- toConcreteContextK
      data <- fillDataFromHistory()
      e <- eps <=< toConcreteContextK
      distance <- distanceMeasure
      minPoints <- minDensityPoints
    } yield {
      log.debug( "DBSCAN eps = [{}]", e )
      log.debug( "DBSCAN filled orig:[{}] past:[{}] points=[{}]", ctx.data.size, data.size - ctx.data.size, data.mkString(",") )
//      log.debug( "cluster: context dist-stats=[{}]", ctx.distanceStatistics )
      import scala.collection.JavaConverters._
      val clustersD = \/ fromTryCatchNonFatal {
        new DBSCANClusterer[DoublePoint]( e, minPoints, distance ).cluster( data.asJava ).asScala.toSeq
      }

      if ( log.isDebugEnabled ) {
        val sizeClusters = clustersD map { clusters =>
          clusters map { c => ( c.getPoints.size, c.getPoints.asScala.mkString("[", ", ", "]")  ) }
        }
        log.debug( "dbscan cluster: clusters = [{}]", sizeClusters map { _.mkString( "\n + [", "; ", "]\n" ) } )
      }

      ( clustersD valueOr { _ => Seq.empty[Cluster[DoublePoint]] }, ctx )
    }
  }

  val eps: KOp[Context, Double] = {
    kleisli[TryV, Context, Double] { ctx =>
      val config = ctx.messageConfig
      val distanceStatistcs = ctx.distanceStatistics
      log.debug( "distance-stats=[{}]", distanceStatistcs )

      val calculatedEps = {
        for {
          mean <- if ( distanceStatistcs.getMean.isNaN ) None else Some(distanceStatistcs.getMean)
          stddev <- if ( distanceStatistcs.getStandardDeviation.isNaN ) None else Some(distanceStatistcs.getStandardDeviation)
        } yield {
          ctx.tolerance map { tolerance =>
            val t = tolerance getOrElse 3.0
            log.debug( "dist-mean=[{}] dist-stddev=[{}] tolerance=[{}] calc-eps=[{}]", mean, stddev, t, (mean + t * stddev) )
            mean + t * stddev
          }
        }
      }

      calculatedEps getOrElse {
        \/ fromTryCatchNonFatal {
          log.debug( "eps seed = {}", config getDouble algorithm.name+".seedEps" )
          config getDouble algorithm.name+".seedEps"
        }
      }
    }
  }

  def makeOutlierTest( cs: Clusters ): DoublePoint => Boolean = {
    import scala.collection.JavaConverters._

    if ( cs.isEmpty ) (pt: DoublePoint) => { false }
    else {
      val largestCluster = cs.maxBy{ _.getPoints.size }.getPoints.asScala.toSet
      ( pt: DoublePoint ) => { !largestCluster.contains( pt ) }
    }
  }

  val filterOutliers: KOp[(Clusters, AlgorithmContext), (Seq[DataPoint], AlgorithmContext)] = {
    for {
      clustersAndContext <- ask[ TryV, (Clusters, AlgorithmContext) ]
      (clusters, ctx) = clustersAndContext
    } yield {
      val isOutlier = makeOutlierTest( clusters )
      val outlyingTestPoints = ctx.data.filter{ isOutlier }.map{ _.timestamp.toLong }.toSet
      val outliers = ctx.source.points filter { dp => outlyingTestPoints contains dp.timestamp.getMillis }
      ( outliers, ctx )
    }
  }

  val toOutliers: KOp[(Seq[DataPoint], AlgorithmContext), (Outliers, AlgorithmContext)] = {
    val toContext = kleisli[TryV, (Seq[DataPoint], AlgorithmContext), AlgorithmContext] { case (_, ctx) => ctx.right }

    for {
      anomaliesAndContext <- ask[TryV, (Seq[DataPoint], AlgorithmContext)]
      (anomalies, ctx) = anomaliesAndContext
      cs <- controls <=< toContext
      ctxWithControls = cs.foldLeft( ctx ){ _ addControlBoundary _ }
      result <- makeOutliers(anomalies, ctxWithControls) <=< toContext
    } yield {
      ( result, ctxWithControls )
    }
  }

  val controls: KOp[AlgorithmContext, Seq[ControlBoundary]] = {
    for {
      ctx <- toConcreteContextK
      e <- eps <=< toConcreteContextK
      distance <- distanceMeasure
      tol <- tolerance
    } yield {
      val distanceStatistics = ctx.distanceStatistics

      if ( !shouldCreateControls(ctx) ) Seq.empty[ControlBoundary]
      else {
        val t = tol getOrElse 3.0
        contiguousPairs( ctx ) map { case (p2, p1) => // later point is paired first since primary
//          val Array( ts2, v2 ) = p2.getPoint

          def extrapolate( label: String, target: Double ): Double => Double = (v: Double) => {
            distance.compute( p1.toPointA, Array(p2.timestamp, v) ) - target
          }

          val expectedDistance = distanceStatistics.getMean
          val farthestDistance = distanceStatistics.getMean + t * distanceStatistics.getStandardDeviation
          log.debug( "expectedDistance=[{}]  farthest=[{}]", expectedDistance, farthestDistance )

          val expected = {
            valueSeek( precision = 0.001, maximumSteps = 20, start = p2.value )( extrapolate("expected", expectedDistance) )
          }
          val farthest = {
            valueSeek( precision = 0.001, maximumSteps = 20, start = expected )( extrapolate("farthest", farthestDistance) )
          }
          log.debug( "actual-value=[{}] expected-value=[{}]  farthest-value=[{}]", p2.value, expected, farthest )
          val result = ControlBoundary.fromExpectedAndDistance( timestamp = p2.timestamp.toLong, expected = expected, distance = farthest - expected )
          log.debug(
            "dist-to-expected=[{}] dist-to-floor=[{}] dist-to-ceiling=[{}]",
            result.expected map { e => extrapolate("expected", expectedDistance)( e ) },
            result.floor map { f => extrapolate("floor", expectedDistance)( f ) },
            result.ceiling map { c => extrapolate("ceiling", expectedDistance)( c ) }
          )

          log.debug( "pt1[{}] ~> pt2[{}] control:[{}] height:[{}]", p1, p2, result, (farthest - expected) )
          result
        }
      }
    }
  }


  /**
    *
    * @param ctx
    * @return pairs of contiguous points including possible last point of previous timeseries. Later point is listed first since
    *         it is the primary from interated over the immediate context source.
    */
  def contiguousPairs( ctx: AlgorithmContext ): Seq[(DoublePoint, DoublePoint)] = {
    val points = ctx.source.points.toDoublePoints
    val last = ctx.history.lastPoints.lastOption map { _.toDoublePoint }
    last map { l => points.zip( l +: points ) } getOrElse { points.drop( 1 ) zip points }
  }

  def shouldCreateControls( ctx: AlgorithmContext ): Boolean = {
    val PublishControlsPath = s"${algorithm.name}.publish-controls"
    if ( ctx.messageConfig hasPath PublishControlsPath ) ctx.messageConfig.getBoolean( PublishControlsPath ) else false
  }

  def valueSeek( precision: Double, maximumSteps: Int, start: Double )( fn: Double => Double ): Double = {
    @tailrec def loop( current: Double, last: Double, step: Int = 0 ): Double = {
      if ( maximumSteps <= step ) current
      else if ( math.abs(current - last) <= precision ) current
      else {
        val next = current - fn(current) * (current - last) / ( fn(current) - fn(last) )
        log.debug( "valueSeek next=[{}]", next )
        loop( next, current, step + 1 )
      }
    }

    loop( fn(start), fn(start + 1) )
  }


  /**
    */
  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    updateDistanceMoment >=> cluster >=> filterOutliers >=> toOutliers
  }

}
