package spotlight.analysis.outlier.algorithm.density

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.Valid
import peds.commons.log.Trace
import peds.commons.math.MahalanobisDistance
import spotlight.analysis.outlier.algorithm.AlgorithmActor._
import spotlight.analysis.outlier.algorithm.density.DBSCANAnalyzer.Clusters
import spotlight.analysis.outlier.{DetectUsing, HistoricalStatistics, HistoryKey}
import spotlight.model.outlier.{NoOutliers, OutlierPlan, Outliers, SeriesOutliers}
import spotlight.model.timeseries._



/**
 * Created by rolfsd on 9/29/15.
 */
object SeriesDensityAnalyzer extends LazyLogging {
  private val trace = Trace[SeriesDensityAnalyzer.type]
  val Algorithm: Symbol = 'dbscanSeries
  def props( router: ActorRef ): Props = Props { new SeriesDensityAnalyzer( router ) }


  final case class Context private[algorithm](
    underlying: AlgorithmContext,
    distanceHistory: DescriptiveStatistics
  ) extends AlgorithmContext {
    logger.debug( s"CONTEXT CTOR: underlying.N=[${underlying.history.N}]\tdistanceHistory.N=[${distanceHistory.getN}]" )
    def withUnderlying( ctx: AlgorithmContext ): Valid[Context] = {
      makeDistanceHistory( ctx, Some(distanceHistory) ) map { dh => copy( underlying = ctx, distanceHistory = dh ) }
    }

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addControlBoundary( control: ControlBoundary ): That = copy(underlying = underlying.addControlBoundary(control))

    override def message: DetectUsing = underlying.message
    override def algorithm: Symbol = underlying.algorithm
    override def topic: Topic = underlying.topic
    override def plan: OutlierPlan = underlying.plan
    override def historyKey: HistoryKey = underlying.historyKey
    override def data: Seq[DoublePoint] = underlying.data
    override def history: HistoricalStatistics = underlying.history
    override def source: TimeSeriesBase = underlying.source
    override def messageConfig: Config = underlying.messageConfig
    override def distanceMeasure: TryV[DistanceMeasure] = underlying.distanceMeasure
    override def tolerance: TryV[Option[Double]] = underlying.tolerance
    override def controlBoundaries: Seq[ControlBoundary] = underlying.controlBoundaries
  }

  def makeDistanceHistory( c: AlgorithmContext, prior: Option[DescriptiveStatistics] ): Valid[DescriptiveStatistics] = trace.block( "makeDistanceHistory" ) {
    c.distanceMeasure.validationNel map { distance =>
      val past = prior getOrElse { new DescriptiveStatistics() }
      logger.debug( "series density past distance history = {}", past )

      // if no last value then start distance stats from head
      val points = DataPoint toDoublePoints c.source.points
      val last = c.history.lastPoints.lastOption map { new DoublePoint( _ ) }
      logger.debug( "history basis last: [{}]", last )
      val basis = last map { l => points.zip( l +: points ) } getOrElse { (points drop 1).zip( points ) }

      basis.foldLeft( past ) { case (h, (cur, prev)) =>
        val ts = cur.getPoint.head
        val dist = distance.compute( prev.getPoint, cur.getPoint )

        //todo cleanup
        import shapeless.syntax.typeable._
        distance.cast[MahalanobisDistance] foreach { mahal => logger.debug( "distance: N:[{}] covariance:{}", mahal.dimension.toString, mahal.covariance.toString ) }
        logger.debug(
          "dist prev:[{}] current:[{}] calculated distance: {}",
          s"(${prev.getPoint.apply(0).toLong}, ${prev.getPoint.apply(1)})",
          s"(${cur.getPoint.apply(0).toLong}, ${cur.getPoint.apply(1)})",
          dist.toString
        )
        if ( !dist.isNaN ) h.addValue( dist )
        h
      }
    }
  }

  final case class DensityContextError private[algorithm]( context: AlgorithmContext )
  extends IllegalStateException( s"Context was not extended for series density algorithm: [${context}]" )
}

class SeriesDensityAnalyzer( override val router: ActorRef ) extends DBSCANAnalyzer {
  import SeriesDensityAnalyzer.Context

  override val algorithm: Symbol = SeriesDensityAnalyzer.Algorithm

  def makeDensityContext( c: AlgorithmContext ): Valid[Context] = {
    SeriesDensityAnalyzer.makeDistanceHistory( c, None ) map { dh => Context( underlying = c, distanceHistory = dh ) }
  }

  var _scopedContexts: Map[HistoryKey, Context] = Map.empty[HistoryKey, Context]
  def setScopedContext( c: Context ): Unit = { _scopedContexts += c.historyKey -> c }

  def preStartContext( context: AlgorithmContext, priorContext: Context ): TryV[Context] = {
    priorContext.withUnderlying( context ).disjunction.leftMap{ _.head }
  }

  override def algorithmContext: Op[DetectUsing, AlgorithmContext] = {
    val toDensity = kleisli[TryV, AlgorithmContext, AlgorithmContext] { c =>
      _scopedContexts
      .get( c.historyKey )
      .map { priorContext => preStartContext( c, priorContext ) }
      .getOrElse {
        val context = makeDensityContext( c )
        context foreach { setScopedContext }
        context.disjunction.leftMap{ _.head }
      }
    }

    super.algorithmContext >=> toDensity
  }

  override def historyStandardDeviation: Op[AlgorithmContext, Double] = {
    kleisli[TryV, AlgorithmContext, Double] { context =>
      context match {
        case contextClassTag( c ) => c.distanceHistory.getStandardDeviation.right
        case c => c.history.standardDeviation( 1 ).right
      }
    }
  }

  implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  def toConcreteContext( context: AlgorithmContext ): TryV[Context] = {
    context match {
      case contextClassTag( ctx ) => ctx.right
      case _ => SeriesDensityAnalyzer.DensityContextError( context ).left
    }
  }

  def toDensityContext: Op[AlgorithmContext, Context] = kleisli { toConcreteContext }

  override def findOutliers: Op[(AlgorithmContext, Clusters), Outliers] = {
    val outliers = {
      for {
        contextAndClusters <- Kleisli.ask[TryV, (AlgorithmContext, Clusters)]
        (context, clusters) = contextAndClusters
        isOutlier = makeOutlierTest( clusters )
      } yield {
        for {
          dp <- context.data if isOutlier( dp )
          o <- context.source.points if o.timestamp.getMillis == dp.getPoint.head.toLong
        } yield o
      }
    }

    for {
      contextAndClusters <- ask[TryV, (AlgorithmContext, Clusters)]
      (ctx, _) = contextAndClusters
      os <- outliers
    } yield {
      val tsTag = ClassTag[TimeSeries]( classOf[TimeSeries] )
      log.debug( "tsTag = {}", tsTag )
      ctx.source match {
        case tsTag( src ) if os.nonEmpty => {
          SeriesOutliers(
            algorithms = Set(algorithm),
            source = src,
            outliers = os.toIndexedSeq,
            plan = ctx.message.plan
          )
        }

        case src => NoOutliers( algorithms = Set(algorithm), source = src, plan = ctx.message.plan )
      }
    }
  }
}
