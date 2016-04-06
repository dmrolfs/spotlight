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

import scala.annotation.tailrec



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

  override def historyMean: Op[AlgorithmContext, Double] = kleisli[TryV, AlgorithmContext, Double] { context =>
    context match {
      case contextClassTag( c ) => c.distanceHistory.getMean.right
      case c => c.history.mean( 1 ).right
    }
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
      ctrls <- controls
    } yield {
      val tsTag = ClassTag[TimeSeries]( classOf[TimeSeries] )
      val controlBoundaries = Map( algorithm -> ctrls )
      log.debug( "tsTag = {}", tsTag )
      ctx.source match {
        case tsTag( src ) if os.nonEmpty => {
          SeriesOutliers(
            algorithms = Set(algorithm),
            source = src,
            outliers = os.toIndexedSeq,
            plan = ctx.message.plan,
            algorithmControlBoundaries = controlBoundaries
          )
        }

        case src => {
          NoOutliers(
            algorithms = Set(algorithm),
            source = src,
            plan = ctx.message.plan,
            algorithmControlBoundaries = controlBoundaries
          )
        }
      }
    }
  }

  def controls: Op[(AlgorithmContext, Clusters), Seq[ControlBoundary]] = {
//    val contextCC = kleisli[TryV, (AlgorithmContext, Clusters), Context] { case (ac, _) => ac.right }
    val context = kleisli[TryV, (AlgorithmContext, Clusters), Context] { case (ac, _) => toConcreteContext( ac )}
    val distanceHistory = context map { _.distanceHistory }
    val distanceMeasure = kleisli[TryV, (AlgorithmContext, Clusters), DistanceMeasure] { case (a, _) => a.distanceMeasure }
    val toleranceCC = kleisli[TryV, (AlgorithmContext, Clusters), Option[Double]] { case (ac, _) => ac.tolerance }

    for {
      ctx <- context
      e <- eps <=< kleisli[TryV, (AlgorithmContext, Clusters), AlgorithmContext] { case (ac, _) => ac.right }
      dh <- distanceHistory
      distance <- distanceMeasure
      tolerance <- toleranceCC
    } yield {
      val createControls = {
        val PublishControlsPath = s"${algorithm.name}.publish-controls"
        if ( ctx.messageConfig hasPath PublishControlsPath ) {
          ctx.messageConfig getBoolean PublishControlsPath
        } else {
          false
        }
      }

      if ( !createControls ) Seq.empty[ControlBoundary]
      else {
        val tol = tolerance getOrElse 3D
        val points = DataPoint toDoublePoints ctx.source.points
        val last = ctx.history.lastPoints.lastOption map { new DoublePoint( _ ) }
        log.debug( "last = [{}]", last )
        val pairs = last map { l => points.zip( l +: points ) } getOrElse { (points drop 1).zip( points ) }
        log.debug( "pairs=[{}]", pairs.mkString(",") )
        pairs map { case (p2, p1) =>
          val Array(ts2, v2) = p2.getPoint

          def extrapolate( label: String, target: Double ) = (v: Double) => {
            math.abs( distance.compute(p1.getPoint, Array(ts2, v)) ) - math.abs( target )
          }

          val expectedDistance = dh.getMean
          val farthestDistance = math.abs( dh.getMean + tol * dh.getStandardDeviation )
          log.debug( "expectedDistance=[{}]  farthest=[{}]", expectedDistance, farthestDistance )
          val expected = valueSeek( precision = 0.001, maximumSteps = 20, start = v2 )( extrapolate("expected", expectedDistance) )
          val farthest = valueSeek( precision = 0.001, maximumSteps = 20, start = expected )( extrapolate("farthest", farthestDistance) )
          val result = ControlBoundary.fromExpectedAndDistance( timestamp = ts2.toLong, expected = expected, distance = farthest - expected )

          log.debug(
            "floor=[{}]  ceiling=[{}]",
            extrapolate( "floor", farthestDistance )( result.floor.get ),
            extrapolate( "ceiling", farthestDistance )( result.ceiling.get )
          )

          log.debug( "pt1~>p2={}~>{} cb:{} h={}", p1, p2, result, (farthest - expected) )
          result
        }
      }
    }
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
}
