package lineup.analysis.outlier.algorithm.skyline

import scala.annotation.tailrec
import scala.reflect.ClassTag
import akka.event.LoggingReceive
import scalaz._, scalaz.Scalaz._
import shapeless.syntax.typeable._
import scalaz.Kleisli.kleisli
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.ml.distance.DistanceMeasure
import peds.commons.Valid
import peds.commons.util._
import lineup.analysis.outlier._
import lineup.analysis.outlier.algorithm.AlgorithmActor
import lineup.analysis.outlier.algorithm.AlgorithmActor._
import lineup.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContextError
import lineup.model.outlier.{ OutlierPlan, Outliers }
import lineup.model.timeseries._


/**
  * Created by rolfsd on 2/12/16.
  */
object SkylineAnalyzer {
//  val HistogramBinsAlgorithm = 'histogram_bins
//  val KsTestAlgorithm = 'ks_test

  trait SkylineContext extends AlgorithmContext {
    def underlying: AlgorithmContext
    def withUnderlying( ctx: AlgorithmContext ): Valid[SkylineContext]

    override def message: DetectUsing = underlying.message
    override def data: Seq[DoublePoint] = underlying.data
    override def algorithm: Symbol = underlying.algorithm
    override def topic: Topic = underlying.topic
    override def plan: OutlierPlan = underlying.plan
    override def historyKey: HistoryKey = underlying.historyKey
    override def history: HistoricalStatistics = underlying.history
    override def source: TimeSeriesBase = underlying.source
    override def messageConfig: Config = underlying.messageConfig
    override def distanceMeasure: TryV[DistanceMeasure] = underlying.distanceMeasure
    override def tolerance: TryV[Option[Double]] = underlying.tolerance
  }


  final case class SimpleSkylineContext private[skyline]( override val underlying: AlgorithmContext ) extends SkylineContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[SkylineContext] = copy( underlying = ctx ).successNel
    override def toString: String = s"""${getClass.safeSimpleName}()"""
  }

  // window size = 1d @ 1 pt per 10s
  val ApproximateDayWindow: Int = 6 * 60 * 24


  final case class SkylineContextError private[algorithm]( context: AlgorithmActor.AlgorithmContext )
  extends IllegalStateException( s"Context was not extended for Skyline algorithms: [${context}]" )
}


trait SkylineAnalyzer[C <: SkylineAnalyzer.SkylineContext] extends AlgorithmActor {
  import SkylineAnalyzer.SkylineContext

  implicit val contextClassTag: ClassTag[C]
  def toConcreteContext( actx: AlgorithmContext ): TryV[C] = {
    actx match {
      case contextClassTag( ctx ) => ctx.right
      case ctx => SkylineContextError( ctx ).left
    }
  }

  override def preStart(): Unit = {
    context watch router
    router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm, self )
  }

  override def detect: Receive = LoggingReceive {
    case msg @ DetectUsing( algo, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      val toOutliers = kleisli[TryV, (Outliers, AlgorithmContext), Outliers] { case (o, _) => o.right }

      ( algorithmContext >=> findOutliers >=> toOutliers ).run( msg ) match {
        case \/-( r ) => aggregator ! r
        case -\/( ex ) => {
          log.error(
            ex,
            "failed [{}] analysis on [{}] @ [{}]",
            algo.name,
            payload.plan.name + "][" + payload.topic,
            payload.source.interval
          )
        }
      }
    }
  }

  def findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)]

  var _scopedContexts: Map[HistoryKey, SkylineContext] = Map.empty[HistoryKey, SkylineContext]

  def setScopedContext( c: SkylineContext ): Unit = { _scopedContexts += c.historyKey -> c }
  def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext]
  def preStartContext( context: AlgorithmContext, priorContext: SkylineContext ): TryV[SkylineContext] = {
    priorContext.withUnderlying( context ).disjunction.leftMap{ _.head }
  }

  override val algorithmContext: Op[DetectUsing, AlgorithmContext] = {
    val toSkyline = kleisli[TryV, AlgorithmContext, AlgorithmContext] { c =>
      _scopedContexts
      .get( c.historyKey )
      .map { priorContext => preStartContext( c, priorContext ) }
      .getOrElse {
        val context = makeSkylineContext( c )
        context foreach { setScopedContext }
        context.disjunction.leftMap{ _.head }
      }
    }

    super.algorithmContext >=> toSkyline
  }

  def toSkylineContext: Op[AlgorithmContext, C] = {
    kleisli { context =>
      context match {
        case contextClassTag( ctx ) => ctx.right
        case _ => SkylineAnalyzer.SkylineContextError( context ).left
      }
    }
  }

  val tailAverage: Op[AlgorithmContext, Seq[Point2D]] = Kleisli[TryV, AlgorithmContext, Seq[Point2D]] { context =>
    val data = context.data.map{ _.getPoint.apply( 1 ) }
    val last = context.history.lastPoints.drop( context.history.lastPoints.size - 2 ) map { case Array(_, v) => v }
    log.debug( "tail-average: last=[{}]", last.mkString(",") )

    val TailLength = 3

    context.data
    .map{ _.getPoint.apply( 0 ) }
    .zipWithIndex
    .map { case (ts, i) =>
      val pointsToAverage = if ( i < TailLength ) {
        val all = last ++ data.take( i + 1 )
        all.drop( all.size - TailLength )
      } else {
        data.drop( i - TailLength + 1 ).take( TailLength )
      }

      ( ts, pointsToAverage )
    }
    .map { case (ts, pts) =>
      log.debug( "points to tail average ({}, [{}]) = {}", ts.toLong, pts.mkString(","), pts.sum / pts.size )
      (ts, pts.sum / pts.size)
    }
    .right
  }


  type UpdateContext[CTX <: AlgorithmContext] = (CTX, DataPoint) => CTX
  type IsOutlier[CTX <: AlgorithmContext] = (Point2D, CTX) => Boolean

  def collectOutlierPoints[CTX <: AlgorithmContext](
    points: Seq[Point2D],
    context: CTX,
    isOutlier: IsOutlier[CTX],
    update: UpdateContext[CTX]
  ): (Row[DataPoint], AlgorithmContext) = {
    @tailrec def loop( pts: List[Point2D], ctx: CTX, acc: Row[DataPoint] ): (Row[DataPoint], AlgorithmContext) = {
      ctx.cast[SkylineContext] foreach {setScopedContext }

      log.debug( "{} checking pt [{}] for outlier = {}", ctx.algorithm, pts.headOption.map{p=>(p._1.toLong, p._2)}, pts.headOption.map{ p => isOutlier(p,ctx) } )
      pts match {
        case Nil => ( acc, ctx )

        case h :: tail if isOutlier( h, ctx ) => {
          val (ts, _) = h
          val original = ctx.source.points find { _.timestamp.getMillis == ts.toLong }
          val (updatedContext, updatedAcc) = original map { orig =>
            ( update(ctx, orig), acc :+ orig  )
          } getOrElse {
            (ctx, acc)
          }

          log.debug( "LOOP-HIT[({})]: updated skyline-context=[{}] acc=[{}]", (ts.toLong, h._2), updatedContext, updatedAcc )
          loop( tail, updatedContext, updatedAcc )
        }

        case h :: tail => {
          val (ts, _) = h
          val updatedContext = {
            ctx.source.points
            .find { _.timestamp.getMillis == ts.toLong }
            .map { orig => update( ctx, orig ) }
            .getOrElse { ctx }
          }

          log.debug( "LOOP-MISS[({})]: updated skyline-context=[{}] acc=[{}]", (ts.toLong, h._2), updatedContext, acc )
          loop( tail, updatedContext, acc )
        }
      }
    }

    loop( points.toList, context, Row.empty[DataPoint] )
  }

  def makeOutliersK(
    algorithm: Symbol,
    outliers: Op[AlgorithmContext, (Row[DataPoint], AlgorithmContext)]
  ): Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {

    def makeOutliers( os: Row[DataPoint] ): Op[AlgorithmContext, Outliers] = {
      kleisli[TryV, AlgorithmContext, Outliers] { context =>
        Outliers.forSeries(
          algorithms = Set( algorithm ),
          plan = context.plan,
          source = context.source,
          outliers = os
        )
        .disjunction
        .leftMap { _.head }
      }
    }

    for {
      outliersContext <- outliers
      (outlierPoints, context) = outliersContext
      result <- makeOutliers( outlierPoints )
    } yield (result, context)
  }
}
