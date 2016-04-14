package spotlight.analysis.outlier.algorithm

import scala.annotation.tailrec
import scala.reflect.ClassTag
import akka.event.LoggingReceive

import scalaz.Kleisli.kleisli
import scalaz.Scalaz._
import scalaz._
import shapeless.syntax.typeable._
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.joda.{time => joda}
import peds.commons.{KOp, TryV, Valid}
import peds.commons.util._
import spotlight.analysis.outlier._
import spotlight.analysis.outlier.algorithm.AlgorithmActor._
import spotlight.model.outlier.{NoOutliers, OutlierPlan, Outliers}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 2/12/16.
  */
object CommonAnalyzer {
  trait WrappingContext extends AlgorithmContext {
    def underlying: AlgorithmContext
    def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext]

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
    override def controlBoundaries: Seq[ControlBoundary] = underlying.controlBoundaries
  }


  final case class SimpleWrappingContext private[algorithm]( override val underlying: AlgorithmContext ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = SimpleWrappingContext
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addControlBoundary( control: ControlBoundary ): That = {
      copy( underlying = underlying.addControlBoundary(control) )
    }

    override def toString: String = s"""${getClass.safeSimpleName}()"""
  }

  // window size = 1d @ 1 pt per 10s
  val ApproximateDayWindow: Int = 6 * 60 * 24


  final case class CommonContextError private[algorithm](context: AlgorithmActor.AlgorithmContext )
  extends IllegalStateException( s"Context was not extended for Skyline algorithms: [${context}]" )
}


trait CommonAnalyzer[C <: CommonAnalyzer.WrappingContext] extends AlgorithmActor {
  import CommonAnalyzer.WrappingContext

  implicit val contextClassTag: ClassTag[C]
  def toConcreteContext( actx: AlgorithmContext ): TryV[C] = {
    actx match {
      case contextClassTag( ctx ) => ctx.right
      case ctx => CommonAnalyzer.CommonContextError( ctx ).left
    }
  }

  override def preStart(): Unit = {
    context watch router
    router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm, self )
  }

  override def detect: Receive = LoggingReceive {
    case msg @ DetectUsing( algo, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
      val toOutliers = kleisli[TryV, (Outliers, AlgorithmContext), Outliers] { case (o, _) => o.right }

      val start = System.currentTimeMillis()
      ( algorithmContext >=> findOutliers >=> toOutliers ).run( msg ) match {
        case \/-( r ) => {
          log.debug( "sending detect result to aggregator[{}]: [{}]", aggregator.path, r )
          algorithmTimer.update( System.currentTimeMillis() - start, scala.concurrent.duration.MILLISECONDS )
          aggregator ! r
        }
        case -\/( ex ) => {
          log.error(
            ex,
            "failed [{}] analysis on [{}] @ [{}]",
            algo.name,
            payload.plan.name + "][" + payload.topic,
            payload.source.interval
          )
          // don't let aggregator time out just due to error in algorithm
          aggregator ! NoOutliers(
            algorithms = Set(algorithm),
            source = payload.source,
            plan = payload.plan,
            algorithmControlBoundaries = Map.empty[Symbol, Seq[ControlBoundary]]
          )
        }
      }
    }
  }

  def findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)]

  //todo dmr place into Agent for use & concurrency *across* actor instances?
  var _scopedContexts: Map[HistoryKey, WrappingContext] = Map.empty[HistoryKey, WrappingContext]

  def setScopedContext( c: WrappingContext ): Unit = {_scopedContexts += c.historyKey -> c }
  def wrapContext(c: AlgorithmContext ): Valid[WrappingContext]
  def preStartContext( context: AlgorithmContext, priorContext: WrappingContext ): TryV[WrappingContext] = {
    log.debug( "preStartContext: [{}]", context )
    priorContext.withUnderlying( context ).disjunction.leftMap{ _.head }
  }

  override val algorithmContext: KOp[DetectUsing, AlgorithmContext] = {
    val wrap = kleisli[TryV, AlgorithmContext, AlgorithmContext] { c =>
      _scopedContexts
      .get( c.historyKey )
      .map { priorContext => preStartContext( c, priorContext ) }
      .getOrElse {
        val context = wrapContext( c )
        context foreach { setScopedContext }
        context.disjunction.leftMap{ _.head }
      }
    }

    super.algorithmContext >=> wrap
  }

  def toConcreteContextK: KOp[AlgorithmContext, C] = kleisli { toConcreteContext }

  val tailAverage: KOp[AlgorithmContext, Seq[Point2D]] = Kleisli[TryV, AlgorithmContext, Seq[Point2D]] { context =>
    val TailLength = 3

    val data = context.data.map{ _.getPoint.apply( 1 ) }
    val last = context.history.lastPoints.drop( context.history.lastPoints.size - TailLength + 1 ) map { case Array(_, v) => v }
    log.debug( "tail-average: last=[{}]", last.mkString(",") )

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


  type UpdateContext[CTX <: AlgorithmContext] = (CTX, Point2D) => CTX
  type EvaluateOutlier[CTX <: AlgorithmContext] = (Point2D, CTX) => (Boolean, ControlBoundary)

  def collectOutlierPoints[CTX <: AlgorithmContext](
    points: Seq[Point2D],
    context: CTX,
    evaluateOutlier: EvaluateOutlier[CTX],
    update: UpdateContext[CTX]
  ): (Seq[DataPoint], AlgorithmContext) = {
    @tailrec def loop( pts: List[Point2D], ctx: CTX, acc: Seq[DataPoint] ): (Seq[DataPoint], AlgorithmContext) = {
      ctx.cast[WrappingContext] foreach { setScopedContext }

      pts match {
        case Nil => ( acc, ctx )

        case pt :: tail => {
          val ts = new joda.DateTime( pt._1.toLong  )
          val (isOutlier, control) = evaluateOutlier( pt, ctx )
          val updatedAcc = {
            if ( isOutlier ) {
              ctx.source.points
              .find { _.timestamp.getMillis == ts.getMillis }
              .map { original => acc :+ original }
              .getOrElse { acc }
            } else {
              acc
            }
          }

          val updatedContext = update( ctx.addControlBoundary(control).asInstanceOf[CTX], pt ) //todo: not a fan of this cast.

          log.debug( "LOOP ControlBoundaries: [{}]", updatedContext.controlBoundaries.mkString(","))
          log.debug(
            "LOOP-{}[{}]: control:[{}] acc:[{}]",
            if ( isOutlier ) "HIT" else "MISS",
            (pt._1.toLong, pt._2),
            control,
            updatedAcc.size
          )

          loop( tail, updatedContext, updatedAcc )
        }
      }
    }

    loop( points.toList, context, Seq.empty[DataPoint] )
  }

  def makeOutliersK(
    algorithm: Symbol,
    outliers: KOp[AlgorithmContext, (Seq[DataPoint], AlgorithmContext)]
  ): KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    for {
      outliersContext <- outliers
      (outlierPoints, resultingContext) = outliersContext
      result <- makeOutliers( outlierPoints, resultingContext )
    } yield (result, resultingContext)
  }

  def makeOutliers( outliers: Seq[DataPoint], resultingContext: AlgorithmContext ): KOp[AlgorithmContext, Outliers] = {
    kleisli[TryV, AlgorithmContext, Outliers] { originalContext =>
      Outliers.forSeries(
        algorithms = Set( algorithm ),
        plan = originalContext.plan,
        source = originalContext.source,
        outliers = outliers,
        algorithmControlBoundaries = Map( algorithm -> resultingContext.controlBoundaries )
      )
      .disjunction
      .leftMap { _.head }
    }
  }

}
