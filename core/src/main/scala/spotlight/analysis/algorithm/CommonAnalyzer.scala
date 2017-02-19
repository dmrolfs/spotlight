package spotlight.analysis.algorithm

import scala.annotation.tailrec
import scala.reflect.ClassTag

import scalaz.Kleisli.kleisli
import scalaz.Scalaz._
import scalaz._
import shapeless.syntax.typeable._
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.joda.{ time ⇒ joda }
import omnibus.commons.{ KOp, TryV, Valid }
import omnibus.commons.util._
import spotlight.analysis._
import spotlight.analysis.algorithm.AlgorithmActor.{ AlgorithmContext ⇒ OLD_AlgorithmContext }
import spotlight.model.outlier.{ NoOutliers, AnalysisPlan, Outliers }
import spotlight.model.timeseries._

/** Created by rolfsd on 2/12/16.
  */
@deprecated( "replaced by AlgorithmModule and AlgorithmModule.AlgorithmProtocol", "v2" )
object CommonAnalyzer {
  trait WrappingContext extends OLD_AlgorithmContext {
    def underlying: OLD_AlgorithmContext
    def withUnderlying( ctx: OLD_AlgorithmContext ): Valid[WrappingContext]

    override def message: DetectUsing = underlying.message
    override def data: Seq[DoublePoint] = underlying.data
    override def algorithm: String = underlying.algorithm
    override def topic: Topic = underlying.topic
    override def plan: AnalysisPlan = underlying.plan
    override def historyKey: AnalysisPlan.Scope = underlying.historyKey
    override def history: HistoricalStatistics = underlying.history
    override def source: TimeSeriesBase = underlying.source
    override def messageConfig: Config = underlying.messageConfig
    override def distanceMeasure: TryV[DistanceMeasure] = underlying.distanceMeasure
    override def tolerance: TryV[Option[Double]] = underlying.tolerance
    override def thresholdBoundaries: Seq[ThresholdBoundary] = underlying.thresholdBoundaries
  }

  val DefaultTailAverageLength: Int = 1

  final case class SimpleWrappingContext private[algorithm] ( override val underlying: OLD_AlgorithmContext ) extends WrappingContext {
    override def withUnderlying( ctx: OLD_AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = SimpleWrappingContext
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addThresholdBoundary( threshold: ThresholdBoundary ): That = {
      copy( underlying = underlying.addThresholdBoundary( threshold ) )
    }

    override def toString: String = s"""${getClass.safeSimpleName}()"""
  }

  // window size = 1d @ 1 pt per 10s
  val ApproximateDayWindow: Int = 6 * 60 * 24

  final case class CommonContextError private[algorithm] ( context: OLD_AlgorithmContext )
    extends IllegalStateException( s"Context was not extended for Skyline algorithms: [${context}]" )
}

@deprecated( "replaced by AlgorithmModule and AlgorithmModule.AlgorithmProtocol", "v2" )
trait CommonAnalyzer[C <: CommonAnalyzer.WrappingContext] extends AlgorithmActor {
  import CommonAnalyzer.WrappingContext

  implicit val contextClassTag: ClassTag[C]
  def toConcreteContext( actx: OLD_AlgorithmContext ): TryV[C] = {
    actx match {
      case contextClassTag( ctx ) ⇒ ctx.right
      case ctx ⇒ CommonAnalyzer.CommonContextError( ctx ).left
    }
  }

  override def preStart(): Unit = {
    context watch router
    router ! DetectionAlgorithmRouter.RegisterAlgorithmReference( algorithm, self )
  }

  override def detect: Receive = {
    case msg @ DetectUsing( _, algo, payload: DetectOutliersInSeries, history, algorithmConfig ) ⇒ {
      val aggregator = sender()
      log.info( "TEST: AGGREGATOR=[{}]", aggregator )
      val toOutliers = kleisli[TryV, ( Outliers, OLD_AlgorithmContext ), Outliers] {
        case ( o, _ ) ⇒
          //        logOutlierToDebug( o )
          o.right
      }

      val start = System.currentTimeMillis()
      ( algorithmContext >=> findOutliers >=> toOutliers ).run( msg ) match {
        case \/-( r ) ⇒ {
          log.debug( "sending detect result to aggregator[{}]: [{}]", aggregator.path, r )
          algorithmTimer.update( System.currentTimeMillis() - start, scala.concurrent.duration.MILLISECONDS )
          aggregator ! r
        }
        case -\/( ex ) ⇒ {
          log.error(
            ex,
            "failed [{}] analysis on [{}] @ [{}]",
            algo,
            payload.plan.name + "][" + payload.topic,
            payload.source.interval
          )
          // don't let aggregator time out just due to error in algorithm
          aggregator ! NoOutliers(
            algorithms = Set( algorithm ),
            source = payload.source,
            plan = payload.plan,
            thresholdBoundaries = Map.empty[String, Seq[ThresholdBoundary]]
          )
        }
      }
    }
  }

  def findOutliers: KOp[OLD_AlgorithmContext, ( Outliers, OLD_AlgorithmContext )]

  //todo dmr place into Agent for use & concurrency *across* actor instances?
  var _scopedContexts: Map[AnalysisPlan.Scope, WrappingContext] = Map.empty[AnalysisPlan.Scope, WrappingContext]

  def setScopedContext( c: WrappingContext ): Unit = { _scopedContexts += c.historyKey → c }
  def wrapContext( c: OLD_AlgorithmContext ): Valid[WrappingContext]
  def preStartContext( context: OLD_AlgorithmContext, priorContext: WrappingContext ): TryV[WrappingContext] = {
    log.debug( "preStartContext: [{}]", context )
    priorContext.withUnderlying( context ).disjunction.leftMap { _.head }
  }

  override val algorithmContext: KOp[DetectUsing, OLD_AlgorithmContext] = {
    val wrap = kleisli[TryV, OLD_AlgorithmContext, OLD_AlgorithmContext] { c ⇒
      _scopedContexts
        .get( c.historyKey )
        .map { priorContext ⇒ preStartContext( c, priorContext ) }
        .getOrElse {
          val context = wrapContext( c )
          context foreach { setScopedContext }
          context.disjunction.leftMap { _.head }
        }
    }

    super.algorithmContext >=> wrap
  }

  def toConcreteContextK: KOp[OLD_AlgorithmContext, C] = kleisli { toConcreteContext }

  def tailAverage(
    data: Seq[DoublePoint],
    tailLength: Int = CommonAnalyzer.DefaultTailAverageLength
  ): KOp[OLD_AlgorithmContext, Seq[PointT]] = {
    kleisli[TryV, OLD_AlgorithmContext, Seq[PointT]] { ctx ⇒
      val values = data map { _.value }
      val lastPos: Int = {
        data.headOption
          .map { h ⇒ ctx.history.lastPoints indexWhere { _.timestamp == h.timestamp } }
          .getOrElse { ctx.history.lastPoints.size }
      }

      val last = ctx.history.lastPoints.drop( lastPos - tailLength + 1 ) map { _.value }
      log.debug( "tail-average: last=[{}]", last.mkString( "," ) )

      data
        .map { _.timestamp }
        .zipWithIndex
        .map {
          case ( ts, i ) ⇒
            val pointsToAverage = {
              if ( i < tailLength ) {
                val all = last ++ values.take( i + 1 )
                all.drop( all.size - tailLength )
              } else {
                values.drop( i - tailLength + 1 ).take( tailLength )
              }
            }

            ( ts, pointsToAverage )
        }
        .map {
          case ( ts, pts ) ⇒
            log.debug( "points to tail average ({}, [{}]) = {}", ts.toLong, pts.mkString( "," ), pts.sum / pts.size )
            ( ts, pts.sum / pts.size )
        }
        .right
    }
  }

  type UpdateContext[CTX <: OLD_AlgorithmContext] = ( CTX, PointT ) ⇒ CTX
  type EvaluateOutlier[CTX <: OLD_AlgorithmContext] = ( PointT, CTX ) ⇒ ( Boolean, ThresholdBoundary )

  def collectOutlierPoints[CTX <: OLD_AlgorithmContext](
    points: Seq[PointT],
    analysisContext: CTX,
    evaluateOutlier: EvaluateOutlier[CTX],
    update: UpdateContext[CTX]
  ): ( Seq[DataPoint], OLD_AlgorithmContext ) = {
    val currentTimestamps = points.map { _.timestamp }.toSet
    @inline def isCurrentPoint( pt: PointT ): Boolean = currentTimestamps contains pt.timestamp

    @tailrec def loop( pts: List[PointT], ctx: CTX, acc: Seq[DataPoint] ): ( Seq[DataPoint], OLD_AlgorithmContext ) = {
      ctx.cast[WrappingContext] foreach { setScopedContext }

      pts match {
        case Nil ⇒ ( acc, ctx )

        case pt :: tail ⇒ {
          val timestamp = pt.timestamp
          val ( isOutlier, threshold ) = evaluateOutlier( pt, ctx )

          val ( updatedAcc, updatedContext ) = {
            ctx.data
              .find { _.timestamp == timestamp }
              .map { original ⇒
                log.debug( "PT:[{}] ORIGINAL:[{}]", pt, original )
                val uacc = if ( isOutlier ) acc :+ original.toDataPoint else acc
                val uctx = update( ctx.addThresholdBoundary( threshold ).asInstanceOf[CTX], pt )
                ( uacc, uctx )
              }
              .getOrElse {
                //todo since pt is not in ctx.data do not add threshold boundary to context but update is okay as long as permanent
                // histories are not modified for past points
                log.debug( "NOT ORIGINAL PT:[{}]", pt )
                ( acc, update( ctx, pt ) )
              }
          }

          log.debug(
            "LOOP-{}[{}]: threshold:[{}] acc:[{}]",
            if ( isOutlier ) "HIT" else "MISS",
            ( pt._1.toLong, pt._2 ),
            threshold,
            updatedAcc.size
          )

          loop( tail, updatedContext, updatedAcc )
        }
      }
    }

    loop( points.toList, analysisContext, Seq.empty[DataPoint] )
  }

  def makeOutliersK(
    outliers: KOp[OLD_AlgorithmContext, ( Seq[DataPoint], OLD_AlgorithmContext )]
  ): KOp[OLD_AlgorithmContext, ( Outliers, OLD_AlgorithmContext )] = {
    for {
      outliersContext ← outliers
      ( outlierPoints, resultingContext ) = outliersContext
      result ← makeOutliers( outlierPoints, resultingContext )
    } yield ( result, resultingContext )
  }

  def makeOutliers( outliers: Seq[DataPoint], resultingContext: OLD_AlgorithmContext ): KOp[OLD_AlgorithmContext, Outliers] = {
    kleisli[TryV, OLD_AlgorithmContext, Outliers] { originalContext ⇒
      Outliers.forSeries(
        algorithms = Set( algorithm ),
        plan = originalContext.plan,
        source = originalContext.source,
        outliers = outliers,
        thresholdBoundaries = Map( algorithm → resultingContext.thresholdBoundaries )
      )
        .disjunction
        .leftMap { _.head }
    }
  }

  private def logOutlierToDebug( o: Outliers ): Unit = {
    val WatchedTopic = "prod-las.em.authz-proxy.1.proxy.p95"
    def acknowledge( t: Topic ): Boolean = t.name == WatchedTopic

    if ( acknowledge( o.source.topic ) ) {
      import org.slf4j.LoggerFactory
      import com.typesafe.scalalogging.Logger

      val debugLogger = Logger( LoggerFactory getLogger "Debug" )

      debugLogger.info(
        """
          |OUTLIER:[{}] [{}] original points: [{}]
          |    OUTLIER Thresholds:[{}]
        """.stripMargin,
        o.plan.name + ":" + WatchedTopic, o.source.points.size.toString, o.hasAnomalies.toString,
        o.thresholdBoundaries.map { case ( a, t ) ⇒ a + ":" + t.mkString( "[", ", ", "]" ) }.mkString( "\n\tOUTLIER: {", "\n", "\n\tOUTLIER: }" )
      )
    }
  }
}
