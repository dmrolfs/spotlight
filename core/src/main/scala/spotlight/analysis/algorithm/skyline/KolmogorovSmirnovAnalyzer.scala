package spotlight.analysis.algorithm.skyline

import scala.collection.immutable.Queue
import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import org.joda.{time => joda}
import com.github.nscala_time.time.Imports._
import org.apache.commons.math3.stat.inference.TestUtils
import org.apache.commons.math3.exception.{InsufficientDataException, MathInternalError}
import peds.commons.{KOp, TryV, Valid}
import peds.commons.util._
import spotlight.analysis.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.analysis.algorithm.skyline.adf.AugmentedDickeyFuller
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 2/25/16.
  */
object KolmogorovSmirnovAnalyzer {
  val Algorithm = Symbol( "ks-test" )

  def props( router: ActorRef ): Props = Props { new KolmogorovSmirnovAnalyzer( router ) }


  final case class Context private[skyline](
    override val underlying: AlgorithmContext,
    referenceOffset: joda.Duration,
    referenceHistory: Queue[DataPoint]
  ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = {
      val boundary = ctx.source.start map { _ - referenceOffset }
      val updatedHistory = boundary map { b => referenceHistory.dropWhile{ _.timestamp < b } } getOrElse { referenceHistory }
      copy( underlying = ctx, referenceHistory = updatedHistory ).successNel
    }

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addThresholdBoundary(threshold: ThresholdBoundary ): That = {
      copy( underlying = underlying.addThresholdBoundary( threshold ) )
    }

    def referenceSeries: Seq[DataPoint] = {
      referenceInterval map { reference =>
        ( referenceHistory ++ underlying.source.points ) filter { rdp => reference contains rdp.timestamp }
      } getOrElse {
        Seq.empty[DataPoint]
      }
    }

    def referenceInterval: Option[joda.Interval] = {
      for {
        start <- underlying.source.start
        end <- underlying.source.end
      } yield ( start - referenceOffset ) to ( end - referenceOffset )
    }

    override def toString: String = s"""${getClass.safeSimpleName}(offset:[${referenceOffset}] history-size:[${referenceHistory.size}])"""
  }
}

class KolmogorovSmirnovAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[KolmogorovSmirnovAnalyzer.Context] {
  import KolmogorovSmirnovAnalyzer.Context

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = KolmogorovSmirnovAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
    referenceOffset( c ) map { offset =>
      log.debug( "makeSkylingContext: [{}]", c )
      Context( underlying = c, referenceOffset = offset, referenceHistory = c.source.points.to[Queue] )
    }
  }

  def referenceOffset( c: AlgorithmContext ): Valid[joda.Duration] = {
    import java.util.concurrent.TimeUnit.MILLISECONDS
    val ReferenceOffset = algorithm.name + ".reference-offset"

    val result = {
      if ( c.messageConfig.hasPath( ReferenceOffset ) ) {
        new joda.Duration( c.messageConfig.getDuration(ReferenceOffset, MILLISECONDS) )
      } else {
        joda.Days.ONE.toStandardDuration
      }
    }

    result.successNel
  }

  /**
  * A timeseries is anomalous if the average of the last three datapoints
  * on a projected least squares model is greater than three sigma.
  */
  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    def isDistributionUnlikeReference( tol: Double ): KOp[Context, Boolean] = {
      kleisli[TryV, Context, Boolean] { implicit ctx =>
        val reference = ctx.referenceSeries.map{ _.value }.toArray
        log.debug( "reference-history[{}] = [{}]", ctx.referenceHistory.size, ctx.referenceHistory.mkString(",") )
        log.debug( "reference-offset = [{}]", ctx.referenceOffset )
        log.debug( "reference-interval = [{}]", ctx.referenceInterval )
        log.debug( "reference-series[{}] = [{}]", reference.size, reference.mkString(",") )
        log.debug( "CURRENT[{}] = [{}]", ctx.data.size, ctx.data )
        val current = ctx.data map { _.value }
        distributionUnlikeReference( current.toArray, reference )
      }
    }

    val outliers = {
      for {
        ctx <- ask[TryV, AlgorithmContext]
        tolerance <- tolerance
        unlike <- isDistributionUnlikeReference( tolerance getOrElse 3D ) <=< toConcreteContextK
      } yield {
        if ( unlike ) ( ctx.source.points, ctx ) else ( Seq.empty[DataPoint], ctx )
      }
    }

    makeOutliersK( outliers )
  }

  def distributionUnlikeReference(
    series: Array[Double],
    reference: Array[Double]
  )(
    implicit context: AlgorithmContext
  ): TryV[Boolean] = {
    val same = {
      if ( reference.isEmpty || series.isEmpty ) false.right
      else {
        for {
          pValue <- \/ fromTryCatchNonFatal { TestUtils.kolmogorovSmirnovTest( reference, series ) }
          testStatisticD <- \/ fromTryCatchNonFatal { TestUtils.kolmogorovSmirnovStatistic( reference, series ) }
        } yield {
          val isStationary = (s: Array[Double]) => {
            val adf = AugmentedDickeyFuller( s ).statistic
            log.debug( "ks-test :: ADF[{}]: adf:[{}]", adf < 0.05, adf )
            adf < 0.05
          }

          log.debug( "ks-test[{}]: p-value=[{}], D-statistic=[{}]", ( pValue < 0.05 ) && ( testStatisticD > 0.5 ), pValue, testStatisticD )
          ( pValue < 0.05 ) && ( testStatisticD > 0.5 ) && isStationary( reference )
        }
      }
    }

    same
    .leftMap {
      case ex: MathInternalError => {
        log.error( "ks-test internal math error. reference[size:{}][{}], series[size:{}][{}]: {}", reference.size, reference.mkString(","), series.size, series.mkString(",") )
        log.error( "ks-test internal math error: {}", ex.getMessage )
        ex
      }

      case ex => ex
    }
    .recover {
      case ex: InsufficientDataException => {
        log.debug( "[{}][{}]: no outliers - ignoring series sample: {}", context.plan, context.topic, ex.getMessage  )
        false
      }
    }
  }
}
