package spotlight.analysis.outlier.algorithm.skyline

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
import peds.commons.Valid
import peds.commons.util._
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, TryV}
import spotlight.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import spotlight.analysis.outlier.algorithm.skyline.adf.AugmentedDickeyFuller
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{DataPoint, Row}


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
  ) extends SkylineContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[SkylineContext] = {
      val boundary = ctx.source.start map { _ - referenceOffset }
      val updatedHistory = boundary map { b => referenceHistory.dropWhile{ _.timestamp < b } } getOrElse { referenceHistory }
      copy( underlying = ctx, referenceHistory = updatedHistory ).successNel
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

class KolmogorovSmirnovAnalyzer( override val router: ActorRef ) extends SkylineAnalyzer[KolmogorovSmirnovAnalyzer.Context] {
  import KolmogorovSmirnovAnalyzer.Context

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = KolmogorovSmirnovAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = {
    referenceOffset( c ) map { offset =>
      log.debug( "makeSkylingContext: [{}]", c )
      Context( underlying = c, referenceOffset = offset, referenceHistory = c.source.points.to[Queue] )
    }
  }

  def referenceOffset( c: AlgorithmContext ): Valid[joda.Duration] = {
    import java.util.concurrent.TimeUnit.MILLISECONDS
    val ReferenceOffset = algorithm.name + ".reference-offset"

    val result = if ( c.messageConfig.hasPath( ReferenceOffset ) ) {
      new joda.Duration( c.messageConfig.getDuration(ReferenceOffset, MILLISECONDS) )
    } else {
      joda.Days.ONE.toStandardDuration
    }

    result.successNel
  }

  /**
  * A timeseries is anomalous if the average of the last three datapoints
  * on a projected least squares model is greater than three sigma.
  */
  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val isDistributionUnlikeReference = kleisli[TryV, Context, Boolean] { implicit context =>
      val reference = context.referenceSeries.map{ _.value }.toArray
      log.debug( "reference-history[{}] = [{}]", context.referenceHistory.size, context.referenceHistory.mkString(",") )
      log.debug( "reference-offset = [{}]", context.referenceOffset )
      log.debug( "reference-interval = [{}]", context.referenceInterval )
      log.debug( "reference-series[{}] = [{}]", reference.size, reference.mkString(",") )
      log.debug( "CURRENT[{}] = [{}]", context.data.size, context.data )
      val current = context.data.map{ _.getPoint }.map{ case Array(_, v) => v }.toArray
      distributionUnlikeReference( current, reference )
    }

    val outliers: Op[AlgorithmContext, (Row[DataPoint], AlgorithmContext)] = for {
      context <- toSkylineContext <=< ask[TryV, AlgorithmContext]
_ = log.debug( "KS CONTEXT = [{}]", context )
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
      unlike <- isDistributionUnlikeReference <=< toSkylineContext <=< ask[TryV, AlgorithmContext]
    } yield {
//      val tol = tolerance getOrElse 3D
      if ( unlike ) (context.source.points, context) else (Row.empty[DataPoint], context)
    }


    makeOutliersK( algorithm, outliers )
  }

  def distributionUnlikeReference(
    series: Array[Double],
    reference: Array[Double]
  )(
    implicit context: AlgorithmContext
  ): TryV[Boolean] = {
    val same = {
      if ( reference.isEmpty ) false.right
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
        log.error( "ks-test internal math error. reference.size:[{}], series.size:[{}]: {}", reference.size, series.size, ex )
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
