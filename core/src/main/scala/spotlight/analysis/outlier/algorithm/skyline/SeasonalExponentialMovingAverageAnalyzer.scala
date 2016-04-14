package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.ask
import org.joda.{time => joda}
import com.github.nscala_time.time.Imports._
import com.typesafe.scalalogging.LazyLogging
import peds.commons.{KOp, TryV, Valid}
import peds.commons.log.Trace
import peds.commons.util._
import spotlight.analysis.outlier.Moment
import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{ControlBoundary, Point2D, TimeSeriesBase}


/**
  * Created by rolfsd on 2/25/16.
  */
object SeasonalExponentialMovingAverageAnalyzer {
  val Algorithm = Symbol( "seasonal-ewma" )

  def props( router: ActorRef ): Props = {
    Props {
      new SeasonalExponentialMovingAverageAnalyzer( router ) with ReferenceProvider {
        override def reference: DateTime = SeasonalExponentialMovingAverageAnalyzer.ClassLoadedReference
      }
    }
  }

  val ClassLoadedReference: joda.DateTime = joda.DateTime.now


  trait ReferenceProvider {
    def reference: joda.DateTime
  }

  abstract class SeasonalModel {
    def seasonStartFor( ts: joda.DateTime ): joda.DateTime
    def binStartFor( ts: joda.DateTime ): joda.DateTime
    def reference: joda.DateTime
    def waveLength: joda.Duration
    def binLength: joda.Duration
    def bins: Int
    def momentAt(ts: joda.DateTime ): Moment

    def withMomentAtDateTime( m: Moment, dt: joda.DateTime ): SeasonalModel
  }

  object SeasonalModel extends LazyLogging {
    def apply(reference: joda.DateTime, waveLength: joda.Duration, bins: Int ): Valid[SeasonalModel] = {
      ( 0 until bins ).map { i => Moment.withAlpha( id = s"seasonal-${i}", alpha = 0.05 ) }
      .toList
      .sequence
      .map { moments =>
        SimpleSeasonalModel(
          reference = reference,
          waveLength = waveLength,
          bins = bins,
          moments = moments.toVector
        )
      }
    }

//    def coarsestStartFor( ts: joda.DateTime, waveLength: joda.Duration ): joda.DateTime = {
//      import scala.concurrent.duration._
//      val d = Duration( waveLength.getMillis, MILLISECONDS ).toCoarsest
//      d.unit match {
//        case MILLISECONDS => ts
//        case MICROSECONDS => new joda.DateTime( ts.getYear, ts.getMonthOfYear, ts.getDayOfMonth, ts.getHourOfDay, ts.getMinuteOfDay, ts.getSecondOfMinute, )
//        case SECONDS =>
//        case MINUTES =>
//        case HOURS =>
//        case _ =>
//      }
//    }
//final def toCoarsest: Duration = {
//  def loop(length: Long, unit: TimeUnit): FiniteDuration = {
//    def coarserOrThis(coarser: TimeUnit, divider: Int) =
//      if (length % divider == 0) loop(length / divider, coarser)
//      else if (unit == this.unit) this
//      else FiniteDuration(length, unit)
//
//    unit match {
//      case DAYS => FiniteDuration(length, unit)
//      case HOURS => coarserOrThis(DAYS, 24)
//      case MINUTES => coarserOrThis(HOURS, 60)
//      case SECONDS => coarserOrThis(MINUTES, 60)
//      case MILLISECONDS => coarserOrThis(SECONDS, 1000)
//      case MICROSECONDS => coarserOrThis(MILLISECONDS, 1000)
//      case NANOSECONDS => coarserOrThis(MICROSECONDS, 1000)
//    }
//  }
//
//  if (unit == DAYS || length == 0) this
//  else loop(length, unit)
//}


    final case class SimpleSeasonalModel private[skyline](
      override val reference: joda.DateTime,
      override val waveLength: joda.Duration,
      override val bins: Int,
      moments: Vector[Moment]
    ) extends SeasonalModel {
      override val binLength: joda.Duration = waveLength / bins

      def binLengths( ts: joda.DateTime ): Double = {
        val diff = {
          if ( reference < ts ) new joda.Interval( reference, ts ).toDurationMillis
          else new joda.Interval( ts, reference ).toDuration.negated.getMillis
        }
        diff.toDouble / binLength.millis.toDouble
      }

      def binFor( ts: joda.DateTime ): Int = {
        val bin = math.floor( binLengths(ts) % bins ).toInt
        if ( bin < 0 ) bins + bin else bin
      }

      override def withMomentAtDateTime( m: Moment, ts: DateTime ): SeasonalModel = {
        copy( moments = moments.updated( binFor(ts), m ) )
      }

      override def seasonStartFor( ts: joda.DateTime ): joda.DateTime = {
        val diff = new joda.Interval( reference, ts ).toDuration
        val waveLengths = math.floor( diff.millis.toDouble / waveLength.millis.toDouble ).toInt
        logger.debug( "seasonStartFor( {} ) => [{}]", Seq[AnyRef](ts, (reference+(waveLength*waveLengths))):_* )
        reference + ( waveLength * waveLengths )
      }


      override def binStartFor( ts: joda.DateTime ): joda.DateTime = {
        val diff = new joda.Interval( reference, ts ).toDuration
        val binLengths = math.floor( diff.millis.toDouble / binLength.millis.toDouble ).toInt
        logger.debug( "seasonStartFor( {} ) => [{}]", Seq[AnyRef](ts, (reference+(binLength*binLengths))):_* )
        reference + ( binLength * binLengths )
      }

      override def momentAt( ts: joda.DateTime ): Moment = moments( binFor( ts ) )
    }
  }


  final case class Context private[skyline](
    override val underlying: AlgorithmContext,
    seasonalModel: SeasonalModel
  ) extends WrappingContext {
    override def withUnderlying( ctx: AlgorithmContext ): Valid[WrappingContext] = copy( underlying = ctx ).successNel

    override type That = Context
    override def withSource( newSource: TimeSeriesBase ): That = {
      val updated = underlying withSource newSource
      copy( underlying = updated )
    }

    override def addControlBoundary( control: ControlBoundary ): That = copy(underlying = underlying.addControlBoundary(control))

    override def toString: String = s"""${getClass.safeSimpleName}(seasonalModel:[${seasonalModel}])"""
  }
}

class SeasonalExponentialMovingAverageAnalyzer(
  override val router: ActorRef
) extends CommonAnalyzer[SeasonalExponentialMovingAverageAnalyzer.Context] {
  outer: SeasonalExponentialMovingAverageAnalyzer.ReferenceProvider =>

  import SeasonalExponentialMovingAverageAnalyzer._

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = SeasonalExponentialMovingAverageAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
    makeSeasonalModel( c ) map { model => Context( underlying = c, seasonalModel = model ) }
  }

  def makeSeasonalModel( context: AlgorithmContext ): Valid[SeasonalModel] = {
    val result = for {
      wl <- waveLength( context ).disjunction
      b <- bins( context ).disjunction
      model <- SeasonalModel( reference = outer.reference, waveLength = wl, bins = b ).disjunction
    } yield model

    result.validation
  }

  def waveLength( c: AlgorithmContext ): Valid[joda.Duration] = {
    import java.util.concurrent.TimeUnit.MILLISECONDS
    val WaveLength = algorithm.name + ".wavelength"

    \/
    .fromTryCatchNonFatal {
      if ( c.messageConfig hasPath WaveLength ) new joda.Duration( c.messageConfig.getDuration(WaveLength, MILLISECONDS) )
      else joda.Days.ONE.toStandardDuration
    }
    .validationNel
  }

  def bins( c: AlgorithmContext ): Valid[Int] = {
    val Bins = algorithm.name + ".bins"
    \/.fromTryCatchNonFatal{ if ( c.messageConfig hasPath Bins ) c.messageConfig getInt Bins else 24 }.validationNel
  }

  /**
    * A timeseries is anomalous if the latest point minus the moving average for its
    * seasonal bin is greater than tolerance * standard deviations of the bin's exponential
    * moving average. This is better for finding anomalies with respect to seasonal periods.
    */
  override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      context <- toConcreteContextK <=< ask[TryV, AlgorithmContext]
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
//      taverages <- tailAverage <=< ask[TryV, AlgorithmContext]
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = context.source.pointsAsPairs,
        context = context,
        evaluateOutlier = (p: Point2D, ctx: Context) => {
          val (ts, v) = p
          ctx.seasonalModel.momentAt( new joda.DateTime( ts.toLong ) ).statistics map { stats =>
            log.debug( "pt:[{}] - bin:[{}] - from seasonal exponential moving average: (mean, stddev):[{}]\ttolerance:[{}]", (ts.toLong, v), ctx.seasonalModel.asInstanceOf[SeasonalModel.SimpleSeasonalModel].binFor(new joda.DateTime(ts.toLong)), (stats.ewma, stats.ewmsd), tol )
            val control = ControlBoundary.fromExpectedAndDistance(
              timestamp = ts.toLong,
              expected = stats.ewma,
              distance = math.abs( tol * stats.ewmsd )
            )
            ( control isOutlier v, control )
            //            math.abs( v - stats.ewma ) > ( tol * stats.ewmsd )
          } getOrElse {
            ( false, ControlBoundary.empty(ts.toLong) )
          }
        },
        update = (ctx: Context, pt: Point2D) => {
          val ts = new joda.DateTime( pt._1.toLong )
          val v = pt._2
          val model = ctx.seasonalModel
          val m = model.momentAt( ts )
          log.debug( "FromSeasonalMovingAverage: adding point ({}, {}) to historical momentAt: [{}]", ts.getMillis, v, m.statistics )
          ctx.copy( seasonalModel = model.withMomentAtDateTime( m :+ v, ts ) )
        }
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
