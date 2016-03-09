package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.ask
import peds.commons.Valid
import peds.commons.util._
import spotlight.analysis.outlier.Moment
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, Point2D, TryV}
import spotlight.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.DataPoint
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.regression.MillerUpdatingRegression

import scala.annotation.tailrec


/**
  * Created by rolfsd on 2/25/16.
  */
object HistogramBinsAnalyzer {
  val Algorithm = 'histogram_bins

  def props( router: ActorRef ): Props = Props { new HistogramBinsAnalyzer( router ) }


  final case class Histogram private[skyline]( bins: IndexedSeq[Bin], binSize: Double, min: Double, max: Double ) {
    def binFor( p: Point2D ): Option[Bin] = bins.lift( binIndexFor(p) )
    def binIndexFor( p: Point2D ): Int = ( ( p._2 - min ) / binSize ).toInt
  }

  final case class Bin private[skyline](
    lowerBoundInclusive: Double,
    upperBoundExclusive: Double,
    points: Map[Double, Double] = Map.empty[Double, Double]
  ) {
    def size: Int = points.size
  }

}

class HistogramBinsAnalyzer( override val router: ActorRef ) extends SkylineAnalyzer[SkylineAnalyzer.SimpleSkylineContext] {
  import HistogramBinsAnalyzer.{ Histogram, Bin }

  type Context = SkylineAnalyzer.SimpleSkylineContext

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = HistogramBinsAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = {
    SkylineAnalyzer.SimpleSkylineContext( underlying = c ).successNel
  }


  /**
    * A timeseries is anomalous if the average of the last three datapoints
    * on a projected least squares model is greater than three sigma.
    */
    override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, AlgorithmContext]
      taverages <- tailAverage <=< ask[TryV, AlgorithmContext]
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
    } yield {
      val tol = tolerance getOrElse 3D

      val MinBinSize = "minimum-bin-size"
      val config = context.messageConfig
      val minimumBinSize = if ( config.hasPath(MinBinSize) ) config.getInt( MinBinSize ) else 5

      //todo: not sure why skyline creates a histogram from raw data then compares 3-pt average against histogram
      // easy case of 3-pt avg falling into a 0-size bin
      val data = context.data.map{ _.getPoint }.map{ case Array(ts, v) => (ts, v) }
      val h = histogram( data )()

      collectOutlierPoints(
        points = taverages,
        context = context,
        isOutlier = (p: Point2D, ctx: Context) => {
          val (_, v) = p
          h.binFor( p )
          .map { bin =>
            log.debug( "histogram-bins: identified bin[{}] :: size:{} < {}: [{}]", h.binIndexFor(p), bin.size, minimumBinSize, bin )
            bin.size < minimumBinSize
          }
          .getOrElse { v < h.min }
        },
        update = (ctx: Context, pt: DataPoint) => ctx
      )
    }

    makeOutliersK( algorithm, outliers )
  }

  def histogram(
    data: Seq[(Double, Double)]
  )(
    numBins: Int = 15,
    min: Double = data.map{ _._2 }.min,
    max: Double = data.map{ _._2 }.max
  ): Histogram = {
    val binSize = ( max - min ) / numBins
    val binData = data groupBy { case (ts, v) => ( ( v - min ) / binSize ).toInt }
    binData foreach { case (bin, pts) =>
      bin match {
        case b if b < 0 => {
          log.warning( "ignoring points [{}] since value is smaller than recognized minimum [{}]", pts.mkString(","), min )
        }

        case b if numBins <= b => {
          log.warning( "ignoring points [{}] since value is bigger than recognized maximum [{}]", pts.mkString(","), max )
        }

        case b => { /* no op - good */ }
      }
    }

    val bins = ( 0 until numBins ) map { i =>
      val binPoints = binData.get( i ) map { pts => Map( pts:_* ) }

      Bin(
        lowerBoundInclusive = min + i * binSize,
        upperBoundExclusive = min + ( i + 1 ) * binSize,
        points = binPoints getOrElse { Map.empty[Double, Double] }
      )
    }

    Histogram( bins, binSize, min, max )
  }

}
