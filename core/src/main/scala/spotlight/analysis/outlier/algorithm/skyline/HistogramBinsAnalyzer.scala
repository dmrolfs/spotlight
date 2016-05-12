package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import peds.commons.{KOp, Valid}
import spotlight.analysis.outlier.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 2/25/16.
  */
object HistogramBinsAnalyzer {
  val Algorithm = Symbol( "histogram-bins" )

  def props( router: ActorRef ): Props = Props { new HistogramBinsAnalyzer( router ) }

  //todo rewrite because it's not working. Consider http://stackoverflow.com/questions/10786465/how-to-generate-bins-for-histogram-using-apache-math-3-0-in-java
  final case class Histogram private[skyline]( bins: IndexedSeq[Bin], binSize: Double, min: Double, max: Double ) {
    def binFor( p: PointT ): Option[Bin] = bins.lift( binIndexFor( p ) )
    def binIndexFor( p: PointT ): Int = ( ( p.value - min ) / binSize ).toInt
  }

  final case class Bin private[skyline](
    lowerBoundInclusive: Double,
    upperBoundExclusive: Double,
    points: Map[Double, Double] = Map.empty[Double, Double]
  ) {
    def size: Int = points.size
  }

}

class HistogramBinsAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[CommonAnalyzer.SimpleWrappingContext] {
  import HistogramBinsAnalyzer.{ Histogram, Bin }

  type Context = CommonAnalyzer.SimpleWrappingContext

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = HistogramBinsAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = {
    CommonAnalyzer.SimpleWrappingContext( underlying = c ).successNel
  }


  /**
    * A timeseries is anomalous if the average of the last three datapoints
    * on a projected least squares model is greater than three sigma.
    */
    override val findOutliers: KOp[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      ctx <- toConcreteContextK
      taverages <- tailAverage( ctx.data )
      tolerance <- tolerance
    } yield {
      val tol = tolerance getOrElse 3D

      val MinBinSize = "minimum-bin-size"
      val config = ctx.messageConfig
      val minimumBinSize = if ( config.hasPath(MinBinSize) ) config.getInt( MinBinSize ) else 5

      //todo: not sure why skyline creates a histogram from raw data then compares 3-pt average against histogram
      // easy case of 3-pt avg falling into a 0-size bin
      val h = histogram( ctx.data )()

      collectOutlierPoints(
        points = taverages,
        analysisContext = ctx,
        evaluateOutlier = (p: PointT, c: Context) => {
          val isOutlier = {
            h.binFor( p )
            .map { bin =>
              log.debug( "histogram-bins: identified bin[{}] :: size:{} < {}: [{}]", h.binIndexFor(p), bin.size, minimumBinSize, bin )
              bin.size < minimumBinSize
            }
            .getOrElse { p.value < h.min }
          }

          //todo: outlier based more on frequency than past some threshold, so does threshold apply?
          ( isOutlier, ThresholdBoundary.empty( p.timestamp.toLong ) )
        },
        update = (c: Context, p: PointT) => { c }
      )
    }

    makeOutliersK( algorithm, outliers )
  }

  def histogram(
    data: Seq[(Double, Double)]
  )(
    numBins: Int = 15,
    min: Double = data.map{ _.value }.min,
    max: Double = data.map{ _.value }.max
  ): Histogram = {
    val binSize = ( max - min ) / ( numBins + 1 ).toDouble
    val binData = data groupBy { d => ( ( d.value - min ) / binSize ).toInt }
    val bins = ( 0 to numBins ) map { i =>
      val binPoints = binData.get( i ) map { pts => Map( pts:_* ) }

      Bin(
        lowerBoundInclusive = min + i * binSize,
        upperBoundExclusive = min + ( i + 1 ) * binSize,
        points = binPoints getOrElse { Map.empty[Double, Double] }
      )
    }

    log.debug(
      "histogram bins = [{}]",
      bins
      .zipWithIndex
      .map { bi => (bi._2, bi._1.lowerBoundInclusive, bi._1.upperBoundExclusive, bi._1.points.size) }.mkString(",")
    )

    Histogram( bins, binSize, min, max )
  }

}
