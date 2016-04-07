package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.ask
import peds.commons.Valid
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, TryV}
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{ControlBoundary, Point2D}


/**
  * Created by rolfsd on 2/25/16.
  */
object HistogramBinsAnalyzer {
  val Algorithm = Symbol( "histogram-bins" )

  def props( router: ActorRef ): Props = Props { new HistogramBinsAnalyzer( router ) }

  //todo rewrite because it's not working. Consider http://stackoverflow.com/questions/10786465/how-to-generate-bins-for-histogram-using-apache-math-3-0-in-java
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

class HistogramBinsAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[CommonAnalyzer.SimpleWrappingContext] {
  import HistogramBinsAnalyzer.{ Histogram, Bin }

  type Context = CommonAnalyzer.SimpleWrappingContext

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = HistogramBinsAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[WrappingContext] = {
    CommonAnalyzer.SimpleWrappingContext( underlying = c ).successNel
  }


  /**
    * A timeseries is anomalous if the average of the last three datapoints
    * on a projected least squares model is greater than three sigma.
    */
    override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      context <- toConcreteContextK <=< ask[TryV, AlgorithmContext]
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
        evaluateOutlier = (p: Point2D, ctx: Context) => {
          val (ts, v) = p
          val isOutlier = h.binFor( p )
          .map { bin =>
            log.debug( "histogram-bins: identified bin[{}] :: size:{} < {}: [{}]", h.binIndexFor(p), bin.size, minimumBinSize, bin )
            bin.size < minimumBinSize
          }
          .getOrElse { v < h.min }

          //todo: outlier based more on frequency than past some threshold, so does control apply?
          ( isOutlier, ControlBoundary.empty(ts.toLong) )
        },
        update = (ctx: Context, pt: Point2D) => { ctx }
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
    val binSize = ( max - min ) / ( numBins + 1).toDouble
    val binData = data groupBy { case (_, v) => ( ( v - min ) / binSize ).toInt }
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
      .map{ bi => (bi._2, bi._1.lowerBoundInclusive, bi._1.upperBoundExclusive, bi._1.points.size) }.mkString(",")
    )

    Histogram( bins, binSize, min, max )
  }

}
