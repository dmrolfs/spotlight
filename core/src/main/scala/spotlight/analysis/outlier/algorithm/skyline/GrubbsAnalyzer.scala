package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.Valid
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, TryV}
import spotlight.analysis.outlier.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{ControlBoundary, Point2D}


/**
  * Created by rolfsd on 2/25/16.
  */
object GrubbsAnalyzer {
  val Algorithm = 'grubbs

  def props( router: ActorRef ): Props = Props { new GrubbsAnalyzer( router ) }
}

class GrubbsAnalyzer( override val router: ActorRef ) extends CommonAnalyzer[CommonAnalyzer.SimpleWrappingContext] {
  import CommonAnalyzer.SimpleWrappingContext

  type Context = SimpleWrappingContext

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = GrubbsAnalyzer.Algorithm

  override def wrapContext(c: AlgorithmContext ): Valid[WrappingContext] = ( SimpleWrappingContext( c ) ).successNel


  /**
    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
    */
  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    def dataThreshold( data: Seq[Point2D] ) = kleisli[TryV, AlgorithmContext, Double] { context =>
      val Alpha = 0.05  //todo drive from context's algoConfig
      val degreesOfFreedom = math.max( data.size - 2, 1 ) //todo: not a great idea but for now avoiding error if size <= 2
      \/ fromTryCatchNonFatal {
        new TDistribution( degreesOfFreedom ).inverseCumulativeProbability( Alpha / (2D * data.size) )
      }
    }

    // background: http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
    // background: http://graphpad.com/support/faqid/1598/
    val outliers = for {
      context <- toConcreteContextK
      taverages <- tailAverage
      threshold <- dataThreshold( taverages )
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
    } yield {
      val tol = tolerance getOrElse 3D

      val data = taverages.map{ case (_, v) => v }.toArray
      val stats = new DescriptiveStatistics( data )
      val stddev = stats.getStandardDeviation
      val mean = stats.getMean
      // zscore calculation considered in control expected and distance formula
//      val zScores = taverages map { case (ts, v) => ( ts, math.abs(v - mean) / stddev ) }
//      log.debug( "Skyline[Grubbs]: mean:[{}] stddev:[{}] zScores:[{}]", mean, stddev, zScores.mkString(",") )

      val thresholdSquared = math.pow( threshold, 2 )
      log.debug( "Skyline[Grubbs]: threshold^2:[{}]", thresholdSquared )
      val grubbsScore = {
        ((data.size - 1) / math.sqrt(data.size)) * math.sqrt( thresholdSquared / (data.size - 2 + thresholdSquared) )
      }
      log.debug( "Skyline[Grubbs]: Grubbs Score:[{}] tolerance:[{}]", grubbsScore, tol )

      collectOutlierPoints(
        points = taverages,
        context = context,
        evaluateOutlier = (p: Point2D, ctx: Context) => {
          val (ts, v) = p
          val control = ControlBoundary.fromExpectedAndDistance(
            timestamp = ts.toLong,
            expected = mean,
            distance = tol * grubbsScore * stddev
          )

          ( control isOutlier v, control )
        },
        update = (ctx: Context, pt: Point2D) => { ctx }
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
