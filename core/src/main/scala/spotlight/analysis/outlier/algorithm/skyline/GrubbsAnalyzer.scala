package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ ActorRef, Props }
import scalaz._, Scalaz._
import scalaz.Kleisli.ask
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import peds.commons.Valid
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{ AlgorithmContext, Op, Point2D, TryV }
import spotlight.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{ DataPoint, Row }


/**
  * Created by rolfsd on 2/25/16.
  */
object GrubbsAnalyzer {
  val Algorithm = 'grubbs

  def props( router: ActorRef ): Props = Props { new GrubbsAnalyzer( router ) }
}

class GrubbsAnalyzer( override val router: ActorRef ) extends SkylineAnalyzer[SkylineAnalyzer.SimpleSkylineContext] {
  import SkylineAnalyzer.SimpleSkylineContext

  type Context = SimpleSkylineContext

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = GrubbsAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = ( SimpleSkylineContext( c ) ).successNel


  /**
    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
    */
  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    // background: http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
    // background: http://graphpad.com/support/faqid/1598/
    val outliers: Op[AlgorithmContext, (Row[DataPoint], AlgorithmContext)] = for {
      context <- toSkylineContext <=< ask[TryV, AlgorithmContext]
      taverages <- tailAverage <=< ask[TryV, AlgorithmContext]
    } yield {
      val data = taverages.map{ case (_, v) => v }.toArray
      val stats = new DescriptiveStatistics( data )
      val stddev = stats.getStandardDeviation
      val mean = stats.getMean
      val zScores = taverages map { case (ts, v) => ( ts, math.abs(v - mean) / stddev ) }
      log.debug( "Skyline[Grubbs]: mean:[{}] stddev:[{}] zScores:[{}]", mean, stddev, zScores.mkString(",") )

      val Alpha = 0.05  //todo drive from context's algoConfig
      val threshold = new TDistribution( data.size - 2 ).inverseCumulativeProbability( Alpha / ( 2D * data.size ) )
      val thresholdSquared = math.pow( threshold, 2 )
      log.debug( "Skyline[Grubbs]: threshold^2:[{}]", thresholdSquared )
      val grubbsScore = {
        ((data.size - 1) / math.sqrt(data.size)) * math.sqrt( thresholdSquared / (data.size - 2 + thresholdSquared) )
      }
      log.debug( "Skyline[Grubbs]: Grubbs Score:[{}]", grubbsScore )

      collectOutlierPoints(
        points = zScores,
        context = context,
        isOutlier = (p: Point2D, ctx: Context) => {
          log.debug( "{} > {}", (p._1.toLong, p._2), grubbsScore )
          p._2 > grubbsScore
        },
        update = (ctx: Context, pt: DataPoint) => { ctx }
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
