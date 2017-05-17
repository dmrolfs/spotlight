package spotlight.analysis.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ ActorRef, Props }

import cats.instances.either._
import cats.syntax.validated._
import omnibus.commons.{ KOp, AllIssuesOr }
import spotlight.analysis.algorithm.AlgorithmActor.AlgorithmContext
import spotlight.analysis.algorithm.CommonAnalyzer
import CommonAnalyzer.WrappingContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries._

/** Created by rolfsd on 2/25/16.
  */
object MeanSubtractionCumulationAnalyzer {
  val Algorithm: String = "mean-subtraction-cumulation"

  def props( router: ActorRef ): Props = Props { new MeanSubtractionCumulationAnalyzer( router ) }
}

class MeanSubtractionCumulationAnalyzer( override val router: ActorRef )
    extends CommonAnalyzer[CommonAnalyzer.SimpleWrappingContext] {
  import CommonAnalyzer.SimpleWrappingContext

  type Context = SimpleWrappingContext

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: String = MeanSubtractionCumulationAnalyzer.Algorithm

  override def wrapContext( c: AlgorithmContext ): AllIssuesOr[WrappingContext] = SimpleWrappingContext( underlying = c ).validNel

  /** A timeseries is anomalous if the Z score is greater than the Grubb's score.
    */
  override val findOutliers: KOp[AlgorithmContext, ( Outliers, AlgorithmContext )] = {
    val outliers = for {
      ctx ← toConcreteContextK
      tolerance ← tolerance
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = ctx.source.points,
        analysisContext = ctx,
        evaluateOutlier = ( p: PointT, c: Context ) ⇒ {
        val threshold = ThresholdBoundary.fromExpectedAndDistance(
          timestamp = p.timestamp.toLong,
          expected = c.history.mean( 1 ),
          distance = math.abs( tol * c.history.standardDeviation( 1 ) )
        )

        ( threshold isOutlier p.value, threshold )
        //          val cumulativeMean = ctx.history.mean( 1 )
        //          val cumulativeStddev = ctx.history.standardDeviation( 1 )
        //          math.abs( v - cumulativeMean ) > ( tol * cumulativeStddev )
      },
        update = ( c: Context, p: PointT ) ⇒ { c }
      )
    }

    makeOutliersK( outliers )
  }
}
