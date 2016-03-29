package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import akka.actor.{ActorRef, Props}

import scalaz._
import Scalaz._
import scalaz.Kleisli.ask
import peds.commons.Valid
import spotlight.analysis.outlier.algorithm.AlgorithmActor.{AlgorithmContext, Op, TryV}
import spotlight.analysis.outlier.algorithm.skyline.SkylineAnalyzer.SkylineContext
import spotlight.model.outlier.Outliers
import spotlight.model.timeseries.{ControlBoundary, Point2D}


/**
  * Created by rolfsd on 2/25/16.
  */
object MeanSubtractionCumulationAnalyzer {
  val Algorithm = Symbol( "mean-subtraction-cumulation" )

  def props( router: ActorRef ): Props = Props { new MeanSubtractionCumulationAnalyzer( router ) }
}

class MeanSubtractionCumulationAnalyzer( override val router: ActorRef )
extends SkylineAnalyzer[SkylineAnalyzer.SimpleSkylineContext] {
  import SkylineAnalyzer.SimpleSkylineContext

  type Context = SimpleSkylineContext

  override implicit val contextClassTag: ClassTag[Context] = ClassTag( classOf[Context] )

  override def algorithm: Symbol = MeanSubtractionCumulationAnalyzer.Algorithm

  override def makeSkylineContext( c: AlgorithmContext ): Valid[SkylineContext] = SimpleSkylineContext( underlying = c).successNel


  /**
    * A timeseries is anomalous if the Z score is greater than the Grubb's score.
    */
  override val findOutliers: Op[AlgorithmContext, (Outliers, AlgorithmContext)] = {
    val outliers = for {
      context <- toSkylineContext <=< ask[TryV, AlgorithmContext]
      tolerance <- tolerance <=< ask[TryV, AlgorithmContext]
    } yield {
      val tol = tolerance getOrElse 3D

      collectOutlierPoints(
        points = context.source.pointsAsPairs,
        context = context,
        evaluateOutlier = (p: Point2D, ctx: Context) => {
          val (ts, v) = p
          val control = ControlBoundary.fromExpectedAndDistance(
            timestamp = ts.toLong,
            expected = ctx.history.mean( 1 ),
            distance = math.abs( tol * ctx.history.standardDeviation(1) )
          )

          ( control isOutlier v, control )
//          val cumulativeMean = ctx.history.mean( 1 )
//          val cumulativeStddev = ctx.history.standardDeviation( 1 )
//          math.abs( v - cumulativeMean ) > ( tol * cumulativeStddev )
        },
        update = (ctx: Context, pt: Point2D) => { ctx }
      )
    }

    makeOutliersK( algorithm, outliers )
  }
}
