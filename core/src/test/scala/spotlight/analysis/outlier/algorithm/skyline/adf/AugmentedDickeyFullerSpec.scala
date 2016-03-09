package spotlight.analysis.outlier.algorithm.skyline.adf

import org.apache.commons.math3.linear.MatrixUtils
import org.scalatest._
import peds.commons.log.Trace

import scala.util.Random


/**
  *
  * Created by rolfsd on 3/6/16.
  */
class AugmentedDickeyFullerSpec
extends fixture.WordSpec
with MustMatchers
with ParallelTestExecution {
  val trace = Trace[AugmentedDickeyFullerSpec]

  override type FixtureParam = Fixture

  class Fixture {
    outer =>
    //    def vectorToMatrix( x: Array[Double], rows: Int, cols: Int ): Array[Array[Double]] = {
    //      val input2D = MatrixUtils.createRealMatrix( rows, cols )
    //      for {
    //        n <- 0 until x.size
    //        i = n % rows
    //        j = math.floor( n / rows ).toInt
    //      } { input2D.addToEntry( i, j, x(n) ) }
    //      input2D.getData
    //    }
    //
    //    def matrixApproximatelyEquals( X: Array[Array[Double]], Y: Array[Array[Double]], epsilon: Double ): Boolean = {
    //      val xs = X.flatten
    //      val ys = Y.flatten
    //      xs.zip( ys ).exists{ xy => math.abs( xy._1 - xy._2 ) > epsilon }
    //    }
  }

  def createFixture(): Fixture = new Fixture

  override def withFixture(test: OneArgTest): Outcome = {
    val fixture = createFixture( )
    try {
      test( fixture )
    } finally {
    }
  }

  object WIP extends Tag( "wip" )


  "AugmentedDickeyFuller" should {
    "spot linear trend" taggedAs (WIP) in { f: Fixture =>
      import f._

      val rand = new Random
      val x = ( 0 until 100 ).map { i => ( i + 1 ) + 5 * rand.nextDouble }.toArray
      val adf = AugmentedDickeyFuller( x )
      trace( s"""adf drift = [${adf.zeroPaddedDiff.mkString(",")}]""" )
      trace( s"""adf t = [${adf.statistic}]""" )
      adf.needsDiff mustBe true
    }

  }

  "spot trend with outlier" in { f: Fixture =>
    import f._

    val rand = new Random
    val x = {
      ( 0 until 100 )
      .collect{
        case 50 => 100D
        case i => ( i + 1 ) + 5 * rand.nextDouble
      }.toArray
    }

    val adf = AugmentedDickeyFuller( x )
    trace( s"""adf drift = [${adf.zeroPaddedDiff.mkString(",")}]""" )
    trace( s"""adf statistic = [${adf.statistic}]""" )
    adf.needsDiff mustBe true
  }
}
