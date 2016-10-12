package spotlight.analysis.outlier.algorithm.skyline

import scala.annotation.tailrec
import org.joda.{ time => joda }
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import spotlight.analysis.outlier.algorithm.AlgorithmModuleSpec
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries.{DataPoint, ThresholdBoundary}



/**
  * Created by rolfsd on 10/7/16.
  */
class GrubbsAlgorithmSpec extends AlgorithmModuleSpec[GrubbsAlgorithmSpec] {
  override type Module = GrubbsAlgorithm.type
  override val defaultModule: Module = GrubbsAlgorithm

  class Fixture extends AlgorithmFixture {
    val testScope: module.TID = identifying tag OutlierPlan.Scope( plan = "TestPlan", topic = "TestTopic" )

    override implicit val shapeOrdering: Ordering[TestShape] = new Ordering[TestShape] {
      override def compare( lhs: TestShape, rhs: TestShape ): Int = {
        val l = lhs.asInstanceOf[DescriptiveStatistics]
        val r = rhs.asInstanceOf[DescriptiveStatistics]
        if ( l.getN == r.getN && l.getMean == r.getMean && l.getStandardDeviation == r.getStandardDeviation ) 0
        else ( r.getN - l.getN ).toInt
      }
    }

    override def nextId(): module.TID = testScope
  }


  override def createAkkaFixture(tags: OneArgTest): Fixture = new Fixture

  //todo consider moving to AlgorithmModuleSpec as default impl
  override def calculateControlBoundaries(
    points: Seq[DataPoint],
    tolerance: Double,
    lastPoints: Seq[DataPoint]
  ): Seq[ThresholdBoundary] = {
    @tailrec def loop(pts: List[DataPoint], history: Array[Double], acc: Seq[ThresholdBoundary]): Seq[ThresholdBoundary] = {
      pts match {
        case Nil => acc

        case p :: tail => {
          val stats = new DescriptiveStatistics( history )
          val mean = stats.getMean
          val stddev = stats.getStandardDeviation
          val control = ThresholdBoundary.fromExpectedAndDistance(
            p.timestamp,
            expected = mean,
            distance = math.abs( tolerance * stddev )
          )
          logger
          .debug( "EXPECTED for point:[{}] Control [{}] = [{}]", (p.timestamp.getMillis, p.value), acc.size.toString, control )
          loop( tail, history :+ p.value, acc :+ control )
        }
      }
    }

    loop( points.toList, lastPoints.map {_.value}.toArray, Seq.empty[ThresholdBoundary] )
  }

  bootstrapSuite()
  analysisStateSuite()


  //todo consider moving to AlgorithmModuleSpec as default impl
  case class Expected(isOutlier: Boolean, floor: Option[Double], expected: Option[Double], ceiling: Option[Double]) {
    def stepResult(i: Int, intervalSeconds: Int = 10)(implicit start: joda.DateTime): (Boolean, ThresholdBoundary) = {
      (
        isOutlier,
        ThresholdBoundary(
          timestamp = start.plusSeconds( i * intervalSeconds ),
          floor = floor,
          expected = expected,
          ceiling = ceiling
        )
      )
    }
  }

  s"${defaultModule.algorithm.label.name} algorithm" should {
    "step to find anomalies from flat signal" in { f: Fixture =>
      pending
    }
  }
}