package spotlight.analysis.algorithm.statistical

import scala.annotation.tailrec
import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.joda.{time => joda}
import org.apache.commons.math3.stat.descriptive.StatisticalSummary
import org.scalatest.Assertion
import spotlight.analysis.Moment
import spotlight.analysis.algorithm.{AlgorithmModuleSpec, AlgorithmProtocol => P}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 11/28/16.
  */
class ExponentialMovingAverageAlgorithmSpec extends AlgorithmModuleSpec[ExponentialMovingAverageAlgorithmSpec] {
  override type Module = ExponentialMovingAverageAlgorithm.type
  override val defaultModule: Module = ExponentialMovingAverageAlgorithm

  override def createAkkaFixture( test: OneArgTest, config: Config, system: ActorSystem, slug: String ): Fixture = {
    new Fixture( config, system, slug )
  }

  class Fixture( _config: Config, _system: ActorSystem, _slug: String ) extends AlgorithmFixture( _config, _system, _slug ) {
    fixture =>

    override implicit val shapeOrdering: Ordering[module.Shape] = new Ordering[module.Shape] {
      override def compare(lhs: TestShape, rhs: TestShape): Int = {
        if ( lhs == rhs ) 0
        else {
          val l = lhs.asInstanceOf[ExponentialMovingAverageAlgorithm.Shape]
          val r = rhs.asInstanceOf[ExponentialMovingAverageAlgorithm.Shape]
          ( l.statistics.map {_.N}.getOrElse( 0L ) - r.statistics.map {_.N}.getOrElse( 0L ) ).toInt
        }
      }
    }

    override def expectedUpdatedShape( shape: TestShape, event: P.Advanced ): TestShape = shape :+ event.point.value


    def assertShape( result: Option[CalculationMagnetResult], topic: Topic )( s: TestShape ): Assertion = {
      logger.info( "assertState  result:[{}]", result )
      s.alpha mustBe 0.05

      val expectedStats = for { r <- result; rs <- r.statistics } yield ( rs.getN, rs.getMean, rs.getStandardDeviation )

      expectedStats match {
        case None => assert( s.statistics.isEmpty )

        case Some( (size, mean, standardDeviation) ) => {
          assert( s.statistics.isDefined )
          s.statistics.value.N mustBe size
          s.statistics.value.ewma mustBe mean
          s.statistics.value.ewmsd mustBe standardDeviation
        }
      }
    }
  }

  implicit def fromAlpha( alpha: Double ): CalculationMagnet = new CommonCalculationMagnet {
    override def apply( points: Seq[DataPoint] ): Result = {
      Result(
        underlying = Moment.Statistics( 0.05, points.map{ _.value }:_* ),
        timestamp = points.last.timestamp,
        tolerance = 3.0
      )
    }
  }


  override def calculateControlBoundaries(
    points: Seq[DataPoint],
    tolerance: Double,
    lastPoints: Seq[DataPoint]
  ): Seq[ThresholdBoundary] = {
    @tailrec def loop( pts: List[DataPoint], history: Array[Double], acc: Seq[ThresholdBoundary] ): Seq[ThresholdBoundary] = {
      pts match {
        case Nil => acc

        case p :: tail => {
          val moment = history.foldLeft( Moment.withAlpha( 0.05 ).toOption.get ){ _ :+ _ }
          val control = ThresholdBoundary.fromExpectedAndDistance(
            p.timestamp,
            expected = moment.statistics.get.ewma,
            distance = math.abs( tolerance * moment.statistics.get.ewmsd )
          )
          loop( tail, history :+ p.value, acc :+ control )
        }
      }
    }

    loop( points.toList, lastPoints.map{ _.value }.toArray, Seq.empty[ThresholdBoundary] )
  }


  bootstrapSuite()
  analysisStateSuite()

  s"${defaultModule.algorithm.label.name} algorithm" should {
    "find outliers across two batches" taggedAs WIP in { f: Fixture =>
      import f._
      val dp1 = makeDataPoints( values = Seq.fill( 5 )( 1.0 ), timeWiggle = (0.97, 1.03) )
      val s1 = spike( scope.topic, dp1 )()
      val h1 = historyWith( None, s1 )
      logger.debug( "test series = [{}]", s1 )
      val (e1, r1) = makeExpected( 0.05 )( points = s1.points, outliers = Seq(false, false, false, false, true ) )
      logger.info( "TEST: R1[{}]  E1:[{}]", r1, e1 )
      evaluate(
        hint = "first",
        series = s1,
        history = h1,
        expectedResults = e1,
        assertShapeFn = assertShape( r1, scope.topic )( _: TestShape )
      )

      val dp2 = makeDataPoints( values = Seq.fill( 5 )( 1.0 ), timeWiggle = (0.97, 1.03) )
      val s2 = spike( scope.topic, dp2 )( 0 )
      val h2 = historyWith( Option( h1.recordLastPoints( s1.points ) ), s2 )
      val (e2, r2) = makeExpected( 0.05 )(
        points = s2.points,
        outliers = Seq(false, false, false, false, false ),
        history = h2.lastPoints map { _.toDataPoint }
      )
      logger.info( "TEST: R2[{}]  E2:[{}]", r2, e2 )
      evaluate(
        hint = "second",
        series = s2,
        history = h2,
        expectedResults = e2,
        assertShapeFn = assertShape( r2, scope.topic )( _: TestShape )
      )
    }
  }
}
