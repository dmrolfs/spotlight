package spotlight.analysis.algorithm.skyline

import akka.actor.ActorSystem

import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import spotlight.analysis.HistoricalStatistics
import spotlight.model.outlier._
import spotlight.model.timeseries.{ DataPoint, TimeSeries, TimeSeriesBase }
import spotlight.testkit.ParallelAkkaSpec
import org.apache.commons.math3.random.RandomDataGenerator
import org.joda.{ time ⇒ joda }
import org.mockito.Mockito._
import org.scalatest.Tag
import org.scalatest.mockito.MockitoSugar
import omnibus.commons.V
import spotlight.analysis.algorithm.CommonAnalyzer

/** Created by rolfsd on 2/15/16.
  */
abstract class SkylineBaseSpec extends ParallelAkkaSpec with MockitoSugar {
  object SkylineFixture {
    val appliesToAll: AnalysisPlan.AppliesTo = {
      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification( 0, 0 )
      val reduce: ReduceOutliers = new ReduceOutliers {
        import scalaz._
        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: AnalysisPlan
        ): V[Outliers] = Validation.failureNel[Throwable, Outliers]( new IllegalStateException( "should not use" ) ).disjunction
      }

      import scala.concurrent.duration._
      val grouping: Option[AnalysisPlan.Grouping] = {
        val window = None
        window map { w ⇒ AnalysisPlan.Grouping( limit = 10000, w ) }
      }

      AnalysisPlan.default( "", 1.second, isQuorun, reduce, Map.empty[String, Config], grouping ).appliesTo
    }
  }

  abstract class SkylineFixture( _config: Config, _system: ActorSystem, _slug: String )
      extends AkkaFixture( _config, _system, _slug ) {

    implicit def scalaDurationToJoda( d: FiniteDuration ): joda.Duration = new joda.Duration( d.toMillis )

    val router = TestProbe()
    val aggregator = TestProbe()
    val subscriber = TestProbe()

    def makeDataPoints(
      values: Seq[Double],
      start: joda.DateTime = joda.DateTime.now,
      period: FiniteDuration = 1.second,
      timeWiggle: ( Double, Double ) = ( 1D, 1D ),
      valueWiggle: ( Double, Double ) = ( 1D, 1D )
    ): Seq[DataPoint] = {
      val secs = start.getMillis / 1000L
      val epochStart = new joda.DateTime( secs * 1000L )
      val random = new RandomDataGenerator
      def nextFactor( wiggle: ( Double, Double ) ): Double = {
        val ( lower, upper ) = wiggle
        if ( upper <= lower ) upper else random.nextUniform( lower, upper )
      }

      values.zipWithIndex map { vi ⇒
        import com.github.nscala_time.time.Imports._
        val ( v, i ) = vi
        val tadj = ( i * nextFactor( timeWiggle ) ) * period
        val ts = epochStart + tadj.toJodaDuration
        val vadj = nextFactor( valueWiggle )
        DataPoint( timestamp = ts, value = ( v * vadj ) )
      }
    }

    def spike( data: Seq[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
      val ( front, last ) = data.sortBy { _.timestamp.getMillis }.splitAt( position )
      val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
      TimeSeries( "test-series", spiked )
    }

    def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = {
      prior map { h ⇒
        series.points.foldLeft( h ) { _ :+ _ }
      } getOrElse {
        HistoricalStatistics.fromActivePoints( series.points, false )
      }
    }

    def tailAverage(
      data: Seq[DataPoint],
      lastPoints: Seq[DataPoint],
      tailLength: Int = CommonAnalyzer.DefaultTailAverageLength
    ): Seq[DataPoint] = {
      val values = data map { _.value }
      val lastPos: Int = {
        data.headOption
          .map { h ⇒ lastPoints indexWhere { _.timestamp == h.timestamp } }
          .getOrElse { lastPoints.size }
      }

      val last: Seq[Double] = lastPoints.drop( lastPos - tailLength + 1 ) map { _.value }
      log.debug( "tail-average: last=[{}]", last.mkString( "," ) )

      data
        .map { _.timestamp }
        .zipWithIndex
        .map {
          case ( ts, i ) ⇒
            val pointsToAverage: Seq[Double] = {
              if ( i < tailLength ) {
                val all = last ++ values.take( i + 1 )
                all.drop( all.size - tailLength )
              } else {
                values.drop( i - tailLength + 1 ).take( tailLength )
              }
            }

            ( ts, pointsToAverage )
        }
        .map {
          case ( ts, pts ) ⇒
            log.debug( "points to tail average ({}, [{}]) = {}", ts, pts.mkString( "," ), pts.sum / pts.size )
            DataPoint( timestamp = ts, value = pts.sum / pts.size )
        }
    }
  }

  object DONE extends Tag( "done" )
}

object SkylineBaseSpec {
  val points = Seq(
    DataPoint( new joda.DateTime( 440 ), 9.46 ),
    DataPoint( new joda.DateTime( 441 ), 9.9 ),
    DataPoint( new joda.DateTime( 442 ), 11.6 ),
    DataPoint( new joda.DateTime( 443 ), 14.5 ),
    DataPoint( new joda.DateTime( 444 ), 17.3 ),
    DataPoint( new joda.DateTime( 445 ), 19.2 ),
    DataPoint( new joda.DateTime( 446 ), 18.4 ),
    DataPoint( new joda.DateTime( 447 ), 14.5 ),
    DataPoint( new joda.DateTime( 448 ), 12.2 ),
    DataPoint( new joda.DateTime( 449 ), 10.8 ),
    DataPoint( new joda.DateTime( 450 ), 8.58 ),
    DataPoint( new joda.DateTime( 451 ), 8.36 ),
    DataPoint( new joda.DateTime( 452 ), 8.58 ),
    DataPoint( new joda.DateTime( 453 ), 7.5 ),
    DataPoint( new joda.DateTime( 454 ), 7.1 ),
    DataPoint( new joda.DateTime( 455 ), 7.3 ),
    DataPoint( new joda.DateTime( 456 ), 7.71 ),
    DataPoint( new joda.DateTime( 457 ), 8.14 ),
    DataPoint( new joda.DateTime( 458 ), 8.14 ),
    DataPoint( new joda.DateTime( 459 ), 7.1 ),
    DataPoint( new joda.DateTime( 460 ), 7.5 ),
    DataPoint( new joda.DateTime( 461 ), 7.1 ),
    DataPoint( new joda.DateTime( 462 ), 7.1 ),
    DataPoint( new joda.DateTime( 463 ), 7.3 ),
    DataPoint( new joda.DateTime( 464 ), 7.71 ),
    DataPoint( new joda.DateTime( 465 ), 8.8 ),
    DataPoint( new joda.DateTime( 466 ), 9.9 ),
    DataPoint( new joda.DateTime( 467 ), 14.2 ),
    DataPoint( new joda.DateTime( 468 ), 18.8 ),
    DataPoint( new joda.DateTime( 469 ), 25.2 ),
    DataPoint( new joda.DateTime( 470 ), 31.5 ),
    DataPoint( new joda.DateTime( 471 ), 22 ),
    DataPoint( new joda.DateTime( 472 ), 24.1 ),
    DataPoint( new joda.DateTime( 473 ), 39.2 )
  )

  val pointsA = Seq(
    DataPoint( new joda.DateTime( 440 ), 9.46 ),
    DataPoint( new joda.DateTime( 441 ), 9.9 ),
    DataPoint( new joda.DateTime( 442 ), 11.6 ),
    DataPoint( new joda.DateTime( 443 ), 14.5 ),
    DataPoint( new joda.DateTime( 444 ), 17.3 ),
    DataPoint( new joda.DateTime( 445 ), 19.2 ),
    DataPoint( new joda.DateTime( 446 ), 18.4 ),
    DataPoint( new joda.DateTime( 447 ), 14.5 ),
    DataPoint( new joda.DateTime( 448 ), 12.2 ),
    DataPoint( new joda.DateTime( 449 ), 10.8 ),
    DataPoint( new joda.DateTime( 450 ), 8.58 ),
    DataPoint( new joda.DateTime( 451 ), 8.36 ),
    DataPoint( new joda.DateTime( 452 ), 8.58 ),
    DataPoint( new joda.DateTime( 453 ), 7.5 ),
    DataPoint( new joda.DateTime( 454 ), 7.1 ),
    DataPoint( new joda.DateTime( 455 ), 7.3 ),
    DataPoint( new joda.DateTime( 456 ), 7.71 ),
    DataPoint( new joda.DateTime( 457 ), 8.14 ),
    DataPoint( new joda.DateTime( 458 ), 8.14 ),
    DataPoint( new joda.DateTime( 459 ), 7.1 ),
    DataPoint( new joda.DateTime( 460 ), 7.5 ),
    DataPoint( new joda.DateTime( 461 ), 7.1 ),
    DataPoint( new joda.DateTime( 462 ), 7.1 ),
    DataPoint( new joda.DateTime( 463 ), 7.3 ),
    DataPoint( new joda.DateTime( 464 ), 7.71 ),
    DataPoint( new joda.DateTime( 465 ), 8.8 ),
    DataPoint( new joda.DateTime( 466 ), 9.9 ),
    DataPoint( new joda.DateTime( 467 ), 14.2 )
  )

  val pointsB = Seq(
    DataPoint( new joda.DateTime( 440 ), 10.1 ),
    DataPoint( new joda.DateTime( 441 ), 10.1 ),
    DataPoint( new joda.DateTime( 442 ), 9.68 ),
    DataPoint( new joda.DateTime( 443 ), 9.46 ),
    DataPoint( new joda.DateTime( 444 ), 10.3 ),
    DataPoint( new joda.DateTime( 445 ), 11.6 ),
    DataPoint( new joda.DateTime( 446 ), 13.9 ),
    DataPoint( new joda.DateTime( 447 ), 13.9 ),
    DataPoint( new joda.DateTime( 448 ), 12.5 ),
    DataPoint( new joda.DateTime( 449 ), 11.9 ),
    DataPoint( new joda.DateTime( 450 ), 12.2 ),
    DataPoint( new joda.DateTime( 451 ), 13 ),
    DataPoint( new joda.DateTime( 452 ), 13.3 ),
    DataPoint( new joda.DateTime( 453 ), 13 ),
    DataPoint( new joda.DateTime( 454 ), 12.7 ),
    DataPoint( new joda.DateTime( 455 ), 11.9 ),
    DataPoint( new joda.DateTime( 456 ), 13.3 ),
    DataPoint( new joda.DateTime( 457 ), 12.5 ),
    DataPoint( new joda.DateTime( 458 ), 11.9 ),
    DataPoint( new joda.DateTime( 459 ), 11.6 ),
    DataPoint( new joda.DateTime( 460 ), 10.5 ),
    DataPoint( new joda.DateTime( 461 ), 10.1 ),
    DataPoint( new joda.DateTime( 462 ), 9.9 ),
    DataPoint( new joda.DateTime( 463 ), 9.68 ),
    DataPoint( new joda.DateTime( 464 ), 9.68 ),
    DataPoint( new joda.DateTime( 465 ), 9.9 ),
    DataPoint( new joda.DateTime( 466 ), 10.8 ),
    DataPoint( new joda.DateTime( 467 ), 11 )
  )
}