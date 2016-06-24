package spotlight.testkit

import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit._
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import shapeless.syntax.typeable._
import org.joda.{time => joda}
import demesne.AggregateRootModule
import demesne.module.EntityAggregateModule
import demesne.testkit.AggregateRootSpec
import org.apache.commons.math3.random.RandomDataGenerator
import org.scalatest.Tag
import org.scalatest.concurrent.ScalaFutures
import peds.archetype.domain.model.core.Entity
import peds.commons.V
import peds.commons.log.Trace
import spotlight.analysis.outlier.HistoricalStatistics
import spotlight.model.outlier._
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/15/16.
  */
abstract class EntityModuleSpec[E <: Entity : ClassTag] extends AggregateRootSpec[E] with ScalaFutures with StrictLogging {
  private val trace = Trace[EntityModuleSpec[E]]

  type Module <: EntityAggregateModule[E]
  val module: Module

  override type Fixture <: EntityFixture
  abstract class EntityFixture(
    id: Int = AggregateRootSpec.sysId.incrementAndGet(),
    config: Config = demesne.testkit.config
  ) extends AggregateFixture( id, config ) { fixture =>
    logger.info( "Fixture: DomainModel=[{}]", model)

    override def moduleCompanions: List[AggregateRootModule[_]] = List( module )
    logger.debug( "Fixture.context = [{}]", context )
    logger.debug(
      "checkSystem elems: system:[{}] raw:[{}]",
      context.get( demesne.SystemKey ).flatMap{ _.cast[ActorSystem] },
      context.get( demesne.SystemKey )
    )

    val appliesToAll: OutlierPlan.AppliesTo = {
      val isQuorun: IsQuorum = IsQuorum.AtLeastQuorumSpecification(0, 0)
      val reduce: ReduceOutliers = new ReduceOutliers {
        import scalaz._
        override def apply(
          results: OutlierAlgorithmResults,
          source: TimeSeriesBase,
          plan: OutlierPlan
        ): V[Outliers] = Validation.failureNel[Throwable, Outliers]( new IllegalStateException("should not use" ) ).disjunction
      }

      import scala.concurrent.duration._
      val grouping: Option[OutlierPlan.Grouping] = {
        val window = None
        window map { w => OutlierPlan.Grouping( limit = 10000, w ) }
      }

      OutlierPlan.default( "", 1.second, isQuorun, reduce, Set.empty[Symbol], grouping ).appliesTo
    }

    implicit val nowTimestamp: joda.DateTime = joda.DateTime.now

    val bus = TestProbe()
    system.eventStream.subscribe( bus.ref, classOf[module.Event])

    def nextId(): module.TID
    lazy val tid: module.TID = nextId()
    lazy val entity: ActorRef = module aggregateOf tid
  }

  def makeDataPoints(
    values: Seq[Double],
    start: joda.DateTime = joda.DateTime.now,
    period: FiniteDuration = 1.second,
    timeWiggle: (Double, Double) = (1D, 1D),
    valueWiggle: (Double, Double) = (1D, 1D)
  ): Seq[DataPoint] = {
    val random = new RandomDataGenerator

    def nextFactor( wiggle: (Double, Double) ): Double = {
      val (lower, upper) = wiggle
      if ( upper <= lower ) upper else random.nextUniform( lower, upper )
    }

    val secs = start.getMillis / 1000L
    val epochStart = new joda.DateTime( secs * 1000L )

    values.zipWithIndex map { vi =>
      import com.github.nscala_time.time.Imports._
      val (v, i) = vi
      val tadj = ( i * nextFactor(timeWiggle) ) * period
      val ts = epochStart + tadj.toJodaDuration
      val vadj = nextFactor( valueWiggle )
      DataPoint( timestamp = ts, value = (v * vadj) )
    }
  }

  def spike( topic: Topic, data: Seq[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
    val (front, last) = data.sortBy{ _.timestamp.getMillis }.splitAt( position )
    val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
    TimeSeries( topic, spiked )
  }

  def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = trace.block("historyWith") {
    logger.info( "series:{}", series)
    logger.info( "prior={}", prior )
    prior map { h =>
      logger.info( "Adding series to prior history")
      series.points.foldLeft( h ){ _ :+ _ }
    } getOrElse {
      logger.info( "Creating new history from series")
      HistoricalStatistics.fromActivePoints( series.points, false )
    }
  }

  def tailAverageData(
    data: Seq[DataPoint],
    last: Seq[DataPoint] = Seq.empty[DataPoint],
    tailLength: Int
  ): Seq[DataPoint] = {
    val lastPoints = last.drop( last.size - tailLength + 1 ) map { _.value }
    data.map { _.timestamp }
    .zipWithIndex
    .map { case (ts, i ) =>
      val pointsToAverage: Seq[Double] = {
        if ( i < tailLength ) {
          val all = lastPoints ++ data.take( i + 1 ).map{ _.value }
          all.drop( all.size - tailLength )
        } else {
          data
          .map { _.value }
          .slice( i - tailLength + 1, i + 1 )
        }
      }

      ( ts, pointsToAverage )
    }
    .map { case (ts, pts) => DataPoint( timestamp = ts, value = pts.sum / pts.size ) }
  }


  object WIP extends Tag( "wip" )
}
