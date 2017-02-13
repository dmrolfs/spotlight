package spotlight.testkit

import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.actor.{ ActorRef, ActorSystem }
import com.typesafe.config.Config
import org.joda.{ time ⇒ joda }
import org.apache.commons.math3.random.RandomDataGenerator
import org.scalatest.concurrent.ScalaFutures
import com.persist.logging._
import peds.archetype.domain.model.core.{ Entity, EntityIdentifying }
import peds.commons.V
import peds.commons.log.Trace
import demesne.AggregateRootType
import demesne.module.entity.{ EntityAggregateModule }
import demesne.testkit.AggregateRootSpec
import spotlight.analysis.HistoricalStatistics
import spotlight.analysis.shard.CellShardModule
import spotlight.model.outlier._
import spotlight.model.timeseries._

/** Created by rolfsd on 6/15/16.
  */
abstract class EntityModuleSpec[E <: Entity: ClassTag] extends AggregateRootSpec[E] with ScalaFutures {
  private val trace = Trace[EntityModuleSpec[E]]

  override type ID = E#ID

  override def testConfiguration( test: OneArgTest, slug: String ): Config = spotlight.testkit.config( "core", slug )

  override type Fixture <: EntityFixture

  abstract class EntityFixture( _config: Config, _system: ActorSystem, _slug: String )
      extends AggregateFixture( _config, _system, _slug ) { fixture ⇒

    var loggingSystem: LoggingSystem = _

    override def before( test: OneArgTest ): Unit = {
      super.before( test )
      loggingSystem = LoggingSystem( _system, s"Test:${getClass.getName}", "1", "localhost" )
    }

    type Module <: EntityAggregateModule[E]
    val module: Module

    override def rootTypes: Set[AggregateRootType] = Set( module.rootType, CellShardModule.module.rootType )

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

      AnalysisPlan.default( "", 1.second, isQuorun, reduce, Set.empty[String], grouping ).appliesTo
    }

    implicit val nowTimestamp: joda.DateTime = joda.DateTime.now

    //    val bus = TestProbe()
    //    system.eventStream.subscribe( bus.ref, classOf[AggregateRootModule.Event[module.ID]])

    val identifying: EntityIdentifying[E]

    override def nextId(): TID = identifying.safeNextId
    override lazy val tid: TID = nextId()
    lazy val entity: ActorRef = trace.block( "entity" ) {
      module aggregateOf tid
    }
  }

  def makeDataPoints(
    values: Seq[Double],
    start: joda.DateTime = joda.DateTime.now,
    period: FiniteDuration = 1.second,
    timeWiggle: ( Double, Double ) = ( 1D, 1D ),
    valueWiggle: ( Double, Double ) = ( 1D, 1D )
  ): Seq[DataPoint] = {
    val random = new RandomDataGenerator

    def nextFactor( wiggle: ( Double, Double ) ): Double = {
      val ( lower, upper ) = wiggle
      if ( upper <= lower ) upper else random.nextUniform( lower, upper )
    }

    val secs = start.getMillis / 1000L
    val epochStart = new joda.DateTime( secs * 1000L )

    values.zipWithIndex map { vi ⇒
      import com.github.nscala_time.time.Imports._
      val ( v, i ) = vi
      val tadj = ( i * nextFactor( timeWiggle ) ) * period
      val ts = epochStart + tadj.toJodaDuration
      val vadj = nextFactor( valueWiggle )
      DataPoint( timestamp = ts, value = ( v * vadj ) )
    }
  }

  def spike( topic: Topic, data: Seq[DataPoint], value: Double = 1000D )( position: Int = data.size - 1 ): TimeSeries = {
    val ( front, last ) = data.sortBy { _.timestamp.getMillis }.splitAt( position )
    val spiked = ( front :+ last.head.copy( value = value ) ) ++ last.tail
    TimeSeries( topic, spiked )
  }

  def historyWith( prior: Option[HistoricalStatistics], series: TimeSeries ): HistoricalStatistics = trace.block( "historyWith" ) {
    logger.info( "series:{}", series )
    logger.info( "prior={}", prior )
    prior map { h ⇒
      logger.info( "Adding series to prior history" )
      series.points.foldLeft( h ) { _ :+ _ }
    } getOrElse {
      logger.info( "Creating new history from series" )
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
      .map {
        case ( ts, i ) ⇒
          val pointsToAverage: Seq[Double] = {
            if ( i < tailLength ) {
              val all = lastPoints ++ data.take( i + 1 ).map { _.value }
              all.drop( all.size - tailLength )
            } else {
              data
                .map { _.value }
                .slice( i - tailLength + 1, i + 1 )
            }
          }

          ( ts, pointsToAverage )
      }
      .map { case ( ts, pts ) ⇒ DataPoint( timestamp = ts, value = pts.sum / pts.size ) }
  }
}
