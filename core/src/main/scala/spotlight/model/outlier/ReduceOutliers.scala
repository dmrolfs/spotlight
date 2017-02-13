package spotlight.model.outlier

import scalaz._
import Scalaz._
import com.typesafe.scalalogging.{ LazyLogging, Logger }
import org.slf4j.LoggerFactory
import org.joda.{ time ⇒ joda }
import peds.commons.{ V, Valid }
import shapeless.syntax.typeable._
import spotlight.model.outlier.ReduceOutliers.CorroboratedReduceOutliers.Check
import spotlight.model.timeseries.{ ThresholdBoundary, DataPoint, TimeSeriesBase, Topic }

trait ReduceOutliers extends Serializable {
  def apply(
    results: OutlierAlgorithmResults,
    source: TimeSeriesBase,
    plan: AnalysisPlan
  ): V[Outliers]
}

object ReduceOutliers extends LazyLogging {
  def byCorroborationPercentage( threshold: Double ): ReduceOutliers = new CorroboratedReduceOutliers {
    override def isCorroborated: Check = ( plan: AnalysisPlan ) ⇒ ( count: Int ) ⇒ {
      ( threshold / 100D * plan.algorithms.size.toDouble ) <= count
    }

    override def toString: String = s"CorroboratedReduceOutliers( required-percent-of-votes-per-point:[${threshold}]% )"
  }

  def byCorroborationCount( minimum: Int ): ReduceOutliers = new CorroboratedReduceOutliers {
    override def isCorroborated: Check = ( plan: AnalysisPlan ) ⇒ ( count: Int ) ⇒ {
      minimum <= count || plan.algorithms.size <= count
    }

    override def toString: String = s"CorroboratedReduceOutliers( required-votes-per-point:[${minimum}] )"
  }

  final case class EmptyResultsError private[outlier] ( plan: AnalysisPlan, topic: Topic )
    extends IllegalStateException( s"ReduceOutliers called on empty results in plan:[${plan.name}] topic:[${topic}]" )

  def checkResults( results: OutlierAlgorithmResults, plan: AnalysisPlan, topic: Topic ): Valid[OutlierAlgorithmResults] = {
    if ( results.nonEmpty ) results.successNel
    else Validation.failureNel( EmptyResultsError( plan, topic ) )
  }

  object CorroboratedReduceOutliers {
    type Check = AnalysisPlan ⇒ Int ⇒ Boolean
  }

  abstract class CorroboratedReduceOutliers extends ReduceOutliers {
    def isCorroborated: CorroboratedReduceOutliers.Check

    override def apply(
      results: OutlierAlgorithmResults,
      source: TimeSeriesBase,
      plan: AnalysisPlan
    ): V[Outliers] = {
      // logger.debug(
      //   "REDUCE before [{}]:[{}]:\n\t+ outliers: [{}]\n\t+ threshold: [{}]",
      //   plan.name,
      //   source.topic,
      //   results.values.cast[SeriesOutliers].map{_.outliers.mkString( ", " )},
      //   results.values.map{_.thresholdBoundaries.mkString( ", " ) }
      // )

      for {
        r ← checkResults( results, plan, source.topic ).disjunction
        result ← reduce( r, source, plan ).disjunction
      } yield result
    }

    private def reduce(
      results: OutlierAlgorithmResults,
      source: TimeSeriesBase,
      plan: AnalysisPlan
    ): Valid[Outliers] = {
      val tally = OutlierAlgorithmResults tally results

      val corroboratedTimestamps = tally.collect { case ( dp, c ) if isCorroborated( plan )( c.size ) ⇒ dp }.toSet
      val corroboratedOutliers = source.points filter { dp ⇒ corroboratedTimestamps contains dp.timestamp }

      val combinedThresholds = {
        Map(
          results.toSeq.map {
            case ( a, o ) ⇒
              ( a, o.thresholdBoundaries.get( a ).getOrElse { Seq.empty[ThresholdBoundary] } )
          }: _*
        )
      }
      // logger.debug( "REDUCE combined threshold: [{}]", combinedThresholds.mkString(",") )

      logDebug( results, source, plan, tally, corroboratedTimestamps, corroboratedOutliers )

      Outliers.forSeries(
        algorithms = results.keySet,
        plan = plan,
        source = source,
        outliers = corroboratedOutliers,
        thresholdBoundaries = combinedThresholds
      )
    }

    private def logDebug(
      results: OutlierAlgorithmResults,
      source: TimeSeriesBase,
      plan: AnalysisPlan,
      tally: Map[joda.DateTime, Set[String]],
      corroboratedTimestamps: Set[joda.DateTime],
      corroboratedOutliers: Seq[DataPoint]
    ): Unit = {
      import com.github.nscala_time.time.Imports._

      val WatchedTopic = "prod-las.em.authz-proxy.1.proxy.p95"
      def acknowledge( t: Topic ): Boolean = t.name == WatchedTopic

      if ( acknowledge( source.topic ) ) {
        val debugLogger = Logger( LoggerFactory getLogger "Debug" )

        debugLogger.info(
          """
            |REDUCE:[{}] outliers[{}] of [{}] total points in [{}] details:
            |    REDUCE: corroborated outlier points: [{}]
            |    REDUCE: tally: [{}]
          """.stripMargin,
          plan.name + ":" + WatchedTopic, corroboratedOutliers.size.toString, source.points.size.toString, source.interval.getOrElse( "" ),
          corroboratedOutliers.mkString( "," ),
          if ( tally.nonEmpty ) tally.toSeq.sortBy { case ( ts, as ) ⇒ ts }.map { case ( ts, as ) ⇒ s"""${ts}: [${as.mkString( ", " )}]""" }.mkString( "\n      REDUCE:  - ", "\n      REDUCE:  - ", "\n" ) else ""
        )
      }
    }
  }
}