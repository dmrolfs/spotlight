package spotlight.model.outlier

import scalaz._
import Scalaz._
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory
import org.joda.{time => joda}
import peds.commons.{V, Valid}
import shapeless.syntax.typeable._
import spotlight.model.outlier.ReduceOutliers.CorroboratedReduceOutliers.Check
import spotlight.model.timeseries.{ControlBoundary, DataPoint, TimeSeriesBase, Topic}


trait ReduceOutliers {
  def apply(
    results: OutlierAlgorithmResults,
    source: TimeSeriesBase,
    plan: OutlierPlan
  ): V[Outliers]
}

object ReduceOutliers extends LazyLogging {
  def byCorroborationPercentage( threshold: Double ): ReduceOutliers = new CorroboratedReduceOutliers {
    override def isCorroborated: Check = ( plan: OutlierPlan ) => ( count: Int ) => {
      ( threshold / 100D * plan.algorithms.size.toDouble ) <= count
    }
  }


  def byCorroborationCount( minimum: Int ): ReduceOutliers = new CorroboratedReduceOutliers {
    override def isCorroborated: Check = ( plan: OutlierPlan ) => ( count: Int ) => {
      minimum <= count || plan.algorithms.size <= count
    }
  }



  final case class EmptyResultsError private[outlier]( plan: OutlierPlan, topic: Topic )
  extends IllegalStateException( s"ReduceOutliers called on empty results in plan:[${plan.name}] topic:[${topic}]" )

  def checkResults( results: OutlierAlgorithmResults, plan: OutlierPlan, topic: Topic ): Valid[OutlierAlgorithmResults] = {
    if ( results.nonEmpty ) results.successNel
    else Validation.failureNel( EmptyResultsError( plan, topic ) )
  }

  object CorroboratedReduceOutliers {
    type Check = OutlierPlan => Int => Boolean
  }

  abstract class CorroboratedReduceOutliers extends ReduceOutliers {
    def isCorroborated: CorroboratedReduceOutliers.Check

    override def apply(
      results: OutlierAlgorithmResults,
      source: TimeSeriesBase,
      plan: OutlierPlan
    ): V[Outliers] = {
      logger.debug(
        "REDUCE before [{}]:[{}]:\n\t+ outliers: [{}]\n\t+ controls: [{}]",
        plan.name,
        source.topic,
        results.values.cast[SeriesOutliers].map{_.outliers.mkString( ", " )},
        results.values.map{_.algorithmControlBoundaries.mkString( ", " ) }
      )

      for {
        r <- checkResults( results, plan, source.topic ).disjunction
        result <- reduce( r, source, plan ).disjunction
      } yield result
    }

    private def reduce(
      results: OutlierAlgorithmResults,
      source: TimeSeriesBase,
      plan: OutlierPlan
    ): Valid[Outliers] = {
      val tally = OutlierAlgorithmResults tally results

      val corroboratedTimestamps = tally.collect{ case (dp, c) if isCorroborated( plan )( c.size ) => dp }.toSet
      val corroboratedOutliers = source.points filter { dp => corroboratedTimestamps contains dp.timestamp }

      val combinedControls = {
        Map(
          results.toSeq.map{ case (a, o) =>
            ( a, o.algorithmControlBoundaries.get( a ).getOrElse{ Seq.empty[ControlBoundary] } )
          }:_*
        )
      }
      logger.debug( "REDUCE combined controls: [{}]", combinedControls.mkString(",") )

      logDebug( results, source, plan, tally, corroboratedTimestamps, corroboratedOutliers )

      Outliers.forSeries(
        algorithms = results.keySet,
        plan = plan,
        source = source,
        outliers = corroboratedOutliers,
        algorithmControlBoundaries = combinedControls
      )
    }

    private def logDebug(
      results: OutlierAlgorithmResults,
      source: TimeSeriesBase,
      plan: OutlierPlan,
      tally: Map[joda.DateTime, Set[Symbol]],
      corroboratedTimestamps: Set[joda.DateTime],
      corroboratedOutliers: Seq[DataPoint]
    ): Unit = {
      val WatchedTopic = "prod.em.authz-proxy.1.proxy.p95"
      def acknowledge( t: Topic ): Boolean = t.name == WatchedTopic

      if ( acknowledge(source.topic) ) {
        val debugLogger = Logger( LoggerFactory getLogger "Debug" )

        debugLogger.info(
          """
            |REDUCE:[{}] [{}] outliers[{}] details:
            |    REDUCE: corroborated timestamps: [{}]
            |    REDUCE: corroborated outlier points: [{}]
            |    REDUCE: tally: [{}]
          """.stripMargin,
          WatchedTopic,
          source.interval.getOrElse(""),
          corroboratedOutliers.size,
          corroboratedTimestamps.mkString(","),
          corroboratedOutliers.mkString(","),
          tally.map{ case (ts, as) => s"""${ts}: [${as.mkString(", ")}]""" }.mkString( "\n", "\n    REDUCE:  - ", "\n")
        )
      }
    }
  }
}