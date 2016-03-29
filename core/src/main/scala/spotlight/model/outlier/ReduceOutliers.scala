package spotlight.model.outlier

import scalaz._
import Scalaz._
import com.typesafe.scalalogging.LazyLogging
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
      val corroboratedTimestamps = tally.collect{ case (dp, c) if isCorroborated( plan )( c ) => dp }.toSet
      logger.debug( "REDUCE corroborated timestamps: [{}]", corroboratedTimestamps.mkString(",") )
      val corroboratedOutliers = source.points filter { dp => corroboratedTimestamps contains dp.timestamp }
      logger.debug( "REDUCE corroborated outlier points: [{}]", corroboratedOutliers.mkString(",") )

      val combinedControls = {
        Map(
          results.toSeq.map{ case (a, o) =>
            ( a, o.algorithmControlBoundaries.get( a ).getOrElse{ Seq.empty[ControlBoundary] } )
          }:_*
        )
      }
      logger.debug( "REDUCE combined controls: [{}]", combinedControls.mkString(",") )

      Outliers.forSeries(
        algorithms = results.keySet,
        plan = plan,
        source = source,
        outliers = corroboratedOutliers,
        algorithmControlBoundaries = combinedControls
      )
    }
  }
}