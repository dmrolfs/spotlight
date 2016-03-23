package spotlight.model.outlier

import org.joda.{time => joda}
import peds.commons.log.Trace


trait IsQuorum {
  def apply( results: OutlierAlgorithmResults ): Boolean
  def totalIssued: Int

  protected def evaluateRemainder( results: OutlierAlgorithmResults ): Boolean = results.size >= totalIssued
}

object IsQuorum {
  private val trace = Trace[IsQuorum.type]

  case class AtLeastQuorumSpecification( override val totalIssued: Int, triggerPoint: Int ) extends IsQuorum {
    override def apply( results: OutlierAlgorithmResults ): Boolean = trace.block( s"""( ${results.keySet.mkString(",")} )""" ) {
      val algorithmCheck = results.count{ _._2.hasAnomalies } >= triggerPoint
      if ( algorithmCheck && tallyMax(results) >= triggerPoint ) true else evaluateRemainder( results )
    }
  }

  case class MajorityQuorumSpecification( override val totalIssued: Int, triggerPoint: Double ) extends IsQuorum {
    override def apply( results: OutlierAlgorithmResults ): Boolean = trace.block( s"""( ${results.keySet.mkString(",")} )""" ) {
      val actual = results.count{ _._2.hasAnomalies }.toDouble / totalIssued.toDouble
      if ( actual > triggerPoint && ( tallyMax(results).toDouble / totalIssued.toDouble ) > triggerPoint ) true
      else evaluateRemainder( results )
    }
  }

  def tallyMax( results: OutlierAlgorithmResults ): Int = trace.briefBlock( s"""tallyMax( ${results.keySet.mkString(",")} )""" ) {
    val t = tally( results ).toSeq.map{ _._2 }
    if ( t.nonEmpty ) t.max else 0
  }

  def tally( results: OutlierAlgorithmResults ): Map[joda.DateTime, Int] = {
    results
    .toSeq
    .flatMap { case (algorithm, outliers) =>
      outliers match {
        case _: NoOutliers => Seq.empty[(joda.DateTime, Int)]
        case s: SeriesOutliers => s.outliers map { dp => ( dp.timestamp, 1 ) }
      }
    }
    .groupBy { case (ts, c) => ts }
    .mapValues { _.foldLeft( 0 ) { _ + _._2 } }
  }
}
