package spotlight.model.outlier

import org.joda.{time => joda}
import peds.commons.log.Trace
import peds.commons.util._


trait IsQuorum extends Serializable {
  def apply( results: OutlierAlgorithmResults ): Boolean
  def totalIssued: Int

  protected def evaluateRemainder( results: OutlierAlgorithmResults ): Boolean = results.size >= totalIssued
}

object IsQuorum {
  case class AtLeastQuorumSpecification( override val totalIssued: Int, triggerPoint: Int ) extends IsQuorum {
    override def apply( results: OutlierAlgorithmResults ): Boolean = {
      if (
        results.count{ _._2.hasAnomalies } >= triggerPoint &&
        OutlierAlgorithmResults.tallyMax(results) >= triggerPoint
      ) {
        true
      } else {
        evaluateRemainder( results )
      }
    }

    override def toString: String = s"${getClass.safeSimpleName}(trigger:[${triggerPoint}] of total:[${totalIssued}])"
  }

  case class MajorityQuorumSpecification( override val totalIssued: Int, triggerPoint: Double ) extends IsQuorum {
    override def apply( results: OutlierAlgorithmResults ): Boolean = {
      val actual = results.count{ _._2.hasAnomalies }.toDouble / totalIssued.toDouble
      if (
        actual > triggerPoint &&
        ( OutlierAlgorithmResults.tallyMax(results).toDouble / totalIssued.toDouble ) > triggerPoint
      ) {
        true
      } else {
        evaluateRemainder( results )
      }
    }

    override def toString: String = s"${getClass.safeSimpleName}(trigger:[${triggerPoint}]% of total:[${totalIssued}])"
  }
}
