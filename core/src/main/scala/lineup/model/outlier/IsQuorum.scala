package lineup.model.outlier

import peds.commons.log.Trace


trait IsQuorum {
  def apply( results: SeriesOutlierResults ): Boolean
  def totalIssued: Int

  protected def evaluateRemainder( results: SeriesOutlierResults ): Boolean = results.size >= totalIssued
}

object IsQuorum {
  val trace = Trace[IsQuorum.type]

  case class AtLeastQuorumSpecification( override val totalIssued: Int, triggerPoint: Int ) extends IsQuorum {
    override def apply( results: SeriesOutlierResults ): Boolean = trace.briefBlock( "AtLeastQurumSpecification()" ) {
      trace( s"anomalies = ${results.count( _._2.hasAnomalies )}" )
      if ( results.count( am => am._2.hasAnomalies ) >= triggerPoint ) true else evaluateRemainder( results )
    }
  }

  case class MajorityQuorumSpecification( override val totalIssued: Int, percentage: Double) extends IsQuorum {
    override def apply( results: SeriesOutlierResults ): Boolean = trace.briefBlock( "MajorityQuorumSpecification()" ) {
      val actual = results.count( am => am._2.hasAnomalies ).toDouble / totalIssued.toDouble
      trace( s"anomalies = ${results.count( _._2.hasAnomalies )}" )
      trace( s"actual [$actual] >= trigger [$percentage] = ${actual >= percentage}" )
      if ( actual >= percentage ) true else evaluateRemainder( results )
    }
  }
}
