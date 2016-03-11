package spotlight.model.outlier


trait IsQuorum {
  def apply( results: OutlierAlgorithmResults ): Boolean
  def totalIssued: Int

  protected def evaluateRemainder( results: OutlierAlgorithmResults ): Boolean = results.size >= totalIssued
}

object IsQuorum {
  case class AtLeastQuorumSpecification( override val totalIssued: Int, triggerPoint: Int ) extends IsQuorum {
    override def apply( results: OutlierAlgorithmResults ): Boolean = {
      if ( results.count( am => am._2.hasAnomalies ) >= triggerPoint ) true else evaluateRemainder( results )
    }
  }

  case class MajorityQuorumSpecification( override val totalIssued: Int, triggerPoint: Double ) extends IsQuorum {
    override def apply( results: OutlierAlgorithmResults ): Boolean = {
      val actual = results.count( am => am._2.hasAnomalies ).toDouble / totalIssued.toDouble
      if ( actual > triggerPoint ) true else evaluateRemainder( results )
    }
  }
}
