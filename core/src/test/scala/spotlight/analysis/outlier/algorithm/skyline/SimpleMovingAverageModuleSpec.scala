package spotlight.analysis.outlier.algorithm.skyline

import spotlight.model.outlier.OutlierPlan
import spotlight.analysis.outlier.algorithm.{AlgorithmModule, AlgorithmModuleSpec}



/**
  * Created by rolfsd on 6/9/16.
  */
class SimpleMovingAverageModuleSpec extends AlgorithmModuleSpec[SimpleMovingAverageModuleSpec] {

  override val module: AlgorithmModule = SimpleMovingAverageModule

  class Fixture extends AlgorithmFixture {
    override def nextId(): module.TID = OutlierPlan.Scope( plan = "TestPlan", topic = "TestTopic", planId = OutlierPlan.nextId() )
  }

  override def createAkkaFixture(): Fixture = new Fixture

  bootstrapSuite()


  override def analysisStateSuite: AnalysisStateSuite = new AnalysisStateSuite {
    override def expectedUpdatedState( state: module.State, event: module.AnalysisState.Advanced ): module.State = {
      val s = super.expectedUpdatedState( state, event ).asInstanceOf[SimpleMovingAverageModule.State]
      val h = s.history
      h.movingStatistics addValue event.point.value
      s.copy( history = h ).asInstanceOf[module.State]
    }
  }

  analysisStateSuite()
}
