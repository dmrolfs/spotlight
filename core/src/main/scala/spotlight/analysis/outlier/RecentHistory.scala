package spotlight.analysis.outlier

import spotlight.model.timeseries.PointA


/**
  * Created by rolfsd on 10/3/16.
  */
case class RecentHistory( lastPoints: Seq[PointA] = Seq.empty[PointA], retention: Int = RecentHistory.LastN ) {
  def withPoints( newPoints: Seq[PointA] ): RecentHistory = {
    val recorded = newPoints drop ( newPoints.size - retention )
    this.copy( lastPoints = this.lastPoints.drop(this.lastPoints.size - retention + recorded.size) ++ recorded )
  }
}

object RecentHistory {
  /**
    * Number of points in a day assuming 6pts / sec rate
    */
  val LastN: Int = 6 * 60 * 24
}
