package lineup.train

import scala.concurrent.{ Promise, Future }
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task

import lineup.model.outlier.Outliers
import lineup.model.timeseries.{ TimeSeries, TimeSeriesCohort }


/**
  * Created by rolfsd on 12/7/15.
  */
object TrainOutlierAnalysis {
  def taskToFuture[A]( task: Task[A] ): Future[A] = {
    val p = Promise[A]()
    task.unsafePerformAsync {
      case \/-(a) => p success a
      case -\/(ex) => p failure ex
    }
    p.future
  }

  def feedTrainingFlow(
    interpreter: TrainingRepository.Interpreter
  )(
    implicit system: ActorSystem
  ): Flow[Outliers, Outliers, Unit] = {
    Flow[Outliers]
    .map {o =>
      val protocol = o.source match {
        case s: TimeSeries => TrainingRepository putSeries s
        case c: TimeSeriesCohort => TrainingRepository putCohort c
      }
      (protocol, o)
    }
    .mapAsync( Runtime.getRuntime.availableProcessors( ) ) { case (p, o) => taskToFuture( interpreter( p ).map{ _ => o } ) }
  }
}
