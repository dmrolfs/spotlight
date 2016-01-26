package lineup.train

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ Promise, Future }
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task
import lineup.model.timeseries.{ TimeSeries, TimeSeriesBase, TimeSeriesCohort }
import lineup.model.timeseries.TimeSeriesBase.Merging


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

  def feedTrainingFlow[T <: TimeSeriesBase](
    interpreter: TrainingRepository.Interpreter,
    maxPoints: Int,
    batchingWindow: FiniteDuration
  )(
    implicit system: ActorSystem,
    merging: Merging[T]
  ): Flow[T, T, Unit] = {
    Flow[T]
    .groupedWithin( maxPoints, batchingWindow )  // batch points before archiving
    .map {
      _.groupBy{ _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => merging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
    .map { ts =>
      val protocol = ts match {
        case s: TimeSeries => TrainingRepository putSeries s
        case c: TimeSeriesCohort => TrainingRepository putCohort c
      }
      ( protocol, ts )
    }
    .map { case (p, ts) => interpreter( p ).map{ _ => ts }.unsafePerformSync } // avro's DataFileWriter is not thread safe
  }
}
