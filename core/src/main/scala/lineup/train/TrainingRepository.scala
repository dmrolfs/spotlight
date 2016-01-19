package lineup.train

import scalaz.concurrent.Task
import scalaz.{ Free, Functor }
import scalaz.Free._
import lineup.model.timeseries.{ TimeSeriesCohort, TimeSeries }


/**
  * Created by rolfsd on 1/17/16.
  */
trait TrainingRepository {
  import TrainingRepository._

  def putSeries( series: TimeSeries ): TrainingProtocol[Unit] = liftF( PutTimeSeries( series, () ) )
  def putCohort( cohort: TimeSeriesCohort ): TrainingProtocol[Unit] = liftF( PutTimeSeriesCohort( cohort, () ) )
//  def put( topic: Topic, datapoint: DataPoint ): TrainingProtocol[Unit] = liftF( PutDataPoint( topic, datapoint, () ) )
}

object TrainingRepository extends TrainingRepository {
  type TrainingProtocol[Next] = Free[TrainingProtocolF, Next]


  trait TrainingProtocolF[+Next]
  case class PutTimeSeries[+Next]( series: TimeSeries, next: Next ) extends TrainingProtocolF[Next]
  case class PutTimeSeriesCohort[+Next]( cohort: TimeSeriesCohort, next: Next ) extends TrainingProtocolF[Next]

//  case class PutDataPoint[+Next](
//    topic: Topic,
//    datapoint: DataPoint,
//    next: Next
//  ) extends TrainingProtocolF[Next]

  object TrainingProtocolF {
    implicit def functor: Functor[TrainingProtocolF] = new Functor[TrainingProtocolF] {
      override def map[A, B]( fa: TrainingProtocolF[A] )( f: (A) => B ): TrainingProtocolF[B] = {
        fa match {
          case p @ PutTimeSeries(_, next) => p.copy( next = f(next) )
          case p @ PutTimeSeriesCohort(_, next) => p.copy( next = f(next) )
//          case p @ PutDataPoint(_, _, next) => p.copy( next = f(next) )
        }
      }
    }
  }


  trait Interpreter {
    def apply[Next]( action: TrainingProtocol[Next] ): Task[Next]
  }
}
