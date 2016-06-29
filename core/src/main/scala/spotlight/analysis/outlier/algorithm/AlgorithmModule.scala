package spotlight.analysis.outlier.algorithm

import scala.annotation.tailrec
import scala.reflect._
import akka.actor.{ActorPath, ActorRef, Props}
import akka.cluster.sharding.ShardRegion
import akka.event.LoggingReceive

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import shapeless.{Lens, TypeCase, Typeable}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.ml.clustering.DoublePoint
import nl.grons.metrics.scala.{MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityIdentifying}
import peds.commons.{KOp, TryV}
import peds.commons.log.Trace
import demesne._
import demesne.module.entity.messages.EntityProtocol
import peds.commons.identifier.TaggedID
import spotlight.analysis.outlier._
import spotlight.model.outlier.{NoOutliers, OutlierPlan, Outliers}
import spotlight.model.timeseries._


object AlgorithmProtocol extends EntityProtocol[AlgorithmModule.AnalysisState] {
  sealed trait AlgorithmMessage
  abstract class AlgorithmCommand extends Command with AlgorithmMessage
  abstract class AlgorithmEvent extends Event with AlgorithmMessage

//  case class AddAlgorithm( override val targetId: AddAlgorithm#TID ) extends AlgorithmCommand

//  case class AlgorithmAdded( override val sourceId: AlgorithmAdded#TID ) extends AlgorithmEvent

  case class Register( override val targetId: Register#TID, routerRef: ActorRef ) extends AlgorithmCommand

  case class Registered( override val sourceId: Registered#TID ) extends AlgorithmEvent

  import AlgorithmModule.AnalysisState

  case class GetStateSnapshot( override val targetId: GetStateSnapshot#TID ) extends AlgorithmCommand

  case class StateSnapshot( sourceId: StateSnapshot#TID, snapshot: AnalysisState ) extends AlgorithmEvent
  object StateSnapshot {
    def apply( s: AnalysisState ): StateSnapshot = StateSnapshot( sourceId = s.id, snapshot = s )
  }

  private[algorithm] case class Advanced(
    override val sourceId: Advanced#TID,
    point: DataPoint,
    isOutlier: Boolean,
    threshold: ThresholdBoundary
  ) extends Event

  case class AlgorithmUsedBeforeRegistrationError(
    sourceId: AnalysisState#TID,
    algorithm: Symbol,
    path: ActorPath
  ) extends IllegalStateException(
    s"actor [${path}] not registered algorithm [${algorithm.name}] with scope [${sourceId}] before use"
  ) with OutlierAlgorithmError
}


/**
  * Created by rolfsd on 6/5/16.
  */
object AlgorithmModule {
  trait StrictSelf[T <: StrictSelf[T]] { self: T =>
    type Self >: self.type <: T
  }

  trait AnalysisState extends Entity with Equals { self: StrictSelf[_] =>
    override type ID = OutlierPlan.Scope

    override val evID: ClassTag[ID] = classTag[OutlierPlan.Scope]
    override val evTID: ClassTag[TID] = classTag[TaggedID[OutlierPlan.Scope]]

    def scope: OutlierPlan.Scope = id
    def algorithm: Symbol
    def topic: Topic = scope.topic

    def thresholds: Seq[ThresholdBoundary]
    def addThreshold( threshold: ThresholdBoundary ): Self

    def tolerance: Double

    override def hashCode: Int = {
      41 * (
        41 * (
          41 * (
            41 * (
              41 + id.##
              ) + name.##
            ) + topic.##
          ) + algorithm.##
        ) + tolerance.##
    }

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: AnalysisState => {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.id == that.id ) &&
            ( this.name == that.name) &&
            ( this.topic == that.topic) &&
            ( this.algorithm == that.algorithm ) &&
            ( this.tolerance == that.tolerance )
          }
        }

        case _ => false
      }
    }
  }


  implicit val analysisStateIdentifying: EntityIdentifying[AnalysisState] = {
    new EntityIdentifying[AnalysisState] with OutlierPlan.Scope.ScopeIdentifying[AnalysisState]
  }

  /**
    *  approximate number of points in a one day window size @ 1 pt per 10s
    */
  val ApproximateDayWindow: Int = 6 * 60 * 24


  //todo: ClusterProvider???
  trait ModuleConfiguration {
    def maximumNrClusterNodes: Int = 6
  }
}

abstract class AlgorithmModule
extends AggregateRootModule
with InitializeAggregateRootClusterSharding { module: AlgorithmModule.ModuleConfiguration =>
  import AlgorithmModule.AnalysisState

  override val trace = Trace[AlgorithmModule]

  implicit val algorithmIdentifying: EntityIdentifying[State] = {
    new EntityIdentifying[State] with OutlierPlan.Scope.ScopeIdentifying[State]
  }


  override type ID = OutlierPlan.Scope
  val idType = Typeable[OutlierPlan.Scope]
  override def nextId: TryV[TID] = algorithmIdentifying.nextIdAs[TID]
  val advanced = TypeCase[AlgorithmProtocol.Advanced]


  override def rootType: AggregateRootType = {
    new AggregateRootType {
      override val name: String = module.shardName
      override def aggregateRootProps( implicit model: DomainModel ): Props = AlgorithmActor.props( model, this )
      override def maximumNrClusterNodes: Int = module.maximumNrClusterNodes
      override def aggregateIdFor: ShardRegion.ExtractEntityId = super.aggregateIdFor orElse {
        case advanced( a ) => (a.sourceId.toString, a )
      }
    }
  }

  def algorithm: Algorithm
  override val aggregateIdTag: Symbol = algorithm.label

  trait Algorithm {
    def label: Symbol
    def prepareContext( algorithmContext: Context ): Context = identity( algorithmContext )
    def prepareData( algorithmContext: Context ): TryV[Seq[DoublePoint]]
    def step( point: PointT )( implicit state: State, algorithmContext: Context ): (Boolean, ThresholdBoundary)
  }


  type Context <: AlgorithmContext
  def makeContext( message: DetectUsing ): Context

  trait AlgorithmContext extends LazyLogging {
    def message: DetectUsing

    def plan: OutlierPlan = message.payload.plan

    def history: HistoricalStatistics

    def source: TimeSeriesBase = message.source

    def data: Seq[DoublePoint]

    def fillData( minimalSize: Int = HistoricalStatistics.LastN ): (Seq[DoublePoint]) => Seq[DoublePoint] = { original =>
      if ( minimalSize <= original.size ) original
      else {
        val historicalSize = history.lastPoints.size
        val needed = minimalSize + 1 - original.size
        val historical = history.lastPoints.drop( historicalSize - needed )
        historical.toDoublePoints ++ original
      }
    }

    def tailAverage(
      tailLength: Int = AlgorithmContext.DefaultTailAverageLength
    ): (Seq[DoublePoint]) => Seq[DoublePoint] = { points =>
      val values = points map { _.value }
      val lastPos = {
        points.headOption
        .map { h => history.lastPoints indexWhere { _.timestamp == h.timestamp } }
        .getOrElse { history.lastPoints.size }
      }

      val last = history.lastPoints.drop( lastPos - tailLength + 1 ) map { _.value }
      logger.debug( "tail-average: last[{}]", last.mkString(",") )

      points
      .map { _.timestamp }
      .zipWithIndex
      .map { case (ts, i) =>
        val pointsToAverage = {
          if ( i < tailLength ) {
            val all = last ++ values.take( i + 1 )
            all.drop( all.size - tailLength )
          } else {
            values.slice( i - tailLength + 1, i + 1 )
          }
        }

        ( ts, pointsToAverage )
      }
      .map { case (ts, pts) =>
        val average = pts.sum / pts.size
        logger.debug( "points to tail average ({}, [{}]) = {}", ts.toLong.toString, pts.mkString(","), average.toString )
        ( ts, average ).toDoublePoint
      }
    }
  }

  object AlgorithmContext {
    val DefaultTailAverageLength: Int = 3
  }


  type State <: AnalysisState
  implicit def evState: ClassTag[State]

  val analysisStateCompanion: AnalysisStateCompanion
  trait AnalysisStateCompanion {
    def zero( id: State#TID ): State

    type History
    def updateHistory( history: History, event: AlgorithmProtocol.Advanced ): History
    def historyLens: Lens[State, History]
    def thresholdLens: Lens[State, Seq[ThresholdBoundary]]
  }


  object AlgorithmActor {
    def props( model: DomainModel, rootType: AggregateRootType ): Props = {
      Props( new AlgorithmActor( model, rootType ) with StackableStreamPublisher )
    }
  }

  class AlgorithmActor(
    override val model: DomainModel,
    override val rootType: AggregateRootType
  ) extends AggregateRoot[State, OutlierPlan.Scope]
  with InstrumentedActor { publisher: EventPublisher =>
    import demesne.module.entity.{ messages => EntityMessage }

    override def parseId( idstr: String ): ID = algorithmIdentifying.safeParseId( idstr )( algorithmIdentifying.evID )

    override lazy val metricBaseName: MetricName = MetricName( "spotlight.analysis.outlier.algorithm" )
    lazy val algorithmTimer: Timer = metrics timer algorithm.label.name

    override var state: State = _

    import analysisStateCompanion.{ historyLens, thresholdLens, updateHistory }
    val advanceLens: Lens[State, (analysisStateCompanion.History, Seq[ThresholdBoundary])] = historyLens ~ thresholdLens

    override val acceptance: Acceptance = {
      case ( EntityMessage.Added(id, _), s ) => {
        idType.cast( id )
        .map { i => analysisStateCompanion zero i }
        .getOrElse { s }
      }
      case ( AlgorithmProtocol.Registered(_), s ) => s
      case ( event: AlgorithmProtocol.Advanced, s ) => {
        advanceLens.modify( s ){ case (h, ts) => ( updateHistory( h, event ), ts :+ event.threshold ) }
      }
    }

    override def receiveCommand: Receive = LoggingReceive { around( quiescent ) }

    val quiescent: Receive = {
      case EntityMessage.Add(id, _) => persist( EntityMessage.Added(id) ) { event =>
        acceptAndPublish( event )
        context become LoggingReceive { around( ready() orElse stateReceiver ) }
      }
    }

    def ready( driver: Option[ActorRef] = None ): Receive = {
      case AlgorithmProtocol.Register( id, router ) if id == state.id => {
        router !+ DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm.label, self )
        context become LoggingReceive { around( ready( Some(sender()) ) ) }
      }

      case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if driver.isDefined => {
        driver foreach { _ !+ AlgorithmProtocol.Registered(state.id) }
        context become LoggingReceive { around( active orElse stateReceiver ) }
      }

      case e: EntityMessage.Add => /* handle and drop since already added */
    }

    val active: Receive = {
      case msg @ DetectUsing( algo, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
        val toOutliers = kleisli[TryV, (Outliers, Context), Outliers] { case (o, _) => o.right }

        val start = System.currentTimeMillis()
        ( algorithmContext >=> findOutliers >=> toOutliers ).run( msg ) match {
          case \/-( r ) => {
            log.debug( "sending detect result to aggregator[{}]: [{}]", aggregator.path, r )
            algorithmTimer.update( System.currentTimeMillis() - start, scala.concurrent.duration.MILLISECONDS )
            aggregator !+ r
          }

          case -\/( ex ) => {
            log.error(
              ex,
              "failed [{}] analysis on [{}] @ [{}]",
              algo.name,
              payload.plan.name + "][" + payload.topic,
              payload.source.interval
            )
            // don't let aggregator time out just due to error in algorithm
            aggregator !+ NoOutliers(
              algorithms = Set(algorithm.label),
              source = payload.source,
              plan = payload.plan,
              thresholdBoundaries = Map.empty[Symbol, Seq[ThresholdBoundary]]
            )
          }
        }
      }

      case adv: AlgorithmProtocol.Advanced => accept( adv ) //todo: what to do here persist?

      case e: EntityMessage.Add => /* handle and drop since already added */
    }

    val stateReceiver: Receive = {
      case _: AlgorithmProtocol.GetStateSnapshot => sender() ! AlgorithmProtocol.StateSnapshot( state )
    }

    override def unhandled( message: Any ): Unit = {
      message match {
        case m: DetectUsing if m.algorithm == state.algorithm && OutlierPlan.Scope(m.payload.plan, m.payload.topic) == state.id => {
          val ex = AlgorithmProtocol.AlgorithmUsedBeforeRegistrationError( state.id, algorithm.label, self.path )
          log.error( ex, "algorithm actor [{}] not registered for scope:[{}]", algorithm.label, state.id )
        }

        case m: DetectUsing => {
          log.error( "algorithm [{}] does not recognize requested payload: [{}]", algorithm, m )
          m.aggregator !+ UnrecognizedPayload( algorithm.label, m )
        }

        case m => super.unhandled( m )
      }
    }

    // -- algorithm functional elements --
    val algorithmContext: KOp[DetectUsing, Context] = kleisli[TryV, DetectUsing, Context] { m => module.makeContext( m ).right }

    def findOutliers: KOp[Context, (Outliers, Context)] = {
      for {
        ctx <- ask[TryV, Context]
        data <- kleisli[TryV, Context, Seq[DoublePoint]] { c => algorithm.prepareData( c ) }
        events <- collectOutlierPoints( data )
        o <- makeOutliers( events )
      } yield ( o, ctx )
    }

    private def collectOutlierPoints( points: Seq[DoublePoint] ): KOp[Context, Seq[AlgorithmProtocol.Advanced]] = {
      kleisli[TryV, Context, Seq[AlgorithmProtocol.Advanced]] { implicit analysisContext =>
        val currentTimestamps = points.map{ _.timestamp }.toSet

        @inline def isCurrentPoint( pt: PointT ): Boolean = currentTimestamps contains pt.timestamp

        @tailrec def loop(
          pts: List[DoublePoint],
          acc: Seq[AlgorithmProtocol.Advanced]
        )(
          implicit loopState: State
        ): Seq[AlgorithmProtocol.Advanced] = {
          pts match {
            case Nil => acc

            case pt :: tail => {
              val ts = pt.timestamp
              val (isOutlier, threshold) = algorithm step pt
              val (updatedAcc, updatedState) = {
                analysisContext.data
                .find { _.timestamp == pt.timestamp }
                .map { original =>
                  log.debug( "PT:[{}] ORIGINAL:[{}]", (pt._1.toLong, pt._2), (original._1.toLong, original._2) )
                  val event = AlgorithmProtocol.Advanced(
                    sourceId = state.id,
                    point = original.toDataPoint,
                    isOutlier = isOutlier,
                    threshold = threshold
                  )

                  log.debug(
                    "LOOP-{}[{}]: AnalysisState.Advanced:[{}]",
                    if ( isOutlier ) "HIT" else "MISS",
                    (pt._1.toLong, pt._2),
                    event
                  )

                  ( acc :+ event, acceptance(event, loopState) )
                }
                .getOrElse {
                  log.debug( "NOT ORIGINAL PT:[{}]", (pt._1.toLong, pt._2) )
                  ( acc, loopState )
                }
              }

              loop( tail, updatedAcc )( updatedState )
            }
          }
        }

        val events = loop( points.toList, Seq.empty[AlgorithmProtocol.Advanced] )( state )
        persistAll( events.to[collection.immutable.Seq] ){ e =>
          log.debug( "{} persisting Advanced:[{}]", state.id, e )
          accept( e )
        }
        events.right
      }
    }

    private def makeOutliers( events: Seq[AlgorithmProtocol.Advanced] ): KOp[Context, Outliers] = {
      kleisli[TryV, Context, Outliers] { ctx =>
        val outliers = events collect { case e if e.isOutlier => e.point }
        val thresholds = events map { _.threshold }

        Outliers.forSeries(
          algorithms = Set( algorithm.label ),
          plan = ctx.plan,
          source = ctx.source,
          outliers = outliers,
          thresholdBoundaries = Map( algorithm.label -> thresholds )
        )
        .disjunction
        .leftMap { _.head }
      }
    }
  }
}
