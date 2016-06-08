package spotlight.analysis.outlier.algorithm

import scala.annotation.tailrec
import scala.reflect.ClassTag
import akka.actor.{ActorPath, ActorRef, Props}
import akka.event.LoggingReceive

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import shapeless.Lens
import org.apache.commons.math3.ml.clustering.DoublePoint
import nl.grons.metrics.scala.{MetricName, Timer}
import peds.akka.metrics.InstrumentedActor
import peds.akka.publish.{EventPublisher, SilentPublisher}
import peds.archetype.domain.model.core.Entity
import peds.commons.{KOp, TryV}
import peds.commons.log.Trace
import demesne._
import peds.commons.identifier.TaggedID
import spotlight.analysis.outlier.algorithm.AlgorithmModule.ModuleProvider
import spotlight.analysis.outlier.{DetectOutliersInSeries, DetectUsing, DetectionAlgorithmRouter, HistoricalStatistics, UnrecognizedPayload}
import spotlight.model.outlier.{NoOutliers, OutlierPlan, Outliers}
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 6/5/16.
  */
object AlgorithmModule {
  type ID = OutlierPlan.Scope
  type TID = TaggedID[ID]

  type Command = AggregateRootModule.Command[ID]
  type Event = AggregateRootModule.Event[ID]

  object Protocol {
    sealed trait AlgorithmMessage
    case class Add( override val targetId: Add#TID ) extends Command with AlgorithmMessage

    case class Added( override val sourceId: Added#TID ) extends Event with AlgorithmMessage

    case class Register(
      override val targetId: Register#TID,
      routerRef: ActorRef
    ) extends Command with AlgorithmMessage

    case class Registered( override val sourceId: Registered#TID ) extends Event with AlgorithmMessage


    case class AlgorithmUsedBeforeRegistrationError(
      sourceId: TID,
      algorithm: Symbol,
      path: ActorPath
    ) extends IllegalStateException(
      s"actor [${path}] not registered algorithm [${algorithm.name}] with scope [${sourceId}] before use"
    ) with OutlierAlgorithmError
  }


  //todo: ClusterProvider???
  trait ModuleProvider {
    def maximumNrClusterNodes: Int
  }
}

trait AlgorithmModule
extends AggregateRootModule[AlgorithmModule.ID]
with InitializeAggregateRootClusterSharding { module: ModuleProvider =>
  override val trace = Trace[AlgorithmModule]

  override def rootType: AggregateRootType = {
    new AggregateRootType {
      override val name: String = module.shardName
      override def aggregateRootProps( implicit model: DomainModel ): Props = AlgorithmActor.props( model, this )
      override def maximumNrClusterNodes: Int = module.maximumNrClusterNodes
    }
  }

  def algorithm: Algorithm
  override val aggregateIdTag: Symbol = algorithm.label

  trait Algorithm {
    val label: Symbol = algorithm.label
    def prepareContext( algorithmContext: Context ): Context = identity( algorithmContext )
    def prepareData( algorithmContext: Context ): TryV[Seq[DoublePoint]]
    def step( point: PointT )( implicit algorithmContext: Context ): (Boolean, ThresholdBoundary)
    def updateStateAfterStep( state: State, point: PointT ): State
  }


  type Context <: AlgorithmContext
  def makeContext( message: DetectUsing ): Context

  trait AlgorithmContext {
    def message: DetectUsing
    def data: Seq[DoublePoint]
    def plan: OutlierPlan = message.payload.plan
    //    def historyKey: OutlierPlan.Scope = OutlierPlan.Scope( plan, topic )
    def source: TimeSeriesBase = message.source
//    def messageConfig: Config = message.properties //should configure into specific AlgoConfig via Add
  }


  type State <: AnalysisState
  implicit def evState: ClassTag[State]

  trait AnalysisState extends Entity {
    override type ID = module.ID
    override def evId: ClassTag[ID] = ClassTag( classOf[ID] )
    def scope: OutlierPlan.Scope // this could (and should) be ID but need to change demesne to allow other ID types for Entity
    def algorithm: Symbol = Symbol(name)
    def topic: Topic = scope.topic

    def history: HistoricalStatistics

    def thresholds: Seq[ThresholdBoundary]
    def addThreshold( threshold: ThresholdBoundary ): State

    def tolerance: Double
  }

  object AnalysisState {
    case class Advanced(
      override val sourceId: Advanced#TID,
      point: DataPoint,
      isOutlier: Boolean,
      threshold: ThresholdBoundary
    ) extends Event
  }

  val analysisStateCompanion: AnalysisStateCompanion
  trait AnalysisStateCompanion {
    def zero( id: State#TID ): State

    type StateHistory
    def updateHistory( history: StateHistory, event: AnalysisState.Advanced ): StateHistory
    def historyLens: Lens[State, StateHistory]
  }


  object AlgorithmActor {
    def props( model: DomainModel, rootType: AggregateRootType ): Props = {
      Props( new AlgorithmActor( model, rootType ) with SilentPublisher )
    }
  }

  class AlgorithmActor(
    override val model: DomainModel,
    override val rootType: AggregateRootType
  ) extends AggregateRoot[State]
  with InstrumentedActor { publisher: EventPublisher =>
    import AlgorithmModule.Protocol._

    override lazy val metricBaseName: MetricName = MetricName( "spotlight.analysis.outlier.algorithm" )
    lazy val algorithmTimer: Timer = metrics timer algorithm.label.name

    override var state: State = _

    override val acceptance: Acceptance = {
      case ( Added(id), _ ) => analysisStateCompanion zero id
      case ( Registered(_), s ) => s
      case ( event: AnalysisState.Advanced, s ) => {
        import analysisStateCompanion.{ historyLens, updateHistory }
        historyLens.modify( s )( oldHistory => updateHistory( oldHistory, event ) )
      }
    }

    override def receiveCommand: Receive = LoggingReceive { around( quiescent ) }

    val quiescent: Receive = {
      case Add(id) => persist( Added(id) ) { event =>
        accept( event )
        context become LoggingReceive { around( ready() ) }
      }
    }

    def ready( driver: Option[ActorRef] = None ): Receive = {
      case Register( id, router ) if id == state.id => {
        router ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm.label, self )
        context become LoggingReceive { around( ready( Some(sender()) ) ) }
      }

      case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if driver.isDefined => {
        driver foreach { _ ! Registered(state.id) }
        context become LoggingReceive { around( active ) }
      }

      case e: Add => /* handle and drop since already added */
    }

    def active: Receive = {
      case msg @ DetectUsing( algo, aggregator, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
        val toOutliers = kleisli[TryV, (Outliers, Context), Outliers] { case (o, _) => o.right }

        val start = System.currentTimeMillis()
        ( algorithmContext >=> findOutliers >=> toOutliers ).run( msg ) match {
          case \/-( r ) => {
            log.debug( "sending detect result to aggregator[{}]: [{}]", aggregator.path, r )
            algorithmTimer.update( System.currentTimeMillis() - start, scala.concurrent.duration.MILLISECONDS )
            aggregator ! r
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
            aggregator ! NoOutliers(
              algorithms = Set(algorithm.label),
              source = payload.source,
              plan = payload.plan,
              thresholdBoundaries = Map.empty[Symbol, Seq[ThresholdBoundary]]
            )
          }
        }
      }

      case e: Add => /* handle and drop since already added */
    }

    override def unhandled( message: Any ): Unit = {
      message match {
        case m: DetectUsing if m.algorithm == state.algorithm && OutlierPlan.Scope(m.payload.plan, m.payload.topic) == state.id => {
          val ex = AlgorithmUsedBeforeRegistrationError( state.id, algorithm.label, self.path )
          log.error( ex, "algorithm actor [{}] not registered for scope:[{}]", algorithm.label, state.id )
        }

        case m: DetectUsing => {
          log.error( "algorithm [{}] does not recognize requested payload: [{}]", algorithm, m )
          m.aggregator ! UnrecognizedPayload( algorithm.label, m )
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

    private def collectOutlierPoints( points: Seq[DoublePoint] ): KOp[Context, Seq[AnalysisState.Advanced]] = {
      import scala.collection.immutable

      kleisli[TryV, Context, Seq[AnalysisState.Advanced]] { implicit analysisContext =>
        val currentTimestamps = points.map{ _.timestamp }.toSet

        @inline def isCurrentPoint( pt: PointT ): Boolean = currentTimestamps contains pt.timestamp

        @tailrec def loop(
          pts: List[DoublePoint],
          loopState: State,
          acc: Seq[AnalysisState.Advanced]
        ): immutable.Seq[AnalysisState.Advanced] = {
          pts match {
            case Nil => acc.to[immutable.Seq]

            case pt :: tail => {
              val ts = pt.timestamp
              val (isOutlier, threshold) = algorithm step pt
              val (updatedAcc, updatedState) = {
                analysisContext.data
                .find { _.timestamp == pt.timestamp }
                .map { original =>
                  log.debug( "PT:[{}] ORIGINAL:[{}]", pt, original )
                  val event = AnalysisState.Advanced(
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
                  log.debug( "NOT ORIGINAL PT:[{}]", pt )
                  ( acc, loopState )
                }
              }

              loop( tail, updatedState, updatedAcc )
            }
          }
        }

        val events = loop( points.toList, state, Seq.empty[AnalysisState.Advanced] )
        persistAllAsync[AnalysisState.Advanced]( events ){ accept }
        events.right
      }
    }

    private def makeOutliers( events: Seq[AnalysisState.Advanced] ): KOp[Context, Outliers] = {
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
