package spotlight.analysis.outlier.algorithm

import scala.annotation.tailrec
import scala.reflect.ClassTag
import akka.actor.{ActorPath, ActorRef, Props}
import akka.cluster.sharding.ShardRegion
import akka.event.LoggingReceive

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import shapeless.Lens
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.ml.clustering.DoublePoint
import nl.grons.metrics.scala.{MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.Entity
import peds.commons.{KOp, TryV}
import peds.commons.log.Trace
import demesne._
import peds.commons.identifier.TaggedID
import spotlight.analysis.outlier._
import spotlight.analysis.outlier.algorithm.AlgorithmModule.ModuleProvider
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


  /**
    *  approximate number of points in a one day window size @ 1 pt per 10s
    */
  val ApproximateDayWindow: Int = 6 * 60 * 24


  //todo: ClusterProvider???
  trait ModuleProvider {
    def maximumNrClusterNodes: Int = 6
  }
}

abstract class AlgorithmModule
extends AggregateRootModule[AlgorithmModule.ID]
with InitializeAggregateRootClusterSharding { module: ModuleProvider =>
  override val trace = Trace[AlgorithmModule]

  override def rootType: AggregateRootType = {
    new AggregateRootType {
      override val name: String = module.shardName
      override def aggregateRootProps( implicit model: DomainModel ): Props = AlgorithmActor.props( model, this )
      override def maximumNrClusterNodes: Int = module.maximumNrClusterNodes
      override def aggregateIdFor: ShardRegion.ExtractEntityId = super.aggregateIdFor orElse {
        case a: AnalysisState.Advanced => ( a.sourceId.toString, a )
      }
    }
  }

  def algorithm: Algorithm
  override val aggregateIdTag: Symbol = algorithm.label

  trait Algorithm {
    def label: Symbol
    def prepareData( algorithmContext: Context ): TryV[Seq[DoublePoint]]
    def step( point: PointT )( implicit state: State, algorithmContext: Context ): (Boolean, ThresholdBoundary)
//todo: updateStateAfterStep redundant given State.History updateHistory and lens
//    def updateStateAfterStep( state: State, point: PointT ): State

    def prepareContext( algorithmContext: Context ): Context = identity( algorithmContext )
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

    //    def historyKey: OutlierPlan.Scope = OutlierPlan.Scope( plan, topic )
    //    def messageConfig: Config = message.properties //should configure into specific AlgoConfig via Add
  }

  object AlgorithmContext {
    val DefaultTailAverageLength: Int = 3
  }


  type State <: AnalysisState
  implicit def evState: ClassTag[State]

  trait AnalysisState extends Entity with Equals {
    override type ID = module.ID
    override def evId: ClassTag[ID] = ClassTag( classOf[ID] )
    def scope: OutlierPlan.Scope = id
    def algorithm: Symbol = module.algorithm.label
    def topic: Topic = scope.topic

    def thresholds: Seq[ThresholdBoundary]
    def addThreshold( threshold: ThresholdBoundary ): State

    def tolerance: Double

    override def hashCode: Int = {
      41 * (
        41 * (
          41 * (
            41 + id.##
          ) + name.##
        ) + topic.##
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
            ( this.tolerance == that.tolerance )
          }
        }

        case _ => false
      }
    }
  }

  object AnalysisState {
    object Protocol {
      import AlgorithmModule.Protocol.AlgorithmMessage

      case class GetStateSnapshot( override val targetId: GetStateSnapshot#TID ) extends Command with AlgorithmMessage

      case class StateSnapshot( sourceId: TID, snapshot: State ) extends Event with AlgorithmMessage
      object StateSnapshot {
        def apply( s: State ): StateSnapshot = StateSnapshot( sourceId = s.id, snapshot = s )
      }
    }

    private[algorithm] case class Advanced(
      override val sourceId: Advanced#TID,
      point: DataPoint,
      isOutlier: Boolean,
      threshold: ThresholdBoundary
    ) extends Event
  }

  val analysisStateCompanion: AnalysisStateCompanion
  trait AnalysisStateCompanion {
    def zero( id: State#TID ): State

    type History
    def updateHistory( history: History, event: AnalysisState.Advanced ): History
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
  ) extends AggregateRoot[State]
  with InstrumentedActor { publisher: EventPublisher =>
    import AlgorithmModule.Protocol._

    override lazy val metricBaseName: MetricName = MetricName( "spotlight.analysis.outlier.algorithm" )
    lazy val algorithmTimer: Timer = metrics timer algorithm.label.name

    override var state: State = _

    import analysisStateCompanion.{ historyLens, thresholdLens, updateHistory }
    val advanceLens: Lens[State, (analysisStateCompanion.History, Seq[ThresholdBoundary])] = historyLens ~ thresholdLens

    override val acceptance: Acceptance = {
      case ( Added(id), _ ) => analysisStateCompanion zero id
      case ( Registered(_), s ) => s
      case ( event: AnalysisState.Advanced, s ) => {
        log.info( "ACCEPTANCE: BEFORE state=[{}]", s )
        //        val interimState = s.addThreshold(event.threshold)
        //        log.info( "ACCEPTANCE: AFTER interim state=[{}]", interimState )
        //        val r = historyLens.modify( interimState )( oldHistory => updateHistory( oldHistory, event ) )
        //        log.info( "ACCEPTANCE: AFTER final state=[{}]", interimState )
        //        r
        val result = advanceLens.modify( s ){ case (h, ts) => ( updateHistory( h, event ), ts :+ event.threshold ) }
        log.info( "ACCEPTANCE: AFTER final state=[{}]", result )
        result
      }
    }

    override def receiveCommand: Receive = LoggingReceive { around( quiescent ) }

    val quiescent: Receive = {
      case Add(id) => persist( Added(id) ) { event =>
        acceptAndPublish( event )
        context become LoggingReceive { around( ready() orElse stateReceiver ) }
      }
    }

    def ready( driver: Option[ActorRef] = None ): Receive = {
      case Register( id, router ) if id == state.id => {
        router !+ DetectionAlgorithmRouter.RegisterDetectionAlgorithm( algorithm.label, self )
        context become LoggingReceive { around( ready( Some(sender()) ) ) }
      }

      case DetectionAlgorithmRouter.AlgorithmRegistered( a ) if driver.isDefined => {
        driver foreach { _ !+ Registered(state.id) }
        context become LoggingReceive { around( active orElse stateReceiver ) }
      }

      case e: Add => /* handle and drop since already added */
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

      case adv: AnalysisState.Advanced => accept( adv ) //todo: what to do here persist?

      case e: Add => /* handle and drop since already added */
    }

    import AnalysisState.Protocol.{GetStateSnapshot, StateSnapshot}
    val stateReceiver: Receive = {
      case _: GetStateSnapshot => sender() ! StateSnapshot( state )
    }

    override def unhandled( message: Any ): Unit = {
      message match {
        case m: DetectUsing if m.algorithm == state.algorithm && OutlierPlan.Scope(m.payload.plan, m.payload.topic) == state.id => {
          val ex = AlgorithmUsedBeforeRegistrationError( state.id, algorithm.label, self.path )
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

    private def collectOutlierPoints( points: Seq[DoublePoint] ): KOp[Context, Seq[AnalysisState.Advanced]] = {
      import scala.collection.immutable

      kleisli[TryV, Context, Seq[AnalysisState.Advanced]] { implicit analysisContext =>
        val currentTimestamps = points.map{ _.timestamp }.toSet

        @inline def isCurrentPoint( pt: PointT ): Boolean = currentTimestamps contains pt.timestamp

        @tailrec def loop(
          pts: List[DoublePoint],
          acc: Seq[AnalysisState.Advanced]
        )(
          implicit loopState: State
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

              loop( tail, updatedAcc )( updatedState )
            }
          }
        }

        val events = loop( points.toList, Seq.empty[AnalysisState.Advanced] )( state )
        persistAllAsync[AnalysisState.Advanced]( events ){ e =>
          log.debug( "{} persisting Advanced:[{}]", state.id, e )
          accept( e )
        }
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
