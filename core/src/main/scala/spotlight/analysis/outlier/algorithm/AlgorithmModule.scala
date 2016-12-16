package spotlight.analysis.outlier.algorithm

import java.io.Serializable

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES, NANOSECONDS}
import scala.util.Random
import scala.reflect._
import akka.actor.{ActorPath, Props}
import akka.agent.Agent
import akka.cluster.sharding.ShardRegion
import akka.event.LoggingReceive
import akka.persistence.{DeleteMessagesFailure, DeleteMessagesSuccess, SaveSnapshotFailure, SaveSnapshotSuccess}

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import shapeless.TypeCase
import com.typesafe.config.Config
import com.typesafe.config.ConfigException.BadValue
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.commons.math3.ml.clustering.DoublePoint
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityIdentifying}
import peds.commons.{KOp, TryV}
import peds.commons.identifier.{Identifying, ShortUUID, TaggedID}
import peds.commons.log.Trace
import demesne._
import demesne.repository.{AggregateRootRepository, EnvelopingAggregateRootRepository}
import demesne.repository.AggregateRootRepository.{ClusteredAggregateContext, LocalAggregateContext}
import peds.commons.collection.BloomFilter
import spotlight.analysis.outlier._
import spotlight.model.outlier.{NoOutliers, OutlierPlan, Outliers}
import spotlight.model.timeseries._


object AlgorithmProtocol extends AggregateProtocol[AlgorithmModule.AnalysisState#ID] {
  sealed trait AlgorithmMessage
  abstract class AlgorithmCommand extends Command with AlgorithmMessage
  abstract class AlgorithmEvent extends Event with AlgorithmMessage

  import AlgorithmModule.AnalysisState

  case class GetTopicShapeSnapshot( override val targetId: GetTopicShapeSnapshot#TID, topic: Topic ) extends AlgorithmCommand
  case class TopicShapeSnapshot(
    override val sourceId: TopicShapeSnapshot#TID,
    topic: Topic,
    snapshot: Option[AlgorithmModule#Shape]
  ) extends AlgorithmEvent

  case class GetStateSnapshot( override val targetId: GetStateSnapshot#TID ) extends AlgorithmCommand
  case class StateSnapshots( override val sourceId: StateSnapshots#TID, snapshot: AnalysisState) extends AlgorithmEvent

  case class Advanced(
    override val sourceId: Advanced#TID,
    topic: Topic,
    point: DataPoint,
    isOutlier: Boolean,
    threshold: ThresholdBoundary
  ) extends AlgorithmEvent

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
object AlgorithmModule extends Instrumented with StrictLogging {
  trait StrictSelf[T <: StrictSelf[T]] { self: T =>
    type Self >: self.type <: T
  }

  type ID = ShortUUID
  type TID = TaggedID[ID]


  trait AnalysisState extends Entity with Equals { self: StrictSelf[_] =>
    type Shape <: Serializable
    def evShape: ClassTag[Shape]

    override type ID = AlgorithmModule.ID

    override val evID: ClassTag[ID] = classTag[AlgorithmModule.ID]
    override val evTID: ClassTag[TID] = classTag[TaggedID[AlgorithmModule.ID]]

    def algorithm: Symbol
    def shapes: Map[Topic, Shape] = Map.empty[Topic, Shape]
    def withTopicShape( topic: Topic, shape: Shape ): Self

//    def thresholds: Seq[ThresholdBoundary]
//    def addThreshold( threshold: ThresholdBoundary ): Self

    override def hashCode: Int = {
      41 * (
        41 * (
          41 * (
            41 + id.id.##
          ) + name.##
        ) + algorithm.##
      ) + shapes.##
    }

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: AnalysisState => {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.id.id == that.id.id ) &&
            ( this.name == that.name) &&
            ( this.algorithm.name == that.algorithm.name) &&
            ( this.shapes == that.shapes )
          }
        }

        case _ => false
      }
    }
  }


  trait ShapeCompanion[S] {
    def zero( configuration: Option[Config] ): S
    def advance( original: S, advanced: AlgorithmProtocol.Advanced ): S

    //todo: there's a functional way to achieve this :-)
    def valueFrom[V]( configuration: Option[Config], path: String )( fn: Config => V ): Option[V] = {
      for {
        c <- configuration if c hasPath path
        value <- \/.fromTryCatchNonFatal{ fn(c) }.toOption
      } yield value
    }
  }


  implicit val identifying: EntityIdentifying[AnalysisState] = {
    new EntityIdentifying[AnalysisState] with ShortUUID.ShortUuidIdentifying[AnalysisState] {
      override lazy val idTag: Symbol = 'algorithm
      override val evEntity: ClassTag[AnalysisState] = classTag[AnalysisState]
      override def nextId: TryV[TID] = new IllegalStateException( "analysis state is fixed to plan and not generated" ).left
    }
  }


  /**
    *  approximate number of points in a one day window size @ 1 pt per 10s
    */
  val ApproximateDayWindow: Int = 6 * 60 * 24


  trait ModuleConfiguration {
    //todo: provide a link to global algorithm configuration
    def maximumNrClusterNodes: Int = 6
  }


  case class InsufficientDataSize(
    algorithm: Symbol,
    size: Long,
    required: Long
  ) extends IllegalArgumentException(
    s"${size} data points is insufficient to perform ${algorithm.name} test, which requires at least ${required} points"
  )


  case class RedundantAlgorithmConfiguration[TID](
    aggregateId: TID,
    path: String,
    value: Any
  ) extends RuntimeException( s"For algorithm ${aggregateId}: ${path}:${value} is redundant" )

  case class InvalidAlgorithmConfiguration(
    algorithm: Symbol,
    path: String,
    requirement: String
  ) extends BadValue( path, s"For algorithm, ${algorithm.name}, ${path} must: ${requirement}" )


  val snapshotFactorizer: Random = new Random()
  override lazy val metricBaseName: MetricName = MetricName( classOf[AlgorithmModule] )

  val _uniqueCalculations: Agent[BloomFilter[AlgorithmModule.ID]] = {
    Agent( BloomFilter[AlgorithmModule.ID](maxFalsePosProbability = 0.01, size = 10000000) )( ExecutionContext.global )
  }

  metrics.gauge( "unique-calculations" ){ _uniqueCalculations.map{ _.size }.get() }

  def addUniqueCalculation( id: AlgorithmModule.ID ): Unit = {
    _uniqueCalculations send { cs =>
      if ( cs.has_?(id) ) cs
      else {
        logger.info( "TEST: adding UNIQUE CALCULATION: [{}]", id )
        cs + id
      }
    }
  }

  val snapshotTimer: Timer = metrics.timer( "snapshot" )
  val snapshotSuccesses: Meter = metrics.meter( "snapshot", "successes" )
  val snapshotFailures: Meter = metrics.meter( "snapshot", "failures" )

  val journalSweepTimer: Timer = metrics.timer( "journal-sweep" )
  val journalSweepSuccesses: Meter = metrics.meter( "journal-sweep", "successes" )
  val journalSweepFailures: Meter = metrics.meter( "journal-sweep", "failures" )

  val passivationTimer: Timer = metrics.timer( "passivation" )
}

abstract class AlgorithmModule extends AggregateRootModule { module: AlgorithmModule.ModuleConfiguration =>
  import AlgorithmModule.{ AnalysisState, ShapeCompanion, StrictSelf }
  import AlgorithmProtocol.Advanced

  private val trace = Trace[AlgorithmModule]

  /**
    * Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  type Shape <: Serializable
  def evShape: ClassTag[Shape]

  val shapeCompanion: ShapeCompanion[Shape]

  case class State(
    override val id: AlgorithmModule.TID,
    override val shapes: Map[Topic, module.Shape] = Map.empty[Topic, module.Shape]
  ) extends AnalysisState with StrictSelf[State] {
    override type Self = State
    override type Shape = module.Shape
    override def evShape: ClassTag[Shape] = module.evShape

    override def algorithm: Symbol = module.algorithm.label
    override def name: String = algorithm.name

    override def withTopicShape( topic: Topic, shape: Shape ): Self = copy( shapes = shapes + (topic -> shape) )
  }


  def algorithm: Algorithm
  override lazy val aggregateIdTag: Symbol = algorithm.label
  override lazy val shardName: String = algorithm.label.name


  trait Algorithm {
    val label: Symbol
    def prepareData( c: Context ): Seq[DoublePoint] = c.data
    def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)]
  }


  type Context <: AlgorithmContext
  def makeContext( message: DetectUsing, state: Option[State] ): Context

  trait AlgorithmContext extends LazyLogging {
    def message: DetectUsing
    def topic: Topic = message.topic
    def data: Seq[DoublePoint]
    def tolerance: Double
    def recent: RecentHistory

    def plan: OutlierPlan = message.payload.plan
    def source: TimeSeriesBase = message.source
    def configuration: Config = message.plan.algorithmConfig

    def fillData( minimalSize: Int = RecentHistory.LastN ): (Seq[DoublePoint]) => Seq[DoublePoint] = { original =>
      if ( minimalSize <= original.size ) original
      else {
        val historicalSize = recent.points.size
        val needed = minimalSize + 1 - original.size
        val historical = recent.points.drop( historicalSize - needed )
        historical.toDoublePoints ++ original
      }
    }

    def tailAverage(
      tailLength: Int = AlgorithmContext.DefaultTailAverageLength
    ): (Seq[DoublePoint]) => Seq[DoublePoint] = { points =>
      val values = points map { _.value }
      val lastPos = {
        points.headOption
        .map { h => recent.points indexWhere { _.timestamp == h.timestamp } }
        .getOrElse { recent.points.size }
      }

      val last = recent.points.drop( lastPos - tailLength + 1 ) map { _.value }

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
    val TolerancePath = "tolerance"
    val DefaultTailAverageLength: Int = 3
  }


  class CommonContext( override val message: DetectUsing ) extends AlgorithmContext {
    override def data: Seq[DoublePoint] = message.payload.source.points
    override def recent: RecentHistory = message.recent
    override def tolerance: Double = {
      import AlgorithmContext.TolerancePath
      if ( message.properties hasPath TolerancePath ) message.properties.getDouble( TolerancePath ) else 3.0
    }
  }


  override type ID = AlgorithmModule.ID
  val IdType = TypeCase[TID]
  override def nextId: TryV[TID] = AlgorithmModule.identifying.nextIdAs[TID]
  val AdvancedType = TypeCase[Advanced]


  override lazy val rootType: AggregateRootType = new RootType

//  def toCamelCase( name: String ): String = {
//    val regex = """-(\w)""".r
//    val result = regex.replaceAllIn( name, m => m.subgroups.head.toUpperCase )
//    result.head.toUpper + result.tail
//  }

  class RootType extends AggregateRootType {
    //todo: make configuration driven for algorithms
    override val snapshotPeriod: Option[FiniteDuration] = None // not used - snapshot timing explicitly defined below
    override def snapshot: Option[SnapshotSpecification] = {
      snapshotPeriod map { _ => super.snapshot } getOrElse {
        import scala.concurrent.duration._

        Some(
          new SnapshotSpecification {
            override val snapshotInterval: FiniteDuration = 10.minutes
            override val snapshotInitialDelay: FiniteDuration = {
              val delay = snapshotInterval * AlgorithmModule.snapshotFactorizer.nextDouble()
              FiniteDuration( delay.toMillis, MILLISECONDS )
            }
          }
        )
      }
    }

    override val passivateTimeout: Duration = Duration( 1, MINUTES ) // Duration.Inf //todo: resolve replaying events with data tsunami

    override lazy val name: String = module.shardName
    override lazy val identifying: Identifying[_] = AlgorithmModule.identifying
    override def repositoryProps( implicit model: DomainModel ): Props = Repository localProps model //todo change to clustered with multi-jvm testing of cluster
    override def maximumNrClusterNodes: Int = module.maximumNrClusterNodes
    override def aggregateIdFor: ShardRegion.ExtractEntityId = super.aggregateIdFor orElse {
      case AdvancedType( a ) => ( a.sourceId.id.toString, a )
    }
    override def toString: String = "Algorithm:"+name
  }


  object Repository {
    def localProps( model: DomainModel ): Props = Props( new LocalRepository(model) )
    def clusteredProps( model: DomainModel ): Props = Props( new ClusteredRepository(model) )
  }

  class LocalRepository( model: DomainModel ) extends Repository( model ) with LocalAggregateContext
  class ClusteredRepository( model: DomainModel ) extends Repository( model ) with ClusteredAggregateContext

  class Repository( model: DomainModel )
  extends EnvelopingAggregateRootRepository( model, module.rootType ) { actor: AggregateRootRepository.AggregateContext =>
    override def aggregateProps: Props = AlgorithmActor.props( model, rootType )

    //todo    import demesne.repository.{ StartProtocol => SP }
//    override def doLoad(): Loaded = super.doLoad()
//    override def doInitialize( resources: Map[Symbol, Any] ): Valid[Done] = super.doInitialize( resources )
  }


  object AlgorithmActor {
    def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

    private class Default( model: DomainModel, rootType: AggregateRootType )
    extends AlgorithmActor(model, rootType) with StackableStreamPublisher
  }


  class AlgorithmActor( override val model: DomainModel, override val rootType: AggregateRootType )
  extends AggregateRoot[State, ID]
  with AggregateRoot.Provider
  with InstrumentedActor {
    publisher: EventPublisher =>

    override val journalPluginId: String = "akka.persistence.algorithm.journal.plugin"
    override val snapshotPluginId: String = "akka.persistence.algorithm.snapshot.plugin"

    override def preStart(): Unit = {
      super.preStart()
      AlgorithmModule addUniqueCalculation aggregateId
    }

    override def parseId( idstr: String ): TID = {
      val identifying = AlgorithmModule.identifying
      identifying.safeParseId( idstr )( identifying.evID )
    }

    override lazy val metricBaseName: MetricName = MetricName( "spotlight.analysis.outlier.algorithm" )
    lazy val algorithmTimer: Timer = metrics timer algorithm.label.name

    override var state: State = State( aggregateId )
    override lazy val evState: ClassTag[State] = ClassTag( classOf[State] )

    var algorithmContext: Context = _

    override val acceptance: Acceptance = {
      case ( AdvancedType(event), cs ) => {
        val s = cs.shapes.get( event.topic ) getOrElse shapeCompanion.zero( None )
        cs.withTopicShape( event.topic, shapeCompanion.advance(s, event) )
      }
    }

    override def receiveCommand: Receive = LoggingReceive { around( active orElse stateReceiver orElse nonfunctional ) }

    val active: Receive = {
      case msg @ DetectUsing( algo, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
        val aggregator = sender()

        val start = System.nanoTime()
        @inline def stopTimer( start: Long ): Unit = {
          algorithmTimer.update( System.nanoTime() - start, scala.concurrent.duration.NANOSECONDS )
        }

        ( makeAlgorithmContext >=> findOutliers >=> toOutliers ).run( msg ) match {
          case \/-( r ) => {
            stopTimer( start )
            log.debug( "[{}] sending detect result to aggregator[{}]: [{}]", workId, aggregator.path.name, r )
            aggregator !+ r
          }

          case -\/( ex: AlgorithmModule.InsufficientDataSize ) => {
            stopTimer( start )
            log.error(
              ex,
              "[{}] skipped [{}] analysis on [{}] @ [{}] due to insufficient data - no outliers marked for interval",
              workId,
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

          case -\/( ex ) => {
            stopTimer( start )
            log.error(
              ex,
              "[{}] failed [{}] analysis on [{}] @ [{}] - no outliers marked for interval",
              workId,
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

      case AdvancedType( adv ) => persist( adv ) { accept }
    }

    val toOutliers = kleisli[TryV, (Outliers, Context), Outliers] { case (o, _) => o.right }

    import AlgorithmProtocol.{ GetTopicShapeSnapshot, TopicShapeSnapshot, GetStateSnapshot, StateSnapshots }

    val stateReceiver: Receive = {
      case GetTopicShapeSnapshot( _, topic ) => sender() ! TopicShapeSnapshot( aggregateId, topic, state.shapes.get(topic) )
      case _: GetStateSnapshot => sender() ! StateSnapshots( aggregateId, state )
    }


    var _snapshotStarted: Long = 0L
    def startSnapshotTimer(): Unit = { _snapshotStarted = System.nanoTime() }
    def stopSnapshotTimer( event: Any ): Unit = {
      if ( 0 < _snapshotStarted ) {
        AlgorithmModule.snapshotTimer.update( System.nanoTime() - _snapshotStarted, NANOSECONDS )
        _snapshotStarted = 0L
      }

      event match {
        case _: SaveSnapshotSuccess => AlgorithmModule.snapshotSuccesses.mark()
        case _: SaveSnapshotFailure => AlgorithmModule.snapshotFailures.mark()
        case _ => { }
      }
    }

    var _journalSweepStarted: Long = 0L
    def startSweepTimer(): Unit = { _journalSweepStarted = System.nanoTime() }
    def stopSweepTimer( event: Any ): Unit = {
      if ( 0 < _journalSweepStarted ) {
        AlgorithmModule.journalSweepTimer.update( System.nanoTime() - _journalSweepStarted, NANOSECONDS )
        _journalSweepStarted = 0L
      }

      event match {
        case _: DeleteMessagesSuccess => AlgorithmModule.journalSweepSuccesses.mark()
        case _: DeleteMessagesFailure => AlgorithmModule.journalSweepFailures.mark()
        case _ => { }
      }
    }

    var _passivationStarted: Long = 0L
    def startPassivationTimer(): Unit = { _passivationStarted = System.nanoTime() }
    def stopPassivationTimer( event: Any ): Unit = {
      if ( 0 < _passivationStarted ) {
        AlgorithmModule.passivationTimer.update( System.nanoTime() - _passivationStarted, NANOSECONDS )
        _passivationStarted = 0L
      }

      event match {
        case e: demesne.PassivationSpecification.StopAggregateRoot[_] => {
          log.info( "[{}] received repeated passivation message: [{}]", self.path.name, e )
        }

        case _ => { }
      }
    }


    override def saveSnapshot( snapshot: Any ): Unit = {
      startSnapshotTimer()
      super.saveSnapshot( snapshot )
    }

    val nonfunctional: Receive = {
      case e: demesne.PassivationSpecification.StopAggregateRoot[_] => {
        startPassivationTimer()
        saveSnapshot( state )
        context become LoggingReceive { around( passivating ) }
      }

      case e @ SaveSnapshotSuccess( meta ) => {
        stopSnapshotTimer( e )

        log.debug(
          "[{} - {}]: successful snapshot, deleting journal messages up to sequenceNr: [{}]",
          self.path.name, rootType.snapshotPeriod.map{ _.toCoarsest }, meta.sequenceNr
        )

        startSweepTimer()
        deleteMessages( meta.sequenceNr )
      }

      case e @ SaveSnapshotFailure( meta, ex ) => {
        stopSnapshotTimer( e )
        log.warning( "[{}] failed to save snapshot. meta:[{}] cause:[{}]", meta, ex )
      }

      case e: DeleteMessagesSuccess => {
        stopSweepTimer( e )
        log.info( "[{}] successfully cleared journal: [{}]", self.path.name, e )
      }

      case e: DeleteMessagesFailure => {
        stopSweepTimer( e )
        log.info( "[{}] FAILED to clear journal will attempt to clear on subsequent snapshot: [{}]", self.path.name, e )
      }
    }

    val passivating: Receive = {
      case e: demesne.PassivationSpecification.StopAggregateRoot[_] => {
        stopPassivationTimer( e )
        context stop self
      }

      case e @ SaveSnapshotSuccess( meta ) => {
        stopSnapshotTimer( e )

        log.debug(
          "[{} - {}]: successful passivation snapshot, deleting journal messages up to sequenceNr: [{}]",
          self.path.name, rootType.snapshotPeriod.map{ _.toCoarsest }, meta.sequenceNr
        )

        startSweepTimer()
        deleteMessages( meta.sequenceNr )
      }

      case e @ SaveSnapshotFailure( meta, ex ) => {
        stopSnapshotTimer( e )
        log.warning( "[{}] failed to save passivation snapshot. meta:[{}] cause:[{}]", meta, ex )
      }


      case e: DeleteMessagesSuccess => {
        stopSweepTimer( e )
        log.debug( "[{}] on passivation successfully cleared journal: [{}]", self.path.name, e )
        stopPassivationTimer( e )
        context stop self
      }

      case e: DeleteMessagesFailure => {
        stopSweepTimer( e )
        log.info(
          "[{}] on passivation FAILED to clear journal will attempt to clear on subsequent snapshot: [{}]",
          self.path.name,
          e
        )

        stopPassivationTimer( e )
        context stop self
      }
    }

    override def unhandled( message: Any ): Unit = {
      message match {
//        case m: DetectUsing if Option(state).isDefined && m.algorithm.name == state.algorithm.name && m.scope == state.id => {
        case m: DetectUsing if m.algorithm.name == state.algorithm.name => {
          val ex = AlgorithmProtocol.AlgorithmUsedBeforeRegistrationError( aggregateId, algorithm.label, self.path )
          log.error( ex, "algorithm actor [{}] not registered for scope:[{}]", algorithm.label, aggregateId )
        }

        case m: DetectUsing => {
          log.error( "algorithm [{}] does not recognize requested payload: [{}]", algorithm, m )
          sender() !+ UnrecognizedPayload( algorithm.label, m )
        }

        case m => super.unhandled( m )
      }
    }

    // -- algorithm functional elements --
    val makeAlgorithmContext: KOp[DetectUsing, Context] = kleisli[TryV, DetectUsing, Context] { message =>
      //todo: with algorithm global config support. merge with state config (probably persist and accept ConfigChanged evt)
      \/ fromTryCatchNonFatal {
        algorithmContext = module.makeContext( message, Option( state ) )
        algorithmContext
      }
    }

    def findOutliers: KOp[Context, (Outliers, Context)] = {
      for {
        ctx <- ask[TryV, Context]
        data <- kleisli[TryV, Context, Seq[DoublePoint]] { c => \/ fromTryCatchNonFatal { algorithm prepareData c } }
        events <- collectOutlierPoints( data )
        o <- makeOutliers( events )
      } yield ( o, ctx )
    }

    case class Accumulation( state: State, topic: Topic, shape: Shape, advances: Seq[Advanced] = Seq.empty[Advanced] )

    private def collectOutlierPoints( points: Seq[DoublePoint] ): KOp[Context, Seq[Advanced]] = {
      kleisli[TryV, Context, Seq[Advanced]] { implicit analysisContext =>
        def tryStep( pt: PointT, shape: Shape )( implicit s: State ): TryV[(Boolean, ThresholdBoundary)] = {
          \/ fromTryCatchNonFatal {
            logger.debug( "algorithm {}.step( {} ): before-shape=[{}]", algorithm.label.name, pt, shape.toString )

            algorithm
            .step( pt, shape )
            .getOrElse {
              logger.debug(
                "skipping point[{}] per insufficient history for algorithm {}",
                s"(${pt.dateTime}:${pt.timestamp.toLong}, ${pt.value})", algorithm.label
              )

              ( false, ThresholdBoundary empty pt.timestamp.toLong )
            }
          }
        }


        @tailrec def loop( points: List[DoublePoint], accumulation: TryV[Accumulation] ): TryV[Seq[Advanced]] = {
          points match {
            case Nil => accumulation map { _.advances }

            case pt :: tail => {
              val newAcc = {
                for {
                  acc <- accumulation
                  ot <- tryStep( pt, acc.shape )( acc.state )
                  (isOutlier, threshold) = ot
                } yield {
                  analysisContext.data
                  .find { _.timestamp == pt.timestamp }
                  .map { original =>
                    val event = Advanced(
                      sourceId = aggregateId,
                      topic = acc.topic,
                      point = original.toDataPoint,
                      isOutlier = isOutlier,
                      threshold = threshold
                    )

                    log.debug(
                      "{} STEP:{} [{}]: AnalysisState.Advanced:[{}]",
                      algorithm.label.name,
                      if ( isOutlier ) "ANOMALY" else "regular",
                      s"ts:[${pt.dateTime}:${pt._1.toLong}] eff-value:[${pt.value}] orig-value:[${original.value}]",
                      event
                    )

                    acc.copy(
                      state = acceptance(event, acc.state),
                      shape = shapeCompanion.advance(acc.shape, event),
                      advances = acc.advances :+ event
                    )
                  }
                  .getOrElse {
                    log.error( "NOT ORIGINAL PT:[{}]", (pt._1.toLong, pt._2) )
                    acc
                  }
                }
              }

              loop( tail, newAcc )
            }
          }
        }

        val events = loop(
          points = points.toList,
          accumulation = Accumulation(
            state = state,
            topic = analysisContext.topic,
            shape = state.shapes.get(analysisContext.topic) getOrElse shapeCompanion.zero( Option(analysisContext.configuration) )
          ).right
        )

        events foreach { persistAdvancements }
        events
      }
    }

    private def persistAdvancements( events: Seq[Advanced] ): Unit = {
      events foreach { evt =>
        persist( evt ){ e =>
          val newState = accept( e )
          log.debug( "advanced [{}]: to state=[{}]", aggregateId, newState )
        }
      }
    }

    private def makeOutliers( events: Seq[Advanced] ): KOp[Context, Outliers] = {
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
