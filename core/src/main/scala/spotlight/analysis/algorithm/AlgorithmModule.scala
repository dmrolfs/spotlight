package spotlight.analysis.algorithm

import java.io.Serializable

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random
import scala.reflect._
import akka.actor._
import akka.agent.Agent
import akka.cluster.sharding.ShardRegion
import akka.event.LoggingReceive
import akka.persistence._
import bloomfilter.mutable.BloomFilter
import bloomfilter.CanGenerateHashFrom
import bloomfilter.CanGenerateHashFrom.CanGenerateHashFromString

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import com.typesafe.config.Config
import com.typesafe.config.ConfigException.BadValue
import org.apache.commons.math3.ml.clustering.DoublePoint
import com.codahale.metrics.{Metric, MetricFilter}
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import squants.information.{Bytes, Information}
import com.persist.logging.{ActorLogging => PersistActorLogging, _}
import peds.akka.envelope._
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.akka.persistence.{Passivating, SnapshotLimiter}
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityIdentifying}
import peds.commons.{KOp, TryV, Valid}
import peds.commons.identifier.{Identifying, ShortUUID, TaggedID}
import demesne._
import demesne.repository.{AggregateRootRepository, EnvelopingAggregateRootRepository}
import demesne.repository.AggregateRootRepository.{ClusteredAggregateContext, LocalAggregateContext}
import shapeless.the
import spotlight.analysis._
import spotlight.analysis.algorithm.AlgorithmModule.ID
import spotlight.analysis.algorithm.AlgorithmProtocol.RouteMessage
import spotlight.model.outlier.{AnalysisPlan, NoOutliers, Outliers}
import spotlight.model.timeseries._


object AlgorithmProtocol extends AggregateProtocol[AlgorithmModule.ID] {
  case class RouteMessage( override val targetId: RouteMessage#TID, payload: Any ) extends MessageLike {
    override type ID = AlgorithmModule.ID
  }

  case class EstimateSize( override val targetId: EstimateSize#TID ) extends Message with NotInfluenceReceiveTimeout
  case class EstimatedSize( val sourceId: AlgorithmModule.TID, nrShapes: Int, size: Information ) extends ProtocolMessage {
    def averageSizePerShape: Information = size / nrShapes
  }

  object EstimatedSize {
    implicit val ordering = new scala.math.Ordering[EstimatedSize] {
      override def compare( lhs: EstimatedSize, rhs: EstimatedSize ): Int = {
        val sizeComparison = lhs.size compare rhs.size
        if ( sizeComparison != 0 ) sizeComparison
        else lhs.nrShapes compare rhs.nrShapes
      }
    }
  }

  case class GetTopicShapeSnapshot(
    override val targetId: GetTopicShapeSnapshot#TID,
    topic: Topic
  ) extends Command with NotInfluenceReceiveTimeout

  case class TopicShapeSnapshot(
    override val sourceId: TopicShapeSnapshot#TID,
    algorithm: String,
    topic: Topic,
    snapshot: Option[AlgorithmModule#Shape]
  ) extends Event

  case class GetStateSnapshot( override val targetId: AlgorithmModule.TID ) extends Command with NotInfluenceReceiveTimeout

  import scala.language.existentials
  case class StateSnapshot(
    override val sourceId: StateSnapshot#TID,
    snapshot: AlgorithmState[_]
  ) extends Event with NotInfluenceReceiveTimeout

  case class Advanced(
    override val sourceId: Advanced#TID,
    topic: Topic,
    point: DataPoint,
    isOutlier: Boolean,
    threshold: ThresholdBoundary
  ) extends Event

  case class AlgorithmUsedBeforeRegistrationError(
    sourceId: TID,
    algorithm: String,
    path: ActorPath
  ) extends IllegalStateException(
    s"actor [${path}] not registered algorithm [${algorithm}] with scope [${sourceId}] before use"
  ) with OutlierAlgorithmError
}

case class AlgorithmState[S: ClassTag](
  override val id: AlgorithmModule.TID,
  override val name: String,
  shapes: Map[Topic, S] = Map.empty[Topic, S]
) extends Entity with Equals {
  override type ID = AlgorithmModule.ID
  override val evID: ClassTag[ID] = classTag[ID]
  override val evTID: ClassTag[TID] = classTag[TID]

  type Shape = S
  val evShape: ClassTag[Shape] = the[ClassTag[S]]

//  override def algorithm: Symbol = module.algorithm.label

  def withTopicShape( topic: Topic, shape: Shape ): AlgorithmState[S] = copy( shapes = shapes + (topic -> shape) )

  override def hashCode: Int = {
    41 * (
      41 * (
        41 + id.id.##
      ) + name.##
    ) + shapes.##
  }


  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[AlgorithmState[S]]

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: AlgorithmState[S] => {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
          ( that canEqual this ) &&
          ( this.id.id == that.id.id ) &&
          ( this.name == that.name) &&
          ( this.shapes == that.shapes )
        }
      }

      case _ => false
    }
  }

  override def toString: String = s"""State( algorithm:${name} id:[${id}] shapesNr:[${shapes.size}] )"""
}



/**
  * Created by rolfsd on 6/5/16.
  */
object AlgorithmModule extends Instrumented with ClassLogging {
  // useful for with* methods to maintain concrete typing
  // idea pick up from: http://stackoverflow.com/questions/14729996/scala-implementing-method-with-return-type-of-concrete-instance
  trait StrictSelf[T <: StrictSelf[T]] { self: T =>
    type Self >: self.type <: T
  }

  type ID = AlgorithmIdentifier
  type TID = TaggedID[ID]


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


  /**
    *  approximate number of points in a one day window size @ 1 pt per 10s
    */
  val ApproximateDayWindow: Int = 6 * 60 * 24


  trait ModuleConfiguration {
    //todo: provide a link to global algorithm configuration
    def maximumNrClusterNodes: Int = 6
  }


  case class InsufficientDataSize(
    algorithm: String,
    size: Long,
    required: Long
  ) extends IllegalArgumentException(
    s"${size} data points is insufficient to perform ${algorithm} test, which requires at least ${required} points"
  )


  case class RedundantAlgorithmConfiguration[TID](
    aggregateId: TID,
    path: String,
    value: Any
  ) extends RuntimeException( s"For algorithm ${aggregateId}: ${path}:${value} is redundant" )

  case class InvalidAlgorithmConfiguration(
    algorithm: String,
    path: String,
    requirement: String
  ) extends BadValue( path, s"For algorithm, ${algorithm}, ${path} must: ${requirement}" )


  private[algorithm] val snapshotFactorizer: Random = new Random()

  override lazy val metricBaseName: MetricName = MetricName( classOf[AlgorithmModule] )

  val snapshotTimer: Timer = metrics.timer( "snapshot" )
  val snapshotSuccesses: Meter = metrics.meter( "snapshot", "successes" )
  val snapshotFailures: Meter = metrics.meter( "snapshot", "failures" )

  val snapshotSweepTimer: Timer = metrics.timer( "snapshot.sweep" )
  val snapshotSweepSuccesses: Meter = metrics.meter( "snapshot.sweep", "successes" )
  val snapshotSweepFailures: Meter = metrics.meter( "snapshot.sweep", "failures" )

  val journalSweepTimer: Timer = metrics.timer( "journal.sweep" )
  val journalSweepSuccesses: Meter = metrics.meter( "journal.sweep", "successes" )
  val journalSweepFailures: Meter = metrics.meter( "journal.sweep", "failures" )

  val passivationTimer: Timer = metrics.timer( "passivation" )
}

abstract class AlgorithmModule extends AggregateRootModule with Instrumented with ClassLogging {
  module: AlgorithmModule.ModuleConfiguration =>

  import AlgorithmModule.{ ShapeCompanion, StrictSelf }
  import AlgorithmProtocol.Advanced

  implicit val identifying: EntityIdentifying[AlgorithmState[Shape]] = new EntityIdentifying[AlgorithmState[Shape]] {
    override lazy val idTag: Symbol = Symbol( algorithm.label )
    override val evID: ClassTag[ID] = classTag[ID]
    override val evTID: ClassTag[TID] =  classTag[TID]
    override val evEntity: ClassTag[AlgorithmState[Shape]] = classTag[AlgorithmState[Shape]]

    override def nextId: TryV[TID] = -\/( new IllegalStateException("AlgorithmModule TIDs are created via AlgorithmRoutes") )
    override def fromString( idstr: String ): ID = {
      AlgorithmIdentifier.fromAggregateId( idstr ).disjunction match {
        case \/-( id ) => id
        case -\/( exs ) => {
          exs foreach { ex => log.error( Map("@msg" -> "failed to parse algorithm id from string", "rep" -> idstr), ex ) }
          throw exs.head
        }
      }
    }
  }


  object ShardMonitor {
    implicit val canGenerateShortUUIDHash = new CanGenerateHashFrom[ShortUUID] {
      override def generateHash( from: ShortUUID ): Long = CanGenerateHashFromString generateHash from.toString
    }

    val ShardsMetricName = "shards"
    // ( nrShards, shardFilter )
    val shards: Agent[(Long, BloomFilter[AlgorithmModule.ID])] = {
      Agent(
        (
          0L,
          BloomFilter[AlgorithmModule.ID]( numberOfItems = 10000, falsePositiveRate = 0.1 ) //todo make config driven
        )
      )(
        ExecutionContext.global
      )
    }

    def :+( id: AlgorithmModule.ID ): Unit = {
      shards send { css =>
        val ( c, ss ) = css
        if ( ss mightContain id ) (c, ss)
        else {
          logger.info( "created new {} algorithm shard: {}", rootType.name, id )
          ss add id
          ( c + 1L, ss )
        }
      }
    }
  }


  initializeMetrics()  // defer to concrete modules to initiate?

  override lazy val metricBaseName: MetricName = {
    MetricName( spotlight.BaseMetricName, spotlight.analysis.BaseMetricName, "algorithm", rootType.name )
  }

  def initializeMetrics(): Unit = {
    stripLingeringMetrics()
    metrics.gauge( ShardMonitor.ShardsMetricName ) { ShardMonitor.shards.get()._1 }
  }

  def stripLingeringMetrics(): Unit = {
    metrics.registry.removeMatching(
      new MetricFilter {
        override def matches( name: String, metric: Metric ): Boolean = {
          name.contains( classOf[PlanCatalog].getName ) && name.contains( ShardMonitor.ShardsMetricName )
        }
      }
    )
  }


  /**
    * Used to answer EstimateSize requests. If the algorithm module overrides estimatedAverageShapeSize to provide a reasonable
    * shape size factor, this function will quickly multiply the factor by the number of shapes managed by the actor.
    * Otherwise, this function relies on serializing current state, which is becomes very expensive and possibly untenable as
    * topics increase.
    *
    * It is recommended that algorithms override the estimatedAverageShapeSize to support the more efficient calculations, and
    * the EstimatedSize message reports on the average shape size to help determine this factor.
    * @return Information byte size
    */
  def estimateSize( state: State )( implicit system: ActorSystem ): Information = {
    estimatedAverageShapeSize
    .map { _ * state.shapes.size }
    .getOrElse {
      import akka.serialization.{ SerializationExtension, Serializer }
      val serializer: Serializer = SerializationExtension( system ) findSerializerFor state
      val bytes = akka.util.ByteString( serializer toBinary state )
      Bytes( bytes.size )
    }
  }

  /**
    * Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    * @return blended average size for the algorithm shape
    */
  def estimatedAverageShapeSize: Option[Information] = None

  /**
    * Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  type Shape <: Serializable
  implicit def evShape: ClassTag[Shape]

  val shapeCompanion: ShapeCompanion[Shape]

  type State = AlgorithmState[Shape]


  def algorithm: Algorithm
  override lazy val aggregateIdTag: Symbol = Symbol( algorithm.label )
  override lazy val shardName: String = algorithm.label


  trait Algorithm {
    val label: String
    def prepareData( c: Context ): Seq[DoublePoint] = c.data
    def step( point: PointT, shape: Shape )( implicit s: State, c: Context ): Option[(Boolean, ThresholdBoundary)]
  }


  type Context <: AlgorithmContext
  def makeContext( message: DetectUsing, state: Option[State] ): Context

  trait AlgorithmContext {
    def message: DetectUsing
    def topic: Topic = message.topic
    def data: Seq[DoublePoint]
    def tolerance: Double
    def recent: RecentHistory

    def plan: AnalysisPlan = message.payload.plan
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
//        logger.debug( "points to tail average ({}, [{}]) = {}", ts.toLong.toString, pts.mkString(","), average.toString )
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
  val IdType: ClassTag[TID] = classTag[TID]
  override def nextId: TryV[TID] = module.identifying.nextIdAs[TID]
  val AdvancedType: ClassTag[Advanced] = classTag[Advanced]


  override lazy val rootType: AggregateRootType = new RootType

//  def toCamelCase( name: String ): String = {
//    val regex = """-(\w)""".r
//    val result = regex.replaceAllIn( name, m => m.subgroups.head.toUpperCase )
//    result.head.toUpper + result.tail
//  }

  class RootType extends AggregateRootType {
    //todo: make configuration driven for algorithms
    override val snapshotPeriod: Option[FiniteDuration] = None // not used - snapshot timing explicitly defined below
//    override def snapshot: Option[SnapshotSpecification] = None
    override def snapshot: Option[SnapshotSpecification] = {
      snapshotPeriod map { _ => super.snapshot } getOrElse {
        import scala.concurrent.duration._

        Some(
          new SnapshotSpecification {
            override val snapshotInterval: FiniteDuration = 2.minutes // 5.minutes okay //todo make config driven
            override val snapshotInitialDelay: FiniteDuration = {
              val delay = snapshotInterval + snapshotInterval * AlgorithmModule.snapshotFactorizer.nextDouble()
              FiniteDuration( delay.toMillis, MILLISECONDS )
            }
          }
        )
      }
    }

    override val passivateTimeout: Duration = Duration( 1, MINUTES ) //todo make config driven

    override lazy val name: String = module.shardName
    override lazy val identifying: Identifying[_] = {
      val mid = module.identifying
      logger.warn( "ALGO_ROOT_TYPE.identifying = [{}]", mid )
      mid
    }
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

  import akka.pattern.{ Backoff, BackoffOptions, BackoffSupervisor }

  class LocalRepository( model: DomainModel ) extends Repository( model ) with LocalAggregateContext {
    def backoffOptionsFor( id: String ): BackoffOptions = {
      Backoff.onStop(
        childProps = aggregateProps,
        childName = id,
        minBackoff = 50.milliseconds,
        maxBackoff = 5.minutes,
        randomFactor = 0.2
      )
    }

    override def aggregateFor( command: Any ): ActorRef = {
      if ( !rootType.aggregateIdFor.isDefinedAt(command) ) {
        log.warning( "AggregateRootType[{}] does not recognize command[{}]", rootType.name, command )
      }
      val (id, _) = rootType aggregateIdFor command
      AlgorithmIdentifier.fromAggregateId( id ).disjunction match {
        case \/-( aid ) => {
          val options = backoffOptionsFor( id )
          val name = "backoff:"+aid.span
          context.child( name ) getOrElse { context.actorOf( BackoffSupervisor.props(options), name ) }
        }

        case -\/( exs ) => {
          exs foreach { ex => log.error( ex, "failed to parse aggregateId:[{}] from rootType:[{}]", id, rootType.name ) }
          throw exs.head
        }
      }
    }
  }

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
  with SnapshotLimiter
  with Passivating
  with EnvelopingActor
  with AggregateRoot.Provider
  with InstrumentedActor {
    publisher: EventPublisher =>

    setInactivityTimeout( inactivity = rootType.passivation.inactivityTimeout, message = ReceiveTimeout /*StopAggregateRoot(aggregateId)*/ )

    override val journalPluginId: String = "akka.persistence.algorithm.journal.plugin"
    override val snapshotPluginId: String = "akka.persistence.algorithm.snapshot.plugin"

    log.info(
      "{} AlgorithmActor instantiated: [{}] with inactivity timeout:[{}]",
      algorithm.label, aggregateId, rootType.passivation.inactivityTimeout
    )

    override def preStart(): Unit = {
      super.preStart()
      module.ShardMonitor :+ aggregateId
    }

    override def postStop(): Unit = {
      clearInactivityTimeout()
      log.info( "AlgorithmModule[{}]: actor stopped with {} shapes", self.path.name, state.shapes.size )
      super.postStop()
    }

    override def passivate(): Unit = {
      startPassivationTimer()
      if ( isRedundantSnapshot ) {
        import PassivationSaga.Complete
        context become LoggingReceive { around( active orElse passivating( PassivationSaga(snapshot = Complete)) ) }
        log.info( "AlgorithmModule[{}] passivation started with current snapshot...", self.path.name )
        postSnapshot( lastSequenceNr - 1L, true )
      } else {
        context become LoggingReceive { around( active orElse passivating() ) }
        log.info( "AlgorithmModule[{}] passivation started with old snapshot...", self.path.name )
        startPassivationTimer()
        saveSnapshot( state )
      }
    }

    override def parseId( idstr: String ): TID = {
      val identifying = module.identifying
      identifying.safeParseId( idstr )( identifying.evID )
    }

    override lazy val metricBaseName: MetricName = module.metricBaseName
    lazy val executionTimer: Timer = metrics.timer( "execution" )

    override var state: State = AlgorithmState[Shape]( aggregateId, algorithm.label )
    override lazy val evState: ClassTag[State] = ClassTag( classOf[State] )

    var algorithmContext: Context = _

    //todo: experimental feature. disabled via MaxValue for now - initial tests helped to limit to 10000 topics for inmem
    var _advances: Int = 0
    val AdvanceLimit: Int = Int.MaxValue // 10000

    override val acceptance: Acceptance = {
      case ( AdvancedType(event), cs ) => {
        val s = cs.shapes.get( event.topic ) getOrElse shapeCompanion.zero( None )
        _advances += 1
        val newState = cs.withTopicShape( event.topic, shapeCompanion.advance(s, event) )

        if ( AdvanceLimit < _advances ) {
          log.debug( "advance limit exceed before snapshot time - taking snapshot with sweeps.." )
          _advances = 0
          saveSnapshot( newState )
        }

        newState
      }
    }

    override def receiveCommand: Receive = LoggingReceive { around( active orElse stateReceiver orElse nonfunctional ) }

    val active: Receive = {
      case msg @ DetectUsing( _, algo, payload: DetectOutliersInSeries, _, _ ) => {
        val aggregator = sender()

        val start = System.currentTimeMillis()
        @inline def stopTimer( start: Long ): Unit = {
          executionTimer.update( System.currentTimeMillis() - start, scala.concurrent.duration.MILLISECONDS )
        }

        ( makeAlgorithmContext >=> findOutliers >=> toOutliers ).run( msg ) match {
          case \/-( r ) => {
            stopTimer( start )
//            log.debug( "[{}] sending detect result to aggregator[{}]: [{}]", workId, aggregator.path.name, r )
            aggregator !+ r
          }

          case -\/( ex: AlgorithmModule.InsufficientDataSize ) => {
            stopTimer( start )
            log.info(
              "[{}] skipped [{}] analysis on [{}] @ [{}] due to insufficient data - no outliers marked for interval",
              workId, algo, payload.plan.name + "][" + payload.topic, payload.source.interval
            )

            // don't let aggregator time out just due to error in algorithm
            aggregator !+ NoOutliers(
              algorithms = Set(algorithm.label),
              source = payload.source,
              plan = payload.plan,
              thresholdBoundaries = Map.empty[String, Seq[ThresholdBoundary]]
            )
          }

          case -\/( ex ) => {
            stopTimer( start )
            log.error(
              ex,
              "[{}] failed [{}] analysis on [{}] @ [{}] - no outliers marked for interval",
              workId, algo, payload.plan.name + "][" + payload.topic, payload.source.interval
            )
            // don't let aggregator time out just due to error in algorithm
            aggregator !+ NoOutliers(
              algorithms = Set(algorithm.label),
              source = payload.source,
              plan = payload.plan,
              thresholdBoundaries = Map.empty[String, Seq[ThresholdBoundary]]
            )
          }
        }
      }

      case RouteMessage( id, m ) if id == aggregateId && active.isDefinedAt( m ) => active( m )

      case AdvancedType( adv ) => persist( adv ) { accept }
    }

    import spotlight.analysis.algorithm.{ AlgorithmProtocol => AP }

    val stateReceiver: Receive = {
      case m: AP.EstimateSize => {
        sender() !+ AP.EstimatedSize(
          sourceId = aggregateId,
          nrShapes = state.shapes.size,
          size = estimateSize( state )( context.system )
        )
      }

      case m @ AP.GetTopicShapeSnapshot( _, topic ) => {
        sender() ! AP.TopicShapeSnapshot( aggregateId, algorithm.label, topic, state.shapes.get(topic) )
      }

      case m: AP.GetStateSnapshot => sender() ! AP.StateSnapshot( aggregateId, state )
    }

    val nonfunctional: Receive = {
      case StopMessageType( m ) => passivate() // not used in normal case as handled by AggregateRoot

      case e: SaveSnapshotSuccess => {
        log.debug(
          "[{}] save snapshot successful to:[{}]. deleting old snapshots before and journals -- meta:[{}]",
          self.path.name, e.metadata.sequenceNr, e.metadata
        )

        postSnapshot( e.metadata.sequenceNr, true )
      }

      case e @ SaveSnapshotFailure( meta, ex ) => {
        log.warning( "[{}] failed to save snapshot. meta:[{}] cause:[{}]", self.path.name, meta, ex )
        postSnapshot( lastSequenceNr - 1L, false )
      }

      case e: DeleteSnapshotsSuccess => {
        stopSnapshotSweepTimer( e )
       log.debug( "[{}] successfully cleared snapshots up to: [{}]", self.path.name, e.criteria )
      }

      case e @ DeleteSnapshotsFailure( criteria, cause ) => {
        stopSnapshotSweepTimer( e )
        log.warning( "[{}] failed to clear snapshots. meta:[{}] cause:[{}]", self.path.name, criteria, cause )
      }

      case e: DeleteMessagesSuccess => {
        stopJournalSweepTimer( e )
       log.debug( "[{}] successfully cleared journal: [{}]", self.path.name, e )
      }

      case e: DeleteMessagesFailure => {
        stopJournalSweepTimer( e )
       log.warning( "[{}] FAILED to clear journal will attempt to clear on subsequent snapshot: [{}]", self.path.name, e )
      }
    }

    object PassivationSaga {
      sealed trait Status
      case object Pending extends Status
      case object Complete extends Status
      case object Failed extends Status
    }

    case class PassivationSaga(
      snapshot: PassivationSaga.Status = PassivationSaga.Pending,
      snapshotSweep: PassivationSaga.Status = PassivationSaga.Pending,
      journalSweep: PassivationSaga.Status = PassivationSaga.Pending
    ) {
      def isComplete: Boolean = {
        val result = {
          ( snapshot != PassivationSaga.Pending ) &&
          ( snapshotSweep != PassivationSaga.Pending ) &&
          ( journalSweep != PassivationSaga.Pending )
        }

        log.debug( "AlgorithmModule[{}] passivation saga status: is-complete:[{}] saga:[{}]", self.path.name, result, this )
        result
      }

      override def toString: String = {
        s"PassivationSaga( snapshot:${snapshot} sweeps:[snapshot:${snapshotSweep} journal:${journalSweep}] )"
      }
    }

    def passivationStep( saga: PassivationSaga ): Unit = {
      if ( saga.isComplete ) {
        log.debug( "AlgorithmModule[{}] passivation completed. stopping actor with shapes:[{}]", self.path.name, state.shapes.size )
        context stop self
      } else {
        log.debug( "AlgorithmModule[{}] passivation progressing but not complete: [{}]", self.path.name, saga )
        context become LoggingReceive { around( passivating(saga) ) }
      }
    }

    def passivating( saga: PassivationSaga = PassivationSaga() ): Receive = {
      case StopMessageType( e ) => {
        stopPassivationTimer( e )
        log.warning( "AlgorithmModule[{}] immediate passivation upon repeat stop command", self.path.name )
        context stop self
      }

      case e: SaveSnapshotSuccess => {
        log.debug(
          "[{}] save passivation snapshot successful to:[{}]. deleting old snapshots before and journals -- meta:[{}]",
          self.path.name, e.metadata.sequenceNr, e.metadata
        )

        passivationStep( saga.copy( snapshot = PassivationSaga.Complete ) )
        postSnapshot( e.metadata.sequenceNr, true )
      }

      case e @ SaveSnapshotFailure( meta, ex ) => {
        log.warning( "[{}] failed to save passivation snapshot. meta:[{}] cause:[{}]", self.path.name, meta, ex )
        postSnapshot( lastSequenceNr - 1L, false )
        passivationStep( saga.copy( snapshot = PassivationSaga.Failed ) )
      }

      case e: DeleteSnapshotsSuccess => {
        stopSnapshotSweepTimer( e )
        log.debug( "AlgorithmModule[{}] on passivation successfully cleared snapshots up to: [{}]", self.path.name, e.criteria )
        passivationStep( saga.copy( snapshotSweep = PassivationSaga.Complete ) )
      }

      case e @ DeleteSnapshotsFailure( criteria, cause ) => {
        stopSnapshotSweepTimer( e )
        log.warning( "AlgorithmModule[{}] on passivation failed to clear snapshots. meta:[{}] cause:[{}]", self.path.name, criteria, cause )
        passivationStep( saga.copy( snapshotSweep = PassivationSaga.Failed ) )
      }


      case e: DeleteMessagesSuccess => {
        stopJournalSweepTimer( e )
        log.debug( "AlgorithmModule[{}] on passivation successfully cleared journal: [{}]", self.path.name, e )
        stopPassivationTimer( e )
        passivationStep( saga.copy( journalSweep = PassivationSaga.Complete ) )
      }

      case e: DeleteMessagesFailure => {
        stopJournalSweepTimer( e )
        log.warning(
          "[{}] on passivation FAILED to clear journal will attempt to clear on subsequent snapshot: [{}]",
          self.path.name,
          e
        )

        stopPassivationTimer( e )
        passivationStep( saga.copy( journalSweep = PassivationSaga.Failed ) )
      }
    }

    override def unhandled( message: Any ): Unit = {
      message match {
        case m: DetectUsing => {
          log.error( "[{}] algorithm [{}] does not recognize requested payload: [{}]", self.path.name, algorithm, m )
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
        events <- collectOutlierPoints( data ) >=> recordAdvancements
        o <- makeOutliers( events )
      } yield ( o, ctx )
    }

    val toOutliers = kleisli[TryV, (Outliers, Context), Outliers] { case (o, _) => o.right }


    case class Accumulation( topic: Topic, shape: Shape, advances: Seq[Advanced] = Seq.empty[Advanced] )

    private def collectOutlierPoints( points: Seq[DoublePoint] ): KOp[Context, Seq[Advanced]] = {
      kleisli[TryV, Context, Seq[Advanced]] { implicit analysisContext =>
        def tryStep( pt: PointT, shape: Shape ): TryV[(Boolean, ThresholdBoundary)] = {
          \/ fromTryCatchNonFatal {
//            logger.debug( "algorithm {}.step( {} ): before-shape=[{}]", algorithm.label.name, pt, shape.toString )

            algorithm
            .step( pt, shape )( state, analysisContext )
            .getOrElse {
//              logger.debug(
//                "skipping point[{}] per insufficient history for algorithm {}",
//                s"(${pt.dateTime}:${pt.timestamp.toLong}, ${pt.value})", algorithm.label
//              )

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
                  ot <- tryStep( pt, acc.shape )
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

//                    log.debug(
//                      "{} STEP:{} [{}]: AnalysisState.Advanced:[{}]",
//                      algorithm.label.name,
//                      if ( isOutlier ) "ANOMALY" else "regular",
//                      s"ts:[${pt.dateTime}:${pt._1.toLong}] eff-value:[${pt.value}] orig-value:[${original.value}]",
//                      event
//                    )

                    acc.copy(
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

        loop(
          points = points.toList,
          accumulation = Accumulation(
            topic = analysisContext.topic,
            shape = state.shapes.get(analysisContext.topic) getOrElse shapeCompanion.zero( Option(analysisContext.configuration) )
          ).right
        )
      }
    }

    private val recordAdvancements: KOp[Seq[Advanced], Seq[Advanced]] = {
      kleisli[TryV, Seq[Advanced], Seq[Advanced]] { advances =>
        persistAll( advances.to[scala.collection.immutable.Seq] ){ accept }
        advances.right
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


    // -- nonfunctional operations --
    var _snapshotStarted: Long = 0L
    def startSnapshotTimer(): Unit = { _snapshotStarted = System.currentTimeMillis() }
    def stopSnapshotTimer( success: Boolean ): Unit = {
      if ( 0 < _snapshotStarted ) {
        AlgorithmModule.snapshotTimer.update( System.currentTimeMillis() - _snapshotStarted, MILLISECONDS )
        _snapshotStarted = 0L
      }

      if ( success ) AlgorithmModule.snapshotSuccesses.mark() else AlgorithmModule.snapshotFailures.mark()
    }

    var _snapshotSweepStarted: Long = 0L
    def startSnapshotSweepTimer(): Unit = { _snapshotSweepStarted = System.currentTimeMillis() }
    def stopSnapshotSweepTimer( event: Any ): Unit = {
      if ( 0 < _snapshotSweepStarted) {
        AlgorithmModule.snapshotSweepTimer.update( System.currentTimeMillis() - _snapshotSweepStarted, MILLISECONDS )
        _snapshotSweepStarted= 0L
      }

      event match {
        case _: DeleteSnapshotsSuccess => AlgorithmModule.snapshotSweepSuccesses.mark()
        case _: DeleteSnapshotsFailure=> AlgorithmModule.snapshotSweepFailures.mark()
        case _ => { }
      }
    }


    var _journalSweepStarted: Long = 0L
    def startJournalSweepTimer(): Unit = { _journalSweepStarted = System.currentTimeMillis() }
    def stopJournalSweepTimer( event: Any ): Unit = {
      if ( 0 < _journalSweepStarted ) {
        AlgorithmModule.journalSweepTimer.update( System.currentTimeMillis() - _journalSweepStarted, MILLISECONDS )
        _journalSweepStarted = 0L
      }

      event match {
        case _: DeleteMessagesSuccess => AlgorithmModule.journalSweepSuccesses.mark()
        case _: DeleteMessagesFailure => AlgorithmModule.journalSweepFailures.mark()
        case _ => { }
      }
    }

    var _passivationStarted: Long = 0L
    def startPassivationTimer(): Unit = { _passivationStarted = System.currentTimeMillis() }
    def stopPassivationTimer( event: Any ): Unit = {
      if ( 0 < _passivationStarted ) {
        AlgorithmModule.passivationTimer.update( System.currentTimeMillis() - _passivationStarted, MILLISECONDS )
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

    def postSnapshot( snapshotSequenceNr: Long, success: Boolean ): Unit = {
      log.debug( "[{}] postSnapshot event:[{}] success:[{}]", self.path.name, snapshotSequenceNr, success )
      stopSnapshotTimer( success )

      startSnapshotSweepTimer()
      deleteSnapshots( SnapshotSelectionCriteria( maxSequenceNr = snapshotSequenceNr - 1 ) )

      startJournalSweepTimer()
      deleteMessages( snapshotSequenceNr )
    }

    override protected def onPersistFailure( cause: Throwable, event: Any, seqNr: Long ): Unit = {
      log.error( "AlgorithmModule[{}] onPersistFailure cause:[{}] event:[{}] seqNr:[{}]", self.path.name, cause, event, seqNr )
      super.onPersistFailure( cause, event, seqNr )
    }

    override protected def onRecoveryFailure( cause: Throwable, event: Option[Any] ): Unit = {
      log.error( "AlgorithmModule[{}] onRecoveryFailure cause:[{}] event:[{}]", self.path.name, cause, event )
      super.onRecoveryFailure( cause, event )
    }
  }
}
