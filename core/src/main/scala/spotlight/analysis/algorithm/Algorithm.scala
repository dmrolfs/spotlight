package spotlight.analysis.algorithm

import java.io.Serializable

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{ Failure, Random, Success, Try }
import scala.reflect._
import akka.actor._
import akka.cluster.sharding.{ ClusterShardingSettings, ShardRegion }
import akka.event.LoggingReceive
import akka.persistence._
import cats.data.Kleisli
import cats.data.Kleisli.ask
import cats.data.Validated.{ Invalid, Valid }
import cats.syntax.either._
import cats.instances.either._
import cats.syntax.applicative._

//import bloomfilter.mutable.BloomFilter
import bloomfilter.CanGenerateHashFrom
import bloomfilter.CanGenerateHashFrom.CanGenerateHashFromString
import com.typesafe.config.Config
import com.typesafe.config.ConfigException.BadValue
import net.ceedubs.ficus.Ficus._
import org.apache.commons.math3.ml.clustering.DoublePoint
import com.codahale.metrics.{ Metric, MetricFilter }
import nl.grons.metrics.scala.{ Meter, MetricName, Timer }
import squants.information.{ Bytes, Information }
import com.persist.logging._
import omnibus.akka.envelope._
import omnibus.akka.metrics.{ Instrumented, InstrumentedActor }
import omnibus.akka.persistence.{ Passivating, SnapshotLimiter }
import omnibus.akka.publish.{ EventPublisher, StackableStreamPublisher }
import omnibus.archetype.domain.model.core.Entity
import omnibus.commons._
import omnibus.commons.identifier.{ Identifying, ShortUUID, TaggedID }
import demesne._
import demesne.repository._
import spotlight.analysis._
import spotlight.analysis.algorithm.Advancing.syntax._
import spotlight.analysis.algorithm.AlgorithmProtocol.RouteMessage
import spotlight.model.outlier.{ NoOutliers, Outliers }
import spotlight.model.timeseries._
import spotlight.{ Settings, SpotlightContext }
import spotlight.infrastructure.ClusterRole

case class AlgorithmState[S](
    override val id: Algorithm.TID,
    override val name: String,
    shapes: Map[Topic, S] = Map.empty[Topic, S]
) extends Entity with Equals {
  override type ID = Algorithm.ID
  type Shape = S
  def withTopicShape( topic: Topic, shape: Shape ): AlgorithmState[S] = copy( shapes = shapes + ( topic → shape ) )

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
      case that: AlgorithmState[S] ⇒ {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.id.id == that.id.id ) &&
            ( this.name == that.name ) &&
            ( this.shapes == that.shapes )
        }
      }

      case _ ⇒ false
    }
  }

  override def toString: String = s"""State( algorithm:${name} id:[${id}] shapesNr:[${shapes.size}] )"""
}

case class SaveAlgorithmSnapshot(
  override val targetId: TaggedID[_],
  correlation: Option[Any] = None
) extends CommandLike with NotInfluenceReceiveTimeout {
  override type ID = Any
}


/** Created by rolfsd on 6/5/16.
  */
object Algorithm extends ClassLogging {
  type ID = AlgorithmIdentifier
  type TID = TaggedID[ID]

  object ConfigurationProvider {
    val MinimalPopulationPath = "minimum-population"
    val TailAveragePath = "tail-average"
    val MaxClusterNodesNrPath = "max-cluster-nodes-nr"
    val ApproxPointsPerDayPath = "approximate-points-per-day"
  }

  trait ConfigurationProvider {
    /** approximate number of points in a one day window size @ 1 pt per 10s
      */
    val ApproximateDayWindow: Int = 6 * 60 * 24

    //todo: provide a link to global algorithm configuration
    val maximumNrClusterNodes: Int = 6
  }

  private[algorithm] val gitterFactor: Random = new Random()

  case class RedundantAlgorithmConfiguration(
    aggregateId: TID,
    path: String,
    value: Any
  ) extends RuntimeException( s"For algorithm ${aggregateId}: ${path}:${value} is redundant" )

  case class InvalidAlgorithmConfiguration(
    algorithm: String,
    path: String,
    requirement: String
  ) extends BadValue( path, s"For algorithm, ${algorithm}, ${path} must: ${requirement}" )

  class ShardMonitor( algorithmLabel: String ) {
    def shards: Long = 0L // _shards.get()._1
    //    val _shards: Agent[( Long, BloomFilter[Algorithm.ID] )] = {
    //      todo make config driven
    //      Agent( ( 0L, BloomFilter[Algorithm.ID]( numberOfItems = 10000, falsePositiveRate = 0.1 ) ) )( ExecutionContext.global )
    //    }

    def :+( id: Algorithm.ID ): Unit = {
      //      _shards send { css ⇒
      //        val ( c, ss ) = css
      //        if ( ss mightContain id ) ( c, ss )
      //        else {
      //          log.info( Map( "@msg" → "created new algorithm shard", "algorithm" → algorithmLabel, "id" → id.toString ) )
      //          ss add id
      //          ( c + 1L, ss )
      //        }
      //      }
    }
  }

  object ShardMonitor {
    implicit val canGenerateShortUUIDHash = new CanGenerateHashFrom[ShortUUID] {
      override def generateHash( from: ShortUUID ): Long = CanGenerateHashFromString generateHash from.toString
    }

    val ShardsMetricName = "shards"
  }

  object Metrics extends Instrumented {
    override lazy val metricBaseName: MetricName = MetricName( classOf[Algorithm[_]] )

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
}

abstract class Algorithm[S <: Serializable: Advancing]( val label: String )
    extends Algorithm.ConfigurationProvider with Instrumented with ClassLogging {
  algorithm ⇒

  type Shape = S
  type State = AlgorithmState[S]

  val advancing: Advancing[Shape] = shapeless.the[Advancing[Shape]]

  //todo: pull into type parameter with TypeClass for make
  type Context <: AlgorithmContext
  def makeContext( message: DetectUsing, state: Option[State] ): Context

  def prepareData( c: Context ): Seq[DoublePoint] = {
    import Algorithm.ConfigurationProvider.TailAveragePath
    val tail = c.properties.as[Option[Int]]( TailAveragePath ) getOrElse 1
    if ( 1 < tail ) { c.tailAverage( tail )( c.data ) } else c.data
  }

  def score( point: PointT, shape: Shape )( implicit c: Context ): Option[AnomalyScore]

  implicit lazy val identifying: Identifying.Aux[State, Algorithm.ID] = new Identifying[State] {
    override type ID = Algorithm.ID
    override val idTag: Symbol = Symbol( algorithm.label )
    override def tidOf( s: AlgorithmState[Shape] ): TID = s.id
    override def nextTID: ErrorOr[TID] = Left( new IllegalStateException( "Algorithm TIDs are created via AlgorithmRoutes" ) )
    override def idFromString( idRep: String ): ID = AlgorithmIdentifier.fromAggregateId( idRep ).unsafeGet
  }

  val shardMonitor: Algorithm.ShardMonitor = new Algorithm.ShardMonitor( label )

  /** Used to answer EstimateSize requests. If the algorithm overrides estimatedAverageShapeSize to provide a reasonable
    * shape size factor, this function will quickly multiply the factor by the number of shapes managed by the actor.
    * Otherwise, this function relies on serializing current state, which is becomes very expensive and possibly untenable as
    * topics increase.
    *
    * It is recommended that algorithms override the estimatedAverageShapeSize to support the more efficient calculations, and
    * the EstimatedSize message reports on the average shape size to help determine this factor.
    * @return Information byte size
    */
  def estimateSize( state: State, properties: Option[Config] )( implicit system: ActorSystem ): Information = {
    algorithm.estimatedAverageShapeSize( properties )
      .map { _ * state.shapes.size }
      .getOrElse {
        import akka.serialization.{ SerializationExtension, Serializer }
        val serializer: Serializer = SerializationExtension( system ) findSerializerFor state
        val bytes = akka.util.ByteString( serializer toBinary state )
        val sz = Bytes( bytes.size )

        log.info(
          Map(
            "@msg" → "algorithm estimateSize relying on serialization - look to optimize",
            "algorithm" → label,
            "size" → sz.toString,
            "nr-shapes" → state.shapes.size
          )
        )

        sz
      }
  }

  /** Optimization available for algorithms to more efficiently respond to size estimate requests for algorithm sharding.
    * @return blended average size for the algorithm shape
    */
  def estimatedAverageShapeSize( properties: Option[Config] ): Option[Information] = None

  initializeMetrics()

  override lazy val metricBaseName: MetricName = {
    MetricName( spotlight.BaseMetricName, spotlight.analysis.BaseMetricName, "algorithm", label )
  }

  def initializeMetrics(): Unit = {
    stripLingeringMetrics()

    Try { metrics.gauge( Algorithm.ShardMonitor.ShardsMetricName ) { algorithm.shardMonitor.shards } } match {
      case Success( g ) ⇒ log.debug( Map( "@msg" → "gauge created", "gauge" → Algorithm.ShardMonitor.ShardsMetricName ) )
      case Failure( ex ) ⇒ log.debug( Map( "@msg" → "gauge already exists", "gauge" → Algorithm.ShardMonitor.ShardsMetricName ) )
    }
  }

  def stripLingeringMetrics(): Unit = {
    metrics.registry.removeMatching(
      new MetricFilter {
        override def matches( name: String, metric: Metric ): Boolean = {
          name.contains( classOf[PlanCatalog].getName ) && name.contains( Algorithm.ShardMonitor.ShardsMetricName )
        }
      }
    )
  }

  // module needs explicit algorithm.identifying to the module. Relying on implicit passing results in some fault that may be
  // an infinite loop -- tests were very short on symptoms.
  val module: AggregateRootModule[State, Algorithm.ID] = new AlgorithmModule()( algorithm.identifying )

  class AlgorithmModule( implicit override val identifying: Identifying.Aux[State, Algorithm.ID] )
      extends AggregateRootModule[State, Algorithm.ID]()( identifying ) with ClassLogging { algorithmModule ⇒

    import AlgorithmProtocol.Advanced

    override def toString: String = s"AlgorithmModule( ${shapeless.the[Identifying[State]].idTag.name} )"

    override lazy val shardName: String = algorithm.label

    val AdvancedType: ClassTag[Advanced] = classTag[Advanced]

    override lazy val rootType: AggregateRootType = new RootType

    //  def toCamelCase( name: String ): String = {
    //    val regex = """-(\w)""".r
    //    val result = regex.replaceAllIn( name, m => m.subgroups.head.toUpperCase )
    //    result.head.toUpper + result.tail
    //  }

    class RootType extends AggregateRootType {
      override type S = State
      override val identifying: Identifying[S] = algorithmModule.identifying

      override val clusterRole: Option[String] = Option( ClusterRole.Analysis.entryName )

      //todo: make configuration driven for algorithms
      override val snapshotPeriod: Option[FiniteDuration] = None // not used - snapshot timing explicitly defined below
      //    override def snapshot: Option[SnapshotSpecification] = None
      override def snapshot: Option[SnapshotSpecification] = {
        import scala.concurrent.duration._

        Some(
          new SnapshotSpecification {
            override def saveSnapshotCommand[ID]( tid: TaggedID[ID] ): Any = SaveAlgorithmSnapshot( targetId = tid )
            override val snapshotInterval: FiniteDuration = 2.minutes // 5.minutes okay //todo make config driven
            override val snapshotInitialDelay: FiniteDuration = {
              val delay = snapshotInterval + snapshotInterval * Algorithm.gitterFactor.nextDouble()
              FiniteDuration( delay.toMillis, MILLISECONDS )
            }
          }
        )
      }

      override val passivateTimeout: Duration = Duration( 1, MINUTES ) //todo make config driven

      override lazy val name: String = algorithmModule.shardName
      override def repositoryProps( implicit model: DomainModel ): Props = {
        val forceLocal = Settings forceLocalFrom model.configuration
        val ( props, label ) = if ( forceLocal ) {
          ( Repository.localProps( model ), "LOCAL" )
        } else {
          ( Repository.clusteredProps( model ), "CLUSTERED" )
        }

        log.alternative(
          SpotlightContext.SystemLogCategory,
          Map( "@msg" → "Algorithm Environment", "label" → label, "force-local" → forceLocal, "props" → props.toString )
        )

        props
      }

      override def maximumNrClusterNodes: Int = algorithm.maximumNrClusterNodes
      override def aggregateIdFor: ShardRegion.ExtractEntityId = super.aggregateIdFor orElse {
        case AdvancedType( a ) ⇒ ( a.sourceId.id.toString, a )
      }
      override def toString: String = "Algorithm:" + name
    }

    object Repository {
      def localProps( model: DomainModel ): Props = Props( new LocalRepository( model ) )
      def clusteredProps( model: DomainModel ): Props = Props( new ClusteredRepository( model ) )
    }

    class Repository( model: DomainModel )
        extends EnvelopingAggregateRootRepository( model, algorithmModule.rootType ) with AltActorLogging {
      actor: AggregateContext ⇒

      override def aggregateProps: Props = AlgorithmActor.props( model, rootType )

      //todo    import demesne.repository.{ StartProtocol => SP }
      //    override def doLoad(): Loaded = super.doLoad()
      //    override def doInitialize( resources: Map[Symbol, Any] ): Valid[Done] = super.doInitialize( resources )
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
        if ( !rootType.aggregateIdFor.isDefinedAt( command ) ) {
          altLog.warn(
            Map(
              "@msg" → "aggregate root type does not recognize command",
              "root-type" → rootType.name,
              "command" → command.toString
            )
          )
        }
        val ( id, _ ) = rootType aggregateIdFor command
        AlgorithmIdentifier.fromAggregateId( id ) match {
          case Valid( aid ) ⇒ {
            val options = backoffOptionsFor( id )
            val name = "backoff:" + aid.span
            context.child( name ) getOrElse { context.actorOf( BackoffSupervisor.props( options ), name ) }
          }

          case Invalid( exs ) ⇒ {
            exs map { ex ⇒
              altLog.error(
                Map(
                  "@msg" → "failed to parse aggregateId via root type",
                  "aggregateId" → id.toString,
                  "root-type" → rootType.name
                ),
                ex
              )
            }
            throw exs.head
          }
        }
      }
    }

    class ClusteredRepository( model: DomainModel ) extends Repository( model ) with ClusteredAggregateContext

    object AlgorithmActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

      private class Default( model: DomainModel, rootType: AggregateRootType )
        extends AlgorithmActor( model, rootType ) with StackableStreamPublisher
    }

    class AlgorithmActor( override val model: DomainModel, override val rootType: AggregateRootType )
        extends AggregateRoot[State, ID]
        with SnapshotLimiter
        with Passivating
        with EnvelopingActor
        with AggregateRoot.Provider
        with InstrumentedActor
        with AltActorLogging {
      publisher: EventPublisher ⇒

      setInactivityTimeout( inactivity = rootType.passivation.inactivityTimeout, message = ReceiveTimeout /*StopAggregateRoot(aggregateId)*/ )

      override val journalPluginId: String = "akka.persistence.algorithm.journal.plugin"
      override val snapshotPluginId: String = "akka.persistence.algorithm.snapshot.plugin"

      altLog.info(
        Map(
          "@msg" → "AlgorithmActor instantiated",
          "algorithm" → algorithm.label,
          "id" → aggregateId.toString,
          "inactivity-timeout" → rootType.passivation.inactivityTimeout.toString
        )
      )

      override def preStart(): Unit = {
        super.preStart()
        algorithm.shardMonitor :+ aggregateId
      }

      override def postStop(): Unit = {
        clearInactivityTimeout()
        altLog.info( Map( "@msg" → "actor stopped", "actor" → self.path.name, "shapes-nr" → state.shapes.size ) )
        super.postStop()
      }

      override def passivate(): Unit = {
        startPassivationTimer()
        if ( isRedundantSnapshot ) {
          import PassivationSaga.Complete
          context become LoggingReceive { around( active orElse passivating( PassivationSaga( snapshot = Complete ) ) ) }
          altLog.info( Map( "@msg" → "passivation started with current snapshot", "actor" → self.path.name ) )
          postSnapshot( lastSequenceNr - 1L, true )
        } else {
          context become LoggingReceive { around( active orElse passivating() ) }
          altLog.info( Map( "@msg" → "passivation started with old snapshot", "actor" → self.path.name ) )
          startPassivationTimer()
          saveSnapshot( state )
        }
      }

      override def persistenceIdFromPath(): String = aggregateIdFromPath().toString

      override lazy val metricBaseName: MetricName = algorithm.metricBaseName
      lazy val executionTimer: Timer = metrics.timer( "execution" )

      override var state: State = AlgorithmState[Shape]( aggregateId, algorithm.label )
      //      override lazy val evState: ClassTag[State] = ClassTag( classOf[State] )

      var algorithmContext: Context = _

      //todo: experimental feature. disabled via MaxValue for now - initial tests helped to limit to 10000 topics for inmem
      var _advances: Int = 0
      val AdvanceLimit: Int = Int.MaxValue // 10000

      override val acceptance: Acceptance = {
        case ( AdvancedType( event ), cs ) ⇒ {
          val s = cs.shapes.get( event.topic ) getOrElse advancing.zero( Option( algorithmContext ) map { ctx ⇒ ctx.properties } )
          _advances += 1
          val newShape = s.advance( event )
          val newState = cs.withTopicShape( event.topic, newShape )

          altLog.debug( Map( "@msg" → "#TEST acceptance", "before" → s.toString, "advanced" → newShape.toString, "advances" → _advances ) )

          if ( AdvanceLimit < _advances ) {
            altLog.debug( "advance limit exceed before snapshot time - taking snapshot with sweeps.." )
            _advances = 0
            saveSnapshot( newState )
          }

          newState
        }
      }

      override def receiveCommand: Receive = LoggingReceive { around( active orElse stateReceiver orElse nonfunctional ) }

      val active: Receive = {
        case msg @ DetectUsing( _, algo, payload: DetectOutliersInSeries, _, _ ) ⇒ {
          val aggregator = sender()

          val start = System.currentTimeMillis()
          @inline def stopTimer( start: Long ): Unit = {
            executionTimer.update( System.currentTimeMillis() - start, scala.concurrent.duration.MILLISECONDS )
          }

          ( makeAlgorithmContext andThen findOutliers andThen toOutliers ).run( msg ) match {
            case Right( r ) ⇒ {
              stopTimer( start )
              //            log.debug( "[{}] sending detect result to aggregator[{}]: [{}]", workId, aggregator.path.name, r )
              aggregator !+ r
            }

            case Left( ex: NonFatalSkipScoringException ) ⇒ {
              stopTimer( start )
              altLog.info(
                Map(
                  "@msg" → "skipped algorithm analysis",
                  "message" → ex.getMessage,
                  "work-id" → workId.toString,
                  "algorithm" → algo,
                  "scope" → Map( "plan" → payload.plan.name, "topic" → payload.topic.toString ),
                  "interval" → payload.source.interval.toString
                ),
                id = requestId
              )

              // don't let aggregator time out just due to error in algorithm
              aggregator !+ NoOutliers(
                algorithms = Set( algorithm.label ),
                source = payload.source,
                plan = payload.plan,
                thresholdBoundaries = Map.empty[String, Seq[ThresholdBoundary]]
              )
            }

            case Left( ex ) ⇒ {
              stopTimer( start )
              altLog.error(
                Map(
                  "@msg" → "failed algorithm analysis - no outliers marked for interval",
                  "work-id" → workId.toString,
                  "algorithm" → algo,
                  "scope" → Map( "plan" → payload.plan.name, "topic" → payload.topic.toString ),
                  "interval" → payload.source.interval.toString
                ),
                ex,
                id = requestId
              )
              // don't let aggregator time out just due to error in algorithm
              aggregator !+ NoOutliers(
                algorithms = Set( algorithm.label ),
                source = payload.source,
                plan = payload.plan,
                thresholdBoundaries = Map.empty[String, Seq[ThresholdBoundary]]
              )
            }
          }
        }

        case RouteMessage( id, m ) if id == aggregateId && active.isDefinedAt( m ) ⇒ active( m )

        case AdvancedType( adv ) ⇒ persist( adv ) { accept }
      }

      import spotlight.analysis.algorithm.{ AlgorithmProtocol ⇒ AP }

      val stateReceiver: Receive = {
        case m: AP.EstimateSize ⇒ {
          sender() !+ AP.EstimatedSize(
            sourceId = aggregateId,
            nrShapes = state.shapes.size,
            size = estimateSize( state, Option( algorithmContext ).map( _.properties ) )( context.system )
          )
        }

        case m @ AP.GetTopicShapeSnapshot( _, topic ) ⇒ {
          sender() ! AP.TopicShapeSnapshot( aggregateId, algorithm.label, topic, state.shapes.get( topic ) )
        }

        case m: AP.GetStateSnapshot ⇒ sender() ! AP.StateSnapshot( aggregateId, state )
      }

      val nonfunctional: Receive = {
        case StopMessageType( m ) ⇒ passivate() // not used in normal case as handled by AggregateRoot

        case e: SaveSnapshotSuccess ⇒ {
          altLog.debug(
            Map(
              "@msg" → "save snapshot successful. deleting old snapshots and journals",
              "actor" → self.path.name,
              "sequence-nr" → e.metadata.sequenceNr,
              "meta" → e.metadata.toString
            ),
            id = requestId
          )

          postSnapshot( e.metadata.sequenceNr, true )
        }

        case e @ SaveSnapshotFailure( meta, ex ) ⇒ {
          altLog.warn(
            Map( "@msg" → "failed to save snapshot", "actor" → self.path.name, "meta" → e.metadata.toString ),
            ex,
            id = requestId
          )
          postSnapshot( lastSequenceNr - 1L, false )
        }

        case e: DeleteSnapshotsSuccess ⇒ {
          stopSnapshotSweepTimer( e )
          altLog.debug(
            Map( "@msg" → "successfully cleared snapshots", "actor" → self.path.name, "to" → e.criteria.toString ),
            id = requestId
          )
        }

        case e @ DeleteSnapshotsFailure( criteria, cause ) ⇒ {
          stopSnapshotSweepTimer( e )
          altLog.warn(
            Map( "@msg" → "failed to clear snapshots", "actor" → self.path.name, "criteria" → e.criteria.toString ),
            cause,
            id = requestId
          )
        }

        case e: DeleteMessagesSuccess ⇒ {
          stopJournalSweepTimer( e )
          altLog.debug(
            Map( "@msg" → "successfully cleared journal", "actor" → self.path.name, "event" → e.toString ),
            id = requestId
          )
        }

        case e: DeleteMessagesFailure ⇒ {
          stopJournalSweepTimer( e )
          altLog.warn(
            Map(
              "@msg" → "failed to clear journal. will try again on subsequent snapshot",
              "actor" → self.path.name,
              "event" → e.toString
            ),
            id = requestId
          )
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
      ) extends ClassLogging {
        def isComplete: Boolean = {
          val result = {
            ( snapshot != PassivationSaga.Pending ) &&
              ( snapshotSweep != PassivationSaga.Pending ) &&
              ( journalSweep != PassivationSaga.Pending )
          }

          log.debug(
            Map(
              "@msg" → "passivation saga status",
              "actor" → self.path.name,
              "is-complete" → result.toString,
              "status" → Map(
                "snapshot" → snapshot.toString,
                "snapshot-sweep" → snapshotSweep.toString,
                "journal-sweep" → journalSweep.toString
              )
            ),
            id = requestId
          )

          result
        }

        override def toString: String = {
          s"PassivationSaga( snapshot:${snapshot} sweeps:[snapshot:${snapshotSweep} journal:${journalSweep}] )"
        }
      }

      def passivationStep( saga: PassivationSaga ): Unit = {
        if ( saga.isComplete ) {
          altLog.debug(
            Map(
              "@msg" → "passivation completed. stopping actor with shapes",
              "actor" → self.path.name,
              "shapes-nr" → state.shapes.size
            ),
            id = requestId
          )

          context stop self
        } else {
          altLog.debug(
            Map(
              "@msg" → "passivation progressing but not complete",
              "actor" → self.path.name,
              "status" → Map(
                "snapshot" → saga.snapshot.toString,
                "snapshot-sweep" → saga.snapshotSweep.toString,
                "journal-sweep" → saga.journalSweep.toString
              )
            ),
            id = requestId
          )

          context become LoggingReceive { around( passivating( saga ) ) }
        }
      }

      def passivating( saga: PassivationSaga = PassivationSaga() ): Receive = {
        case StopMessageType( e ) ⇒ {
          stopPassivationTimer( e )

          altLog.warn(
            Map( "@msg" → "immediate passivation upon repeat stop command", "actor" → self.path.name ),
            id = requestId
          )

          context stop self
        }

        case e: SaveSnapshotSuccess ⇒ {
          altLog.debug(
            Map(
              "@msg" → "save passivation snapshot successful. deleting old snapshots and journals",
              "actor" → self.path.name,
              "sequence-nr" → e.metadata.sequenceNr,
              "metadata" → e.metadata
            ),
            id = requestId
          )

          passivationStep( saga.copy( snapshot = PassivationSaga.Complete ) )
          postSnapshot( e.metadata.sequenceNr, true )
        }

        case e @ SaveSnapshotFailure( meta, ex ) ⇒ {
          altLog.warn(
            Map( "@msg" → "failed to save passivation snapshot", "actor" → self.path.name, "metadata" → meta.toString ),
            ex,
            id = requestId
          )
          postSnapshot( lastSequenceNr - 1L, false )
          passivationStep( saga.copy( snapshot = PassivationSaga.Failed ) )
        }

        case e: DeleteSnapshotsSuccess ⇒ {
          stopSnapshotSweepTimer( e )
          altLog.debug(
            Map( "@msg" → "passivation successfully cleared snapshots", "actor" → self.path.name, "to" → e.criteria.toString ),
            id = requestId
          )
          passivationStep( saga.copy( snapshotSweep = PassivationSaga.Complete ) )
        }

        case e @ DeleteSnapshotsFailure( criteria, cause ) ⇒ {
          stopSnapshotSweepTimer( e )
          altLog.warn(
            Map( "@msg" → "passivation failed to clear snapshots", "actor" → self.path.name, "criteria" → criteria.toString ),
            cause,
            id = requestId
          )
          passivationStep( saga.copy( snapshotSweep = PassivationSaga.Failed ) )
        }

        case e: DeleteMessagesSuccess ⇒ {
          stopJournalSweepTimer( e )
          altLog.debug(
            Map( "@msg" → "passivation successfully cleared journal", "actor" → self.path.name, "event" → e.toString ),
            id = requestId
          )
          stopPassivationTimer( e )
          passivationStep( saga.copy( journalSweep = PassivationSaga.Complete ) )
        }

        case e: DeleteMessagesFailure ⇒ {
          stopJournalSweepTimer( e )
          altLog.warn(
            Map(
              "@msg" → "passivation FAILED to clear journal will attempt to clear on subsequent snapshot",
              "actor" → self.path.name,
              "event" → e.toString
            ),
            id = requestId
          )

          stopPassivationTimer( e )
          passivationStep( saga.copy( journalSweep = PassivationSaga.Failed ) )
        }
      }

      override def unhandled( message: Any ): Unit = {
        message match {
          case m: DetectUsing ⇒ {
            altLog.error(
              Map(
                "@msg" → "algorithm does not recognize requested payload",
                "actor" → self.path.name,
                "algorithm" → algorithm,
                "payload" → m.toString
              )
            )
            sender() !+ UnrecognizedPayload( algorithm.label, m )
          }

          case m ⇒ super.unhandled( m )
        }
      }

      // -- algorithm functional elements --
      val makeAlgorithmContext: KOp[DetectUsing, Context] = Kleisli[ErrorOr, DetectUsing, Context] { message ⇒
        //todo: with algorithm global config support. merge with state config (probably persist and accept ConfigChanged evt)
        Either catchNonFatal {
          algorithmContext = algorithm.makeContext( message, Option( state ) )
          algorithmContext
        }
      }

      def findOutliers: KOp[Context, ( Outliers, Context )] = {
        for {
          ctx ← ask[ErrorOr, Context]
          data ← Kleisli[ErrorOr, Context, Seq[DoublePoint]] { c ⇒ Either catchNonFatal { algorithm prepareData c } }
          events ← collectOutlierPoints( data ) andThen recordAdvancements
          o ← makeOutliers( events )
        } yield ( o, ctx )
      }

      val toOutliers = Kleisli[ErrorOr, ( Outliers, Context ), Outliers] { case ( o, _ ) ⇒ o.asRight }

      case class Accumulation( topic: Topic, shape: Shape, advances: Seq[Advanced] = Seq.empty[Advanced] )

      /** The test should not be used for sample sizes of six or fewer since it frequently tags most of the points as outliers.
        * @return
        */
      private def checkSize( shape: S )( implicit analysisContext: Context ): ErrorOr[Long] = {
        import Algorithm.ConfigurationProvider.MinimalPopulationPath

        val minimumDataPoints = analysisContext.properties.as[Option[Int]]( MinimalPopulationPath ) getOrElse 0

        shape.N match {
          case s if s < minimumDataPoints ⇒ InsufficientDataSize( label, s, minimumDataPoints ).asLeft
          case s ⇒ s.asRight
        }
      }

      private def checkScore( score: AnomalyScore, shape: Shape )( implicit analysisContext: Context ): ErrorOr[AnomalyScore] = {
        ( score.threshold.floor, score.threshold.expected, score.threshold.ceiling ) match {
          case ( Some( f ), Some( e ), Some( c ) ) if f == e && e == c ⇒ {
            InsufficientVariance( algorithm.label, shape.N ).asLeft
          }
          case ( Some( f ), _, Some( c ) ) if f.isNaN && c.isNaN ⇒ InsufficientVariance( algorithm.label, shape.N ).asLeft
          case _ ⇒ score.asRight
        }
      }

      private def collectOutlierPoints( points: Seq[DoublePoint] ): KOp[Context, Seq[Advanced]] = {
        Kleisli[ErrorOr, Context, Seq[Advanced]] { implicit analysisContext ⇒
          val workingShape = state.shapes.get( analysisContext.topic ) match {
            case None ⇒ advancing zero Option( analysisContext.properties )
            case Some( shape ) ⇒ shape.copy
          }

          @tailrec def loop( points: List[DoublePoint], accumulation: ErrorOr[Accumulation] ): ErrorOr[Seq[Advanced]] = {
            points match {
              case Nil ⇒ accumulation map { _.advances }

              case pt :: tail ⇒ {
                val newAcc = {
                  for {
                    acc ← accumulation
                    score ← tryScoring( pt, acc.shape )
                  } yield {
                    analysisContext.data
                      .find { _.timestamp == pt.timestamp }
                      .map { original ⇒
                        val event = Advanced(
                          sourceId = aggregateId,
                          topic = acc.topic,
                          point = original.toDataPoint,
                          isOutlier = score.isOutlier,
                          threshold = score.threshold
                        )

                        altLog.debug(
                          Map(
                            "@msg" → "STEP",
                            "properties" → analysisContext.properties.toString,
                            "algorithm" → algorithm.label,
                            "assessment" → ( if ( score.isOutlier ) "ANOMALY" else "normal" ),
                            "point" → Map(
                              "datetime" → pt.dateTime.toString,
                              "timestamp" → pt.timestamp.toLong,
                              "tail-averaged-value" → f"${pt.value}%2.5f",
                              "original-value" → f"${original.value}%2.5f"
                            ),
                            "event" → Map(
                              "topic" → event.topic.toString,
                              "tags" → event.tags.toString,
                              "threshold" → Map(
                                "floor" → event.threshold.floor.map( v ⇒ f"${v}%2.5f" ).toString,
                                "expected" → event.threshold.expected.map( v ⇒ f"${v}%2.5f" ).toString,
                                "ceiling" → event.threshold.ceiling.map( v ⇒ f"${v}%2.5f" ).toString
                              )
                            )
                          ),
                          id = requestId
                        )

                        acc.copy(
                          shape = acc.shape.advance( event ),
                          advances = acc.advances :+ event
                        )
                      }
                      .getOrElse {
                        altLog.error(
                          Map(
                            "@msg" → "NOT ORIGINAL",
                            "point" → Map(
                              "datetime" → pt.dateTime.toString,
                              "timestamp" → pt.timestamp.toLong,
                              "value" → f"${pt.value}%2.5f"
                            )
                          ),
                          id = requestId
                        )

                        acc
                      }
                  }
                }

                loop( tail, newAcc )
              }
            }
          }

          def tryScoring( pt: PointT, shape: Shape ): ErrorOr[AnomalyScore] = {
            val attempt = Either catchNonFatal {
              algorithm
                .score( pt, shape )( analysisContext )
                .getOrElse { AnomalyScore( false, ThresholdBoundary empty pt.timestamp.toLong ) }
            }

            val anomaly = {
              for {
                _ ← checkSize( shape )
                score ← attempt
                checkedScore ← checkScore( score, shape )
              } yield checkedScore
            }

            anomaly recover {
              case ex: NonFatalSkipScoringException ⇒ {
                altLog.info(
                  Map(
                    "@msg" → "will not score point",
                    "message" → ex.getMessage,
                    "point" → Map(
                      "datetime" → pt.dateTime.toString,
                      "timestamp" → pt.timestamp.toLong,
                      "value" → f"${pt.value}%2.5f"
                    ),
                    "algorithm" → algorithm.label
                  ),
                  id = requestId
                )

                AnomalyScore( false, ThresholdBoundary empty pt.timestamp.toLong )
              }
            }
          }

          loop( points.toList, Accumulation( analysisContext.topic, workingShape ).asRight )
        }
      }

      private val recordAdvancements: KOp[Seq[Advanced], Seq[Advanced]] = {
        Kleisli[ErrorOr, Seq[Advanced], Seq[Advanced]] { advances ⇒
          persistAll( advances.to[scala.collection.immutable.Seq] ) { accept }
          advances.asRight
        }
      }

      private def makeOutliers( events: Seq[Advanced] ): KOp[Context, Outliers] = {
        Kleisli[ErrorOr, Context, Outliers] { ctx ⇒
          val outliers = events collect { case e if e.isOutlier ⇒ e.point }
          val thresholds = events map { _.threshold }

          Outliers.forSeries(
            algorithms = Set( algorithm.label ),
            plan = ctx.plan,
            source = ctx.source,
            outliers = outliers,
            thresholdBoundaries = Map( algorithm.label → thresholds )
          )
            .toEither
            .leftMap { _.head }
        }
      }

      // -- nonfunctional operations --
      var _snapshotStarted: Long = 0L
      def startSnapshotTimer(): Unit = { _snapshotStarted = System.currentTimeMillis() }
      def stopSnapshotTimer( success: Boolean ): Unit = {
        if ( 0 < _snapshotStarted ) {
          Algorithm.Metrics.snapshotTimer.update( System.currentTimeMillis() - _snapshotStarted, MILLISECONDS )
          _snapshotStarted = 0L
        }

        if ( success ) Algorithm.Metrics.snapshotSuccesses.mark() else Algorithm.Metrics.snapshotFailures.mark()
      }

      var _snapshotSweepStarted: Long = 0L
      def startSnapshotSweepTimer(): Unit = { _snapshotSweepStarted = System.currentTimeMillis() }
      def stopSnapshotSweepTimer( event: Any ): Unit = {
        if ( 0 < _snapshotSweepStarted ) {
          Algorithm.Metrics.snapshotSweepTimer.update( System.currentTimeMillis() - _snapshotSweepStarted, MILLISECONDS )
          _snapshotSweepStarted = 0L
        }

        event match {
          case _: DeleteSnapshotsSuccess ⇒ Algorithm.Metrics.snapshotSweepSuccesses.mark()
          case _: DeleteSnapshotsFailure ⇒ Algorithm.Metrics.snapshotSweepFailures.mark()
          case _ ⇒ {}
        }
      }

      var _journalSweepStarted: Long = 0L
      def startJournalSweepTimer(): Unit = { _journalSweepStarted = System.currentTimeMillis() }
      def stopJournalSweepTimer( event: Any ): Unit = {
        if ( 0 < _journalSweepStarted ) {
          Algorithm.Metrics.journalSweepTimer.update( System.currentTimeMillis() - _journalSweepStarted, MILLISECONDS )
          _journalSweepStarted = 0L
        }

        event match {
          case _: DeleteMessagesSuccess ⇒ Algorithm.Metrics.journalSweepSuccesses.mark()
          case _: DeleteMessagesFailure ⇒ Algorithm.Metrics.journalSweepFailures.mark()
          case _ ⇒ {}
        }
      }

      var _passivationStarted: Long = 0L
      def startPassivationTimer(): Unit = { _passivationStarted = System.currentTimeMillis() }
      def stopPassivationTimer( event: Any ): Unit = {
        if ( 0 < _passivationStarted ) {
          Algorithm.Metrics.passivationTimer.update( System.currentTimeMillis() - _passivationStarted, MILLISECONDS )
          _passivationStarted = 0L
        }

        event match {
          case e: demesne.PassivationSpecification.StopAggregateRoot[_] ⇒ {
            altLog.info(
              Map( "@msg" → "received repeated passivation message", "actor" → self.path.name, "event" → e.toString ),
              id = requestId
            )
          }

          case _ ⇒ {}
        }
      }

      override def saveSnapshot( snapshot: Any ): Unit = {
        startSnapshotTimer()
        super.saveSnapshot( snapshot )
      }

      def postSnapshot( snapshotSequenceNr: Long, success: Boolean ): Unit = {
        altLog.debug(
          Map( "@msg" → "postSnapshot", "actor" → self.path.name, "sequence-nr" → snapshotSequenceNr, "success" → success ),
          id = requestId
        )
        stopSnapshotTimer( success )

        startSnapshotSweepTimer()
        deleteSnapshots( SnapshotSelectionCriteria( maxSequenceNr = snapshotSequenceNr - 1 ) )

        startJournalSweepTimer()
        deleteMessages( snapshotSequenceNr )
      }

      override protected def onPersistFailure( cause: Throwable, event: Any, seqNr: Long ): Unit = {
        altLog.error(
          Map( "@msg" → "onPersistFailure", "actor" → self.path.name, "event" → event.toString, "sequence-nr" → seqNr ),
          cause,
          id = requestId
        )
      }

      override protected def onRecoveryFailure( cause: Throwable, event: Option[Any] ): Unit = {
        altLog.error(
          Map( "@msg" → "onRecoveryFailure", "actor" → self.path.name, "event" → event.toString ),
          cause,
          id = requestId
        )
        super.onRecoveryFailure( cause, event )
      }
    }
  }
}
