package spotlight.analysis.shard

import scala.concurrent.duration._
import akka.actor.{ ActorRef, Cancellable, Props }
import akka.cluster.sharding.ClusterShardingSettings
import akka.event.LoggingReceive
import akka.persistence.RecoveryCompleted
import nl.grons.metrics.scala.{ Meter, MetricName }
import squants.information._
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import omnibus.akka.envelope._
import omnibus.akka.metrics.{ Instrumented, InstrumentedActor }
import omnibus.akka.publish.{ EventPublisher, StackableStreamPublisher }
import omnibus.commons.identifier._
import omnibus.commons.util._
import demesne._
import demesne.repository.CommonClusteredRepository
import spotlight.analysis.DetectUsing
import spotlight.analysis.shard.ShardCatalog.ShardCatalogIdentifying
import spotlight.analysis.algorithm.{ Algorithm, AlgorithmIdGenerator, AlgorithmProtocol ⇒ AP }
import spotlight.infrastructure.ClusterRole
import spotlight.model.outlier.AnalysisPlan
import spotlight.model.timeseries._

/** Created by rolfsd on 12/21/16.
  */
object LookupShardProtocol extends AggregateProtocol[LookupShardCatalog#ID] {
  //  import LookupShardCatalog.Control

  case class Add(
    override val targetId: Add#TID,
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    expectedNrTopics: Int,
    controlBySize: Information,
    idGenerator: AlgorithmIdGenerator
  //    control: Option[Control] = None
  ) extends Command

  case class Added(
    override val sourceId: Added#TID,
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    expectedNrTopics: Int,
    controlBySize: Information,
    idGenerator: AlgorithmIdGenerator
  //    control: LookupShardCatalog.Control
  ) extends Event

  //  case class UseControl( override val targetId: UseControl#TID, control: Control ) extends AlgorithmShardCommand
  //  case class ControlSet( override val sourceId: ControlSet#TID, control: Control ) extends AlgorithmShardEvent

  case class ShardAssigned(
    override val sourceId: ShardAssigned#TID,
    topic: Topic,
    algorithmId: Algorithm.TID
  ) extends Event
}

case class LookupShardCatalog(
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    control: LookupShardCatalog.Control,
    override val idGenerator: AlgorithmIdGenerator,
    shards: Object2ObjectOpenHashMap[Topic, AlgoTID]
) extends ShardCatalog with Equals {
  import LookupShardCatalog.identifying

  override def id: TID = ShardCatalog.idFor[LookupShardCatalog]( plan, algorithmRootType.name )
  //  override def id: TID = identifying.tag( ShardCatalog.ID( plan.id, algorithmRootType.name ).asInstanceOf[identifying.ID] ).asInstanceOf[TID]
  override def name: String = plan.name
  override def slug: String = plan.slug

  def apply( t: Topic ): AlgoTID = shards get t
  def contains( t: Topic ): Boolean = shards containsKey t

  def assignShard( t: Topic, shard: AlgoTID ): LookupShardCatalog = {
    //    val newShards = shards + ( t -> shard )
    //    copy( shards = newShards )
    //    shards += ( t -> shard )
    shards.put( t, shard )
    this
  }

  //  def tally: Map[AlgoTID, Int] = {
  //    import scala.collection.JavaConversions._
  //
  //    val zero = Map( shards.values.toSet[AlgoTID].map{ s => ( s, 0 ) }.toSeq:_* )
  //    shards.foldLeft( zero ){ case (acc, (_, aid)) =>
  //      val newTally = acc( aid ) + 1
  //      acc + ( aid -> newTally )
  //    }
  //  }

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[LookupShardCatalog]

  override def hashCode: Int = 41 + id.##

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: LookupShardCatalog ⇒ {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.id == that.id )
        }
      }

      case _ ⇒ false
    }
  }

  override def toString(): String = getClass.safeSimpleName + s"( plan:${plan} algorithm:${algorithmRootType.name} )"
}

object LookupShardCatalog {
  def makeShardMap( size: Int ): Object2ObjectOpenHashMap[Topic, AlgoTID] = new Object2ObjectOpenHashMap[Topic, AlgoTID]( size )

  implicit val identifying: Identifying.Aux[LookupShardCatalog, ShardCatalog.ID] = new ShardCatalogIdentifying[LookupShardCatalog] {
    override val idTag: Symbol = Symbol( "lookup-shard" )
  }

  sealed trait Control {
    def withinThreshold( estimate: AP.EstimatedSize ): Boolean
  }

  object Control {
    final case class BySize( threshold: Information ) extends Control {
      override def withinThreshold( estimate: AP.EstimatedSize ): Boolean = estimate.size <= threshold
    }
  }
}

object LookupShardModule extends AggregateRootModule[LookupShardCatalog, LookupShardCatalog#ID] { module ⇒
  override val rootType: AggregateRootType = AlgorithmShardCatalogRootType
  object AlgorithmShardCatalogRootType extends AggregateRootType {
    override val name: String = module.identifying.idTag.name
    override type S = LookupShardCatalog
    override val identifying: Identifying[S] = module.identifying

    override val clusterRole: Option[String] = Option( ClusterRole.Analysis.entryName )

    override val snapshotPeriod: Option[FiniteDuration] = None
    override def repositoryProps( implicit model: DomainModel ): Props = {
      CommonClusteredRepository.props(
        model = model,
        rootType = this,
        makeAggregateProps = AggregateRoot.ShardingActor.props( _: DomainModel, _: AggregateRootType )
      )(
          settings = ClusterShardingSettings( model.system )
        )
      //      CommonLocalRepository.props( model, this, AggregateRoot.ShardingActor.props( _: DomainModel, _: AggregateRootType ) )
    }
  }

  object AggregateRoot {
    object ShardingActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

      def name( rootType: AggregateRootType ): String = rootType.name + "ShardCatalog"

      private class Default( model: DomainModel, rootType: AggregateRootType )
        extends ShardingActor( model, rootType )
        with StackableStreamPublisher

      case class Availability private[LookupShardModule] (
          shardSizes: Map[AlgoTID, AP.EstimatedSize] = Map.empty[AlgoTID, AP.EstimatedSize],
          control: LookupShardCatalog.Control
      ) {
        def withNewShardId( shard: AlgoTID ): Availability = {
          val newShardSizes = shardSizes + ( shard → AP.EstimatedSize( shard, 0, Bytes( 0 ) ) )
          copy( shardSizes = newShardSizes )
        }

        def withEstimate( estimate: AP.EstimatedSize ): Availability = {
          val newShardSizes = shardSizes + ( estimate.sourceId → estimate )
          copy( shardSizes = newShardSizes )
        }

        val available: Set[AlgoTID] = shardSizes.collect { case ( aid, size ) if control withinThreshold size ⇒ aid }.toSet

        val mostAvailable: Option[AlgoTID] = {
          available.toSeq.map { aid ⇒ ( aid, shardSizes( aid ) ) }.sortBy { _._2 }.headOption.map { _._1 }
        }

        @inline def isEmpty: Boolean = mostAvailable.isDefined
        @inline def nonEmpty: Boolean = !isEmpty

        val isCandidate: AlgoTID ⇒ Boolean = { aid: AlgoTID ⇒ available.contains( aid ) || !shardSizes.contains( aid ) }

        override def toString(): String = {
          val shardStatuses = shardSizes map {
            case ( aid, e ) ⇒
              s"(open:${control.withinThreshold( e )} shapes:${e.nrShapes} size:${e.size})"
          }
          s"""Availability( shards[${shardSizes.size}]: control:${control} ${shardStatuses.mkString( "[", ", ", "]" )}"""
        }
      }
    }

    class ShardingActor( override val model: DomainModel, override val rootType: AggregateRootType )
        extends demesne.AggregateRoot[LookupShardCatalog, LookupShardCatalog#ID]
        with InstrumentedActor
        with demesne.AggregateRoot.Provider {
      outer: EventPublisher ⇒

      import ShardingActor.Availability
      import spotlight.analysis.shard.{ LookupShardProtocol ⇒ P }

      override def saveSnapshot( snapshot: Any ): Unit = { log.error( "[{}] should not snaphot!!", self.path ) }

      override lazy val metricBaseName: MetricName = MetricName( classOf[ShardingActor] )

      var shardMetrics: Option[ShardMetrics] = None

      class ShardMetrics( plan: AnalysisPlan.Summary, id: ShardCatalog.ID ) extends Instrumented {
        val ShardBaseName = "lookup-shard"

        override lazy val metricBaseName: MetricName = {
          MetricName(
            spotlight.BaseMetricName,
            spotlight.analysis.BaseMetricName,
            "algorithm",
            plan.name,
            id.algorithmLabel,
            ShardBaseName
          )
        }

        val CountName = "count"
        val ActiveShardSizeGaugeName = "active-size-mb"
        val TotalShardSizeGaugeName = "total-size-mb"
        val AssignmentsName = "assignments"
        val RoutingsName = "routings"
        val HitRateGaugeName = "hit-percentage"
        //        val LookupName = "lookup"

        val assignmentsMeter: Meter = metrics.meter( AssignmentsName )
        val routingMeter: Meter = metrics.meter( RoutingsName )
        //        val lookupTimer: Timer = metrics.timer( LookupName )

        initializeMetrics()

        def initializeMetrics(): Unit = {
          stripLingeringMetrics()
          metrics.unregisterGauges()

          metrics.gauge( CountName ) { availability.shardSizes.size }

          metrics.gauge( ActiveShardSizeGaugeName ) {
            availability.mostAvailable.map { availability.shardSizes }.map { _.size }.getOrElse { Bytes( 0 ) }.toKilobytes
          }

          metrics.gauge( TotalShardSizeGaugeName ) {
            availability.shardSizes.values.foldLeft( Bytes( 0 ) ) { _ + _.size }.toKilobytes
          }

          metrics.gauge( HitRateGaugeName ) { 100.0 * ( 1.0 - ( assignmentsMeter.count.toDouble / routingMeter.count.toDouble ) ) }
        }

        def stripLingeringMetrics(): Unit = {
          import com.codahale.metrics.{ Metric, MetricFilter }

          metrics.registry.removeMatching(
            new MetricFilter {
              override def matches( name: String, metric: Metric ): Boolean = {
                val isMatch = {
                  name.contains( ShardBaseName ) &&
                    (
                      name.contains( CountName ) ||
                      name.contains( ActiveShardSizeGaugeName ) ||
                      name.contains( TotalShardSizeGaugeName ) ||
                      name.contains( HitRateGaugeName )
                    )
                }

                isMatch
              }
            }
          )
        }
      }

      var updateAvailability: Option[Cancellable] = None
      override def postStop(): Unit = updateAvailability foreach { _.cancel() }

      //      override def parseId(idrep: String): TID = identifying.safeParseTid[TID](idrep)(classTag[TID])

      //      import ShardingActor.Lookup
      //      implicit val alloc = scala.offheap.malloc
      //      val shards: Lookup = Lookup.apply()

      override var state: LookupShardCatalog = _
      //      override val evState: ClassTag[LookupShardCatalog] = classTag[LookupShardCatalog]
      var availability: Availability = _

      override def acceptance: Acceptance = {
        case ( LookupShardProtocol.Added( tid, p, rt, nrTopics, bySize, nextAlgorithmId ), s ) if Option( s ).isEmpty ⇒ {
          if ( shardMetrics.isEmpty ) {
            shardMetrics = Some( new ShardMetrics( plan = p, id = tid.id.asInstanceOf[ShardCatalog.ID] ) )
          }

          val newState = LookupShardCatalog(
            plan = p,
            algorithmRootType = rt,
            control = LookupShardCatalog.Control.BySize( bySize ),
            idGenerator = AlgorithmIdGenerator( p.name, p.id, rt ),
            shards = LookupShardCatalog.makeShardMap( nrTopics )
          )

          context become LoggingReceive { around( active( Availability( control = newState.control ) ) ) }

          newState
        }

        case ( LookupShardProtocol.ShardAssigned( _, t, aid ), s ) ⇒ s.assignShard( t, aid )
        //        case (P.ControlSet(_, c), s ) if c != s.control => s.copy( control = c )
      }

      def referenceFor( aid: AlgoTID ): ActorRef = model( state.algorithmRootType, aid )

      def dispatchEstimateRequests( forCandidates: AlgoTID ⇒ Boolean ): Unit = {
        import scala.collection.JavaConverters._

        for {
          s ← Option( state ).toSet[LookupShardCatalog]
          allShards = s.shards.values.asScala.toSet
          candidates = allShards filter forCandidates
          cid ← candidates
          ref = referenceFor( cid )
        } {
          ref !+ AP.EstimateSize( cid.asInstanceOf[AP.EstimateSize#TID] )
        }
      }

      case object UpdateAvailability extends LookupShardProtocol.ProtocolMessage
      val AvailabilityCheckPeriod: FiniteDuration = 10.seconds

      val myReceiveRecover: Receive = {
        case RecoveryCompleted ⇒ {
          shardMetrics = Option( state ) map { s ⇒ new ShardMetrics( s.plan, s.id.id.asInstanceOf[ShardCatalog.ID] ) }

          dispatchEstimateRequests( _ ⇒ true )

          updateAvailability = Option(
            context.system.scheduler.schedule(
              initialDelay = AvailabilityCheckPeriod,
              interval = AvailabilityCheckPeriod,
              receiver = self,
              message = UpdateAvailability
            )(
              context.system.dispatcher
            )
          )
        }
      }

      override def receiveRecover: Receive = myReceiveRecover orElse super.receiveRecover

      override def receiveCommand: Receive = active( Availability( control = LookupShardCatalog.Control.BySize( Megabytes( 3 ) ) ) )

      def active( newAvailability: Availability ): Receive = {
        availability = newAvailability
        log.debug( "ShardCatalog[{}] updating availability to:[{}]", self.path.name, newAvailability )
        LoggingReceive { around( knownRouting orElse unknownRouting( newAvailability ) orElse admin ) }
      }

      val admin: Receive = {
        case LookupShardProtocol.Add( id, p, art, nrTopics, bySize, next ) if id == aggregateId && Option( state ).isEmpty ⇒ {
          persist( P.Added( id, p, art, nrTopics, bySize, next ) ) { acceptAndPublish }
        }

        case e: LookupShardProtocol.Add if e.targetId == aggregateId && Option( state ).nonEmpty ⇒ {}

        case UpdateAvailability ⇒ dispatchEstimateRequests( availability.isCandidate )
      }

      val knownRouting: Receive = {
        case m: DetectUsing if Option( state ).isDefined && state.contains( m.topic ) ⇒ {
          val sid = state( m.topic )
          shardMetrics foreach { _.routingMeter.mark() }
          log.debug( "ShardCatalog[{}]: topic [{}] routed to algorithm shard:[{}]", self.path.name, m.topic, sid )
          referenceFor( sid ) forwardEnvelope m.copy( targetId = sid )
        }

        case ShardProtocol.RouteMessage( _, payload ) if knownRouting isDefinedAt payload ⇒ {
          log.debug( "ShardCatalog[{}]: known RouteMessage received. extracting payload:[{}]", self.path.name, payload )
          knownRouting( payload )
        }
      }

      def unknownRouting( availability: Availability ): Receive = {
        case ShardProtocol.RouteMessage( _, payload ) if assignRouting isDefinedAt payload ⇒ assignRouting( payload )

        case m: DetectUsing if assignRouting isDefinedAt m ⇒ assignRouting( m )

        case estimate: AP.EstimatedSize ⇒ {
          log.info(
            "LookupShardCatalog[{}] shard:[{}] shapes:[{}] estimated average shape size:[{}]",
            self.path.name, estimate.sourceId, estimate.nrShapes, estimate.averageSizePerShape
          )

          context become active( availability withEstimate estimate )
        }
      }

      val assignRouting: Receive = {
        case m: DetectUsing if Option( state ).isDefined ⇒ {
          import scalaz.Scalaz.{ state ⇒ _, _ }

          val aid = {
            availability
              .mostAvailable
              .getOrElse {
                val nid = state.idGenerator.next()
                context become active( availability withNewShardId nid )
                log.info(
                  "ShardCatalog[{}]: creating new shard id: NID[{}]  root-type:[{}] tag:[{}]",
                  self.path.name,
                  nid,
                  state.algorithmRootType,
                  identifying.idTag.name
                )
                nid
              }
          }

          log.debug( "ShardCatalog[{}]: unknown topic:[{}] routed to algorithm shard:[{}]", self.path.name, m.topic, aid )
          persistAsync( P.ShardAssigned( aggregateId, m.topic, aid ) ) { e ⇒
            //val e = P.ShardAssigned(aggregateId, m.topic, aid)
            accept( e )

            shardMetrics foreach { sm ⇒
              sm.assignmentsMeter.mark()
              sm.routingMeter.mark()
            }

            log.debug( "ShardCatalog[{}]: topic [{}] assigned and routed to algorithm shard:[{}]", self.path.name, m.topic, e.algorithmId )
            //todo should message be reworked for shard's aid? since that's different than router and shard-catalog or should router use routeMessage and forward
            referenceFor( e.algorithmId ) forwardEnvelope m.copy( targetId = aid ) //okay to close over sender in persist handler
          }
        }
      }
    }
  }
}
