package spotlight.analysis.algorithm.shard

import scala.concurrent.duration._
import scala.reflect._
import akka.actor.{ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import akka.persistence.RecoveryCompleted

import nl.grons.metrics.scala.{Meter, MetricName}
import squants.information._
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import peds.akka.envelope._
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.commons.identifier._
import peds.commons.util._
import peds.commons.TryV
import demesne._
import demesne.repository.CommonLocalRepository
import spotlight.analysis.DetectUsing
import spotlight.analysis.algorithm.shard.ShardCatalog.ShardCatalogIdentifying
import spotlight.analysis.algorithm.{AlgorithmModule, AlgorithmProtocol => AP}
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 12/21/16.
  */
object LookupShardProtocol extends AggregateProtocol[LookupShardCatalog#ID] {
//  import LookupShardCatalog.Control

  case class Add(
    override val targetId: Add#TID,
    plan: OutlierPlan.Summary,
    algorithmRootType: AggregateRootType
//    control: Option[Control] = None
  ) extends Command

  case class Added(
    override val sourceId: Added#TID,
    plan: OutlierPlan.Summary,
    algorithmRootType: AggregateRootType
//    control: LookupShardCatalog.Control
  ) extends Event

//  case class RouteMessage( override val targetId: RouteMessage#TID, payload: Any ) extends Message

//  case class UseControl( override val targetId: UseControl#TID, control: Control ) extends AlgorithmShardCommand
//  case class ControlSet( override val sourceId: ControlSet#TID, control: Control ) extends AlgorithmShardEvent

  case class ShardAssigned(
    override val sourceId: ShardAssigned#TID,
    topic: Topic,
    algorithmId: AlgorithmModule.TID
  ) extends Event
}


//import scala.collection.mutable

case class LookupShardCatalog(
  plan: OutlierPlan.Summary,
  algorithmRootType: AggregateRootType,
  //  control: LookupShardCatalog.Control,
  shards: Object2ObjectOpenHashMap[Topic, AlgoTID] = new Object2ObjectOpenHashMap[Topic, AlgoTID]( ) // set expected size?
//    shards: mutable.Map[Topic, AlgoTID] = mutable.Map.empty[Topic, AlgoTID]
) extends ShardCatalog with Equals {
  import LookupShardCatalog.identifying

  override def id: TID = identifying.tag( ShardCatalog.ID( plan.id, algorithmRootType.name ).asInstanceOf[identifying.ID] ).asInstanceOf[TID]
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

  def tally: Map[AlgoTID, Int] = {
    import scala.collection.JavaConversions._

    val zero = Map( shards.values.toSet[AlgoTID].map{ s => ( s, 0 ) }.toSeq:_* )
    shards.foldLeft( zero ){ case (acc, (_, aid)) =>
      val newTally = acc( aid ) + 1
      acc + ( aid -> newTally )
    }
  }

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[LookupShardCatalog]

  override def hashCode: Int = 41 + id.##

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: LookupShardCatalog => {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.id == that.id )
        }
      }

      case _ => false
    }
  }

  override def toString(): String = getClass.safeSimpleName + s"( plan:${plan} algorithm:${algorithmRootType.name} )"
}

object LookupShardCatalog {
  implicit val identifying: Identifying[LookupShardCatalog] = new ShardCatalogIdentifying[LookupShardCatalog] {
    override val idTag: Symbol = Symbol( "lookup-shard" )
  }


  sealed trait Control {
    def withinThreshold( estimate: AP.EstimatedSize ): Boolean
  }

  object Control {
    final case class BySize( threshold: Information = Megabytes(10) ) extends Control {
      override def withinThreshold( estimate: AP.EstimatedSize ): Boolean = estimate.size <= threshold
    }
  }
}


object LookupShardModule extends AggregateRootModule { module =>
  override type ID = LookupShardCatalog#ID

  implicit val identifying: Identifying[LookupShardCatalog] = LookupShardCatalog.identifying
  override def nextId: TryV[TID] = identifying.nextIdAs[TID]


  override val rootType: AggregateRootType = AlgorithmShardCatalogRootType
  object AlgorithmShardCatalogRootType extends AggregateRootType {
    override val name: String = module.identifying.idTag.name
    override val identifying: Identifying[_] = module.identifying
    override val snapshotPeriod: Option[FiniteDuration] = None
    override def repositoryProps( implicit model: DomainModel ): Props = {
      CommonLocalRepository.props( model, this, AggregateRoot.ShardingActor.props( _: DomainModel, _: AggregateRootType ) )
    }
  }

  object AggregateRoot {
    object ShardingActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default(model, rootType) )

      def name( rootType: AggregateRootType ): String = rootType.name + "ShardCatalog"

      private class Default( model: DomainModel, rootType: AggregateRootType )
      extends ShardingActor( model, rootType )
      with StackableStreamPublisher


      case class Availability private[LookupShardModule](
        shardSizes: Map[AlgoTID, AP.EstimatedSize] = Map.empty[AlgoTID, AP.EstimatedSize],
        control: LookupShardCatalog.Control
      ) {
        def withNewShardId( shard: AlgoTID ): Availability = {
          val newShardSizes = shardSizes + ( shard -> AP.EstimatedSize(shard, 0, Bytes(0)) )
          copy( shardSizes = newShardSizes )
        }

        def withEstimate( estimate: AP.EstimatedSize ): Availability = {
          val newShardSizes = shardSizes + ( estimate.sourceId -> estimate )
          copy( shardSizes = newShardSizes )
        }

        val available: Set[AlgoTID] = shardSizes.collect{ case (aid, size) if control withinThreshold size => aid }.toSet

        val mostAvailable: Option[AlgoTID] = {
          available.toSeq.map{ aid => ( aid, shardSizes(aid) ) }.sortBy{ _._2 }.headOption.map{ _._1 }
        }

        @inline def isEmpty: Boolean = mostAvailable.isDefined
        @inline def nonEmpty: Boolean = !isEmpty

        val isCandidate: AlgoTID => Boolean = { aid: AlgoTID => available.contains( aid ) || !shardSizes.contains( aid ) }
      }
    }

    class ShardingActor( override val model: DomainModel, override val rootType: AggregateRootType )
    extends demesne.AggregateRoot[LookupShardCatalog, LookupShardCatalog#ID]
    with InstrumentedActor
    with demesne.AggregateRoot.Provider {
      outer: EventPublisher =>

      import ShardingActor.Availability
      import spotlight.analysis.algorithm.shard.{LookupShardProtocol => P}

//      override val journalPluginId: String = "akka.persistence.algorithm.journal.plugin"
//      override val snapshotPluginId: String = "akka.persistence.algorithm.snapshot.plugin"

      override lazy val metricBaseName: MetricName = MetricName( classOf[ShardingActor] )

      var shardMetrics: Option[ShardMetrics] = None

      class ShardMetrics( plan: OutlierPlan.Summary, id: ShardCatalog.ID ) extends Instrumented {
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

        val assignmentsMeter: Meter = metrics.meter( AssignmentsName )
        val routingMeter: Meter = metrics.meter( RoutingsName )

        initializeMetrics()

        def initializeMetrics(): Unit = {
          stripLingeringMetrics()
          metrics.unregisterGauges()

          metrics.gauge( CountName ) { availability.shardSizes.size }

          metrics.gauge( ActiveShardSizeGaugeName ) {
            availability.mostAvailable.map{ availability.shardSizes }.map{ _.size }.getOrElse{ Bytes( 0 ) }.toKilobytes
          }

          metrics.gauge( TotalShardSizeGaugeName ) {
            availability.shardSizes.values.foldLeft( Bytes(0) ){ _ + _.size }.toKilobytes
          }

          metrics.gauge( HitRateGaugeName ) { 100.0 * ( 1.0 - (routingMeter.count.toDouble / assignmentsMeter.count.toDouble) ) }
        }

        def stripLingeringMetrics(): Unit = {
          import com.codahale.metrics.{Metric, MetricFilter}

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

                log.warning( s"#TEST #ShardCatalog[{}]: stripLingeringMetrics removeMatching: name:[{}] TO-REMOVE:[{}] has-${ShardBaseName}:${name contains ShardBaseName} has-${CountName}:${name contains CountName} has-${ActiveShardSizeGaugeName}:${name contains ActiveShardSizeGaugeName} has-${TotalShardSizeGaugeName}:${name contains TotalShardSizeGaugeName}", self.path.name, name, isMatch )
                isMatch
              }
            }
          )
        }
      }


      var updateAvailability: Option[Cancellable] = None
      override def postStop(): Unit = updateAvailability foreach { _.cancel() }

      override def parseId( idrep: String ): TID = identifying.safeParseTid[TID]( idrep )( classTag[TID] )

      override var state: LookupShardCatalog = _
      override val evState: ClassTag[LookupShardCatalog] = classTag[LookupShardCatalog]
      var availability: Availability = _

      override def acceptance: Acceptance = {
        case ( LookupShardProtocol.Added(tid, p, rt ), s ) if Option( s ).isEmpty => {
          if ( shardMetrics.isEmpty ) {
            shardMetrics = Some( new ShardMetrics(plan = p, id = tid.id.asInstanceOf[ShardCatalog.ID] ) )
          }

          LookupShardCatalog( plan = p, algorithmRootType = rt )
        }

        case ( LookupShardProtocol.ShardAssigned(_, t, aid ), s ) => s.assignShard( t, aid )
//        case (P.ControlSet(_, c), s ) if c != s.control => s.copy( control = c )
      }

      def referenceFor( aid: AlgoTID ): ActorRef = model( state.algorithmRootType, aid )


      def dispatchEstimateRequests( forCandidates: AlgoTID => Boolean ): Unit = {
        import scala.collection.JavaConversions._

        for {
          s <- Option( state ).toSet[LookupShardCatalog]
          allShards = s.shards.values.toSet
          tally = s.tally
          candidates = allShards filter forCandidates
          cid <- candidates
          ref = referenceFor( cid )
        } {
          ref !+ AP.EstimateSize( cid.asInstanceOf[AP.EstimateSize#TID] )
        }
      }


      case object UpdateAvailability extends LookupShardProtocol.ProtocolMessage
      val AvailabilityCheckPeriod: FiniteDuration = 10.seconds

      override def receiveRecover: Receive = {
        case RecoveryCompleted => {

          shardMetrics = Option( state ) map { s => new ShardMetrics( s.plan, s.id.id.asInstanceOf[ShardCatalog.ID] ) }

          dispatchEstimateRequests( _ => true )

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

      override def receiveCommand: Receive = active( Availability(control = LookupShardCatalog.Control.BySize(Megabytes(3))))

      def active( newAvailability: Availability ): Receive = {
        availability = newAvailability
        log.info( "ShardCatalog[{}] updating availability to:[{}]", self.path.name, newAvailability )
        LoggingReceive { around( knownRouting orElse unknownRouting( newAvailability ) orElse admin ) }
      }

      val admin: Receive = {
        case LookupShardProtocol.Add( id, plan, algorithmRootType ) if id == aggregateId && Option( state ).isEmpty => {
          persist( P.Added(id, plan, algorithmRootType) ) { acceptAndPublish }
        }

        case LookupShardProtocol.Add( id, _, algorithmRootType ) if id == aggregateId && Option( state ).nonEmpty => { }

        case UpdateAvailability => dispatchEstimateRequests( availability.isCandidate )
      }

      val knownRouting: Receive = {
        case m: DetectUsing if Option(state).isDefined && state.contains( m.topic ) => {
          val sid = state( m.topic )
          shardMetrics foreach { _.routingMeter.mark() }
          log.debug( "ShardCatalog[{}]: topic [{}] routed to algorithm shard:[{}]", self.path.name, m.topic, sid )
          referenceFor( sid ) forwardEnvelope m.copy( targetId = sid )
        }

        case ShardProtocol.RouteMessage( _, payload ) if knownRouting isDefinedAt payload => {
          log.debug( "ShardCatalog[{}]: known RouteMessage received. extracting payload:[{}]", self.path.name, payload )
          knownRouting( payload )
        }
      }

      def unknownRouting( availability: Availability ): Receive = {
        case ShardProtocol.RouteMessage( _, payload ) if assignRouting isDefinedAt payload => {
          log.debug( "ShardCatalog[{}]: unknown RouteMessage received. extracting payload:[{}]", self.path.name, payload )
          assignRouting( payload )
        }

        case m: DetectUsing if assignRouting isDefinedAt m => assignRouting( m )

        case estimate: AP.EstimatedSize => {
          log.info(
            "LookupShardCatalog[{}] shard:[{}] shapes:[{}] estimated average shape size:[{}]",
            self.path.name, estimate.sourceId, estimate.nrShapes, estimate.averageSizePerShape
          )

          context become active( availability withEstimate estimate )
        }
      }

      val assignRouting: Receive = {
        case m: DetectUsing if Option(state).isDefined => {
          import scalaz.Scalaz.{ state => _, _ }

          availability
          .mostAvailable
          .map { _.right[Throwable] }
          .getOrElse {
            val nid = state.algorithmRootType.identifying.nextIdAs[AlgoTID]
            nid foreach { n => context become active( availability withNewShardId n ) }
            log.warning( "ShardCatalog[{}]: creating new shard id: NID[{}]  root-type:[{}] tag:[{}]", self.path.name, nid, state.algorithmRootType, state.algorithmRootType.identifying.idTag.name )
            nid
          }
          .foreach { aid =>
            log.debug( "ShardCatalog[{}]: unknown topic:[{}] routed to algorithm shard:[{}]", self.path.name, m.topic, aid )
            persistAsync( P.ShardAssigned(aggregateId, m.topic, aid) ) { e =>
//val e = P.ShardAssigned(aggregateId, m.topic, aid)
              accept( e )

              shardMetrics foreach { sm =>
                sm.assignmentsMeter.mark()
                sm.routingMeter.mark()
              }

              log.info( "ShardCatalog[{}]: topic [{}] assigned and routed to algorithm shard:[{}]", self.path.name, m.topic, e.algorithmId )
              //todo should message be reworked for shard's aid? since that's different than router and shard-catalog or should router use routeMessage and forward
              referenceFor( e.algorithmId ) forwardEnvelope m.copy( targetId = aid ) //okay to close over sender in persist handler
            }
          }
        }
      }
    }
  }
}
