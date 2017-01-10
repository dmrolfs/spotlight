package spotlight.analysis.outlier.algorithm

import scala.reflect._
import akka.actor.{ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import akka.persistence.RecoveryCompleted

import scalaz.{-\/, Validation, \/-}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{Meter, MetricName}
import squants.information.{Bytes, Information, Kilobytes, Megabytes}
import peds.akka.envelope._
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.archetype.domain.model.core.Entity
import peds.commons.{TryV, Valid}
import peds.commons.identifier._
import peds.commons.util._
import demesne._
import demesne.module.{LocalAggregate, SimpleAggregateModule}
import spotlight.analysis.outlier.DetectUsing
import spotlight.analysis.outlier.algorithm.AlgorithmShardCatalogModule.AlgoTID
import spotlight.analysis.outlier.algorithm.{AlgorithmProtocol => AP}
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 12/21/16.
  */
object AlgorithmShardProtocol extends AggregateProtocol[AlgorithmShardCatalog#ID] {
//  import AlgorithmShardCatalog.Control

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
//    control: AlgorithmShardCatalog.Control
  ) extends Event

  case class RouteMessage( override val targetId: RouteMessage#TID, payload: Any ) extends Message

//  case class UseControl( override val targetId: UseControl#TID, control: Control ) extends AlgorithmShardCommand
//  case class ControlSet( override val sourceId: ControlSet#TID, control: Control ) extends AlgorithmShardEvent

  case class ShardAssigned(
    override val sourceId: ShardAssigned#TID,
    topic: Topic,
    algorithmId: AlgorithmModule.TID
  ) extends Event
}


case class AlgorithmShardCatalog(
  plan: OutlierPlan.Summary,
  algorithmRootType: AggregateRootType,
  //  control: AlgorithmShardCatalog.Control,
  shards: Map[Topic, AlgoTID] = Map.empty[Topic, AlgoTID]
) extends Entity with Equals {
  import AlgorithmShardCatalog.identifying

  override type ID = identifying.ID
  override val evID: ClassTag[ID] = ClassTag( classOf[AlgorithmShardCatalog.ID] )
  override val evTID: ClassTag[TID] = classTag[TID]

  override def id: TID = identifying.tag( AlgorithmShardCatalog.ID(plan.id, algorithmRootType.name).asInstanceOf[identifying.ID] )
  override def name: String = plan.name
  override def slug: String = plan.slug

  def apply( t: Topic ): AlgoTID = shards( t )
  def contains( t: Topic ): Boolean = shards contains t

  def assignShard( t: Topic, shard: AlgoTID ): AlgorithmShardCatalog = {
    val newShards = shards + ( t -> shard )
    copy( shards = newShards )
  }

  def tally: Map[AlgoTID, Int] = {
    val zero = Map( shards.values.toSet[AlgoTID].map{ s => ( s, 0 ) }.toSeq:_* )
    shards.foldLeft( zero ){ case (acc, (_, aid)) =>
      val newTally = acc( aid ) + 1
      acc + ( aid -> newTally )
    }
  }

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[AlgorithmShardCatalog]

  override def hashCode: Int = 41 + id.##

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: AlgorithmShardCatalog => {
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

  override def toString(): String = getClass.safeSimpleName + s"( plan:${plan} )"
}

object AlgorithmShardCatalog {
  final case class ID( planId: OutlierPlan#ID, algorithmLabel: String ) {
    override def toString: String = planId + ":" + algorithmLabel
  }

  object ID {
    def fromString( rep: String ): Valid[ID] = {
      Validation
      .fromTryCatchNonFatal {
        val Array( pid, algo ) = rep.split( ':' )
        ID( planId = ShortUUID(pid), algorithmLabel = algo )
      }
      .toValidationNel
    }
  }

  implicit val identifying: Identifying[AlgorithmShardCatalog] = {
    new Identifying[AlgorithmShardCatalog] {
      override type ID = AlgorithmShardCatalog.ID
      override val evID: ClassTag[ID] = classTag[ID]
      override val evTID: ClassTag[TID] = classTag[TID]

      override def fromString( idrep: String ): ID = {
        AlgorithmShardCatalog.ID.fromString( idrep ).disjunction match {
          case \/-( id ) => id
          case -\/( exs ) => {
            exs foreach { ex => logger.error( s"failed to parse id string [${idrep}] into ${evID}", ex ) }
            throw exs.head
          }
        }
      }

      override def nextId: TryV[TID] = -\/( new IllegalStateException("AlgorithmShardCatalog does not support nextId") )

      override val idTag: Symbol = Symbol( "shard-catalog" )
      override def idOf( c: AlgorithmShardCatalog ): TID = c.id.asInstanceOf[TID]
    }
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


object AlgorithmShardCatalogModule extends Instrumented with LazyLogging {
  type ID = AlgorithmShardCatalog#ID
  type TID = AlgorithmShardCatalog#TID
  type AlgoTID = AlgorithmModule.TID


  def idFor( plan: OutlierPlan.Summary, algorithmLabel: String ): TID = {
    //todo: this casting bit wrt path dependent types is driving me nuts
    identifying.tag( AlgorithmShardCatalog.ID(plan.id, algorithmLabel).asInstanceOf[identifying.ID] ).asInstanceOf[TID]
  }

  override lazy val metricBaseName: MetricName = MetricName( getClass )

  implicit val identifying: Identifying[AlgorithmShardCatalog] = AlgorithmShardCatalog.identifying


  val module: SimpleAggregateModule[AlgorithmShardCatalog] = {
    val b = SimpleAggregateModule.builderFor[AlgorithmShardCatalog].make
    import b.P.{ Tag => BTag, Props => BProps, _ }

    b
    .builder
    .set( BTag, identifying.idTag )
    .set( Environment, LocalAggregate )
    .set( BProps, AggregateRoot.ShardCatalogActor.props(_, _) )
    .build()
  }


  object AggregateRoot {
    object ShardCatalogActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default(model, rootType) )

      def name( rootType: AggregateRootType ): String = rootType.name + "ShardCatalog"

      private class Default( model: DomainModel, rootType: AggregateRootType )
      extends ShardCatalogActor( model, rootType )
      with StackableStreamPublisher


      case class Availability private[AlgorithmShardCatalogModule](
        shardSizes: Map[AlgoTID, AP.EstimatedSize] = Map.empty[AlgoTID, AP.EstimatedSize],
        control: AlgorithmShardCatalog.Control
      ) {
        def withNewShardId( shard: AlgoTID ): Availability = {
          logger.warn( "#TEST #ShardCatalog #Availability: withNewShardId:[{}]", shard )
          val newShardSizes = shardSizes + ( shard -> AP.EstimatedSize(shard, 0, Bytes(0)) )
          copy( shardSizes = newShardSizes )
        }

        def withEstimate( estimate: AP.EstimatedSize ): Availability = {
          logger.warn( "#TEST #ShardCatalog #Availability: withEstimate:[{}]", estimate )
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

    class ShardCatalogActor( override val model: DomainModel, override val rootType: AggregateRootType )
    extends demesne.AggregateRoot[AlgorithmShardCatalog, AlgorithmShardCatalog#ID]
    with InstrumentedActor
    with demesne.AggregateRoot.Provider {
      outer: EventPublisher =>

      import ShardCatalogActor.Availability
      import spotlight.analysis.outlier.algorithm.{ AlgorithmShardProtocol => P }


      override lazy val metricBaseName: MetricName = MetricName( classOf[ShardCatalogActor] )

      var shardMetrics: Option[ShardMetrics] = None

      class ShardMetrics( plan: OutlierPlan.Summary, id: AlgorithmShardCatalog.ID ) extends Instrumented {
        val ShardBaseName = "shard"

        override lazy val metricBaseName: MetricName = {
          AlgorithmShardCatalogModule.metricBaseName.append( ShardBaseName, plan.name, id.algorithmLabel )
        }

        val CountName = "count"
        val ActiveShardSizeGaugeName = "active-size-mb"
        val TotalShardSizeGaugeName = "total-size-mb"
        val AssignmentsName = "assignments"
        val RoutingsName = "routings"

        val assignmentsMeter: Meter = metrics.meter( AssignmentsName )
        val routingMeter: Meter = metrics.meter( RoutingsName )

        initializeMetrics()

        def initializeMetrics(): Unit = {
          stripLingeringMetrics()

          metrics.gauge( CountName ) { availability.shardSizes.size }

          metrics.gauge( ActiveShardSizeGaugeName ) {
            availability.mostAvailable.map{ availability.shardSizes }.map{ _.size }.getOrElse{ Bytes( 0 ) }.toKilobytes
          }

          metrics.gauge( TotalShardSizeGaugeName ) {
            availability.shardSizes.values.foldLeft( Bytes(0) ){ _ + _.size }.toKilobytes
          }
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
                      name.contains( TotalShardSizeGaugeName )
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

      override var state: AlgorithmShardCatalog = _
      override val evState: ClassTag[AlgorithmShardCatalog] = classTag[AlgorithmShardCatalog]
      var availability: Availability = _

      override def acceptance: Acceptance = {
        case ( P.Added(tid, p, rt), s ) if Option(s).isEmpty => {
          if ( shardMetrics.isEmpty ) {
            shardMetrics = Some( new ShardMetrics(plan = p, id = tid.id.asInstanceOf[AlgorithmShardCatalog.ID] ) )
          }

          AlgorithmShardCatalog( plan = p, algorithmRootType = rt )
        }

        case ( P.ShardAssigned(_, t, aid), s ) => s.assignShard( t, aid )
//        case (P.ControlSet(_, c), s ) if c != s.control => s.copy( control = c )
      }

      def referenceFor( aid: AlgoTID ): ActorRef = model( state.algorithmRootType, aid )


      def dispatchEstimateRequests( forCandidates: AlgoTID => Boolean ): Unit = {
        log.warning( "#TEST ShardCatalog: dispatching availability:[{}]", availability )
        for {
          s <- Option( state ).toSet[AlgorithmShardCatalog]
          allShards = s.shards.values.toSet
          tally = s.tally
          _ = log.warning( "#TEST ShardCatalog: dispatching considering all-shards:[{}]", allShards.map{ s => (s, tally(s), forCandidates(s)) }.mkString(", ") )
          candidates = allShards filter forCandidates
          _ = log.warning( "#TEST ShardCatalog: dispatching availability estimate requests to [{}:{}] shards: [{}]", s.plan.name, s.algorithmRootType.name, candidates.mkString(", ") )
          cid <- candidates
          ref = model( s.algorithmRootType, cid )
        } {
          ref !+ AP.EstimateSize( cid.asInstanceOf[AP.EstimateSize#TID] )
        }
      }


      case object UpdateAvailability extends P.ProtocolMessage

      override def receiveRecover: Receive = {
        case RecoveryCompleted => {
          import scala.concurrent.duration._

          shardMetrics = Option( state ) map { s => new ShardMetrics( s.plan, s.id.id.asInstanceOf[AlgorithmShardCatalog.ID] ) }

          dispatchEstimateRequests( _ => true )

          updateAvailability = Option(
            context.system.scheduler.schedule(
              initialDelay = 1.minute,
              interval = 1.minute,
              receiver = self,
              message = UpdateAvailability
            )(
              context.system.dispatcher
            )
          )
        }
      }

      override def receiveCommand: Receive = active( Availability(control = AlgorithmShardCatalog.Control.BySize( Kilobytes(250) ) ) )

      def active( newAvailability: Availability ): Receive = {
        availability = newAvailability
        log.warning( "#TEST ShardCatalog[{}] updating availability to:[{}]", self.path.name, newAvailability )
        LoggingReceive { around( knownRouting orElse unknownRouting( newAvailability ) orElse admin ) }
      }

      def isAlgorithmKnown( algorithmRootType: AggregateRootType ): Boolean = {
        Option( state )
        .map { _.shards.values.toSet contains algorithmRootType }
        .getOrElse { false }
      }

      val admin: Receive = {
        case P.Add( id, _, algorithmRootType ) if id == aggregateId && isAlgorithmKnown(algorithmRootType) => { }
        case P.Add( id, plan, algorithmRootType) if id == aggregateId => {
          persist( P.Added(id, plan, algorithmRootType) ) { acceptAndPublish }
        }

        case UpdateAvailability => dispatchEstimateRequests( availability.isCandidate )
      }

      val knownRouting: Receive = {
        case m: DetectUsing if Option(state).isDefined && state.contains( m.topic ) => {
          val sid = state( m.topic )
          shardMetrics foreach { _.routingMeter.mark() }
          log.debug( "ShardCatalog[{}]: topic [{}] routed to algorithm shard:[{}]", self.path.name, m.topic, sid )
          referenceFor( sid ) forwardEnvelope m.copy( targetId = sid )
        }

        case P.RouteMessage( _, payload ) if knownRouting isDefinedAt payload => {
          log.debug( "ShardCatalog[{}]: known RouteMessage received. extracting payload:[{}]", self.path.name, payload )
          knownRouting( payload )
        }
      }

      def unknownRouting( availability: Availability ): Receive = {
        case P.RouteMessage( _, payload ) if assignRouting isDefinedAt payload => {
          log.debug( "ShardCatalog[{}]: unknown RouteMessage received. extracting payload:[{}]", self.path.name, payload )
          assignRouting( payload )
        }

        case m: DetectUsing if assignRouting isDefinedAt m => assignRouting( m )

        case estimate: AP.EstimatedSize => {
          if ( estimate.nrShapes != state.shards.size ) {
            log.warning(
              "ShardCatalog[{}] mistmatching number of topics between catalog:[{}] and algorithm-shard[{}]:[{}]",
              self.path.name,
              state.shards.size,
              estimate.sourceId,
              estimate.nrShapes
            )
          }
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
            persist( P.ShardAssigned(aggregateId, m.topic, aid) ) { e =>
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
