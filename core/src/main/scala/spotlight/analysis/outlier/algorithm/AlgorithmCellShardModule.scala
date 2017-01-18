package spotlight.analysis.outlier.algorithm

import scala.reflect._
import scala.concurrent.duration._
import akka.actor.{ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import akka.persistence.RecoveryCompleted

import scalaz.{-\/, Validation, \/-}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{Histogram, Meter, MetricName}
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
import spotlight.analysis.outlier.algorithm.AlgorithmCellShardModule.AlgoTID
import spotlight.analysis.outlier.algorithm.{AlgorithmProtocol => AP}
import spotlight.model.outlier.OutlierPlan
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 12/21/16.
  */
object AlgorithmCellShardProtocol extends AggregateProtocol[AlgorithmCellShardCatalog#ID] {
  case class Add(
    override val targetId: Add#TID,
    plan: OutlierPlan.Summary,
    algorithmRootType: AggregateRootType,
    nrCells: Int
  ) extends Command

  case class Added(
    override val sourceId: Added#TID,
    plan: OutlierPlan.Summary,
    algorithmRootType: AggregateRootType,
    cells: Vector[AlgoTID]
  ) extends Event

//  case class CellAdded( override val sourceId: CellAdded#TID, cellId: AlgorithmModule.TID ) extends Event

  case class RouteMessage( override val targetId: RouteMessage#TID, payload: Any ) extends Message
}


case class AlgorithmCellShardCatalog(
  plan: OutlierPlan.Summary,
  algorithmRootType: AggregateRootType,
  cells: Vector[AlgoTID]
) extends Entity with Equals with LazyLogging {
  import AlgorithmCellShardCatalog.identifying

  override type ID = identifying.ID
  override val evID: ClassTag[ID] = ClassTag( classOf[AlgorithmCellShardCatalog.ID] )
  override val evTID: ClassTag[TID] = classTag[TID]

  override def id: TID = identifying.tag( AlgorithmCellShardCatalog.ID( plan.id, algorithmRootType.name ).asInstanceOf[identifying.ID] )
  override def name: String = plan.name
  override def slug: String = plan.slug

  def apply( t: Topic ): AlgoTID = {
    logger.debug( "#TEST: AlgorithmCellShardCatalog.apply: topic:[{}] cells:[{}] t.##:[{}] tpos:[{}]", t, cells.size.toString, t.##.toString, (math.abs(t.##) % cells.size).toString )
    cells( math.abs(t.##) % cells.size )
  }

  def contains( t: Topic ): Boolean = true

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[AlgorithmCellShardCatalog]

  override def hashCode: Int = 41 + id.##

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: AlgorithmCellShardCatalog => {
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

object AlgorithmCellShardCatalog {
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

  implicit val identifying: Identifying[AlgorithmCellShardCatalog] = {
    new Identifying[AlgorithmCellShardCatalog] {
      override type ID = AlgorithmCellShardCatalog.ID
      override val evID: ClassTag[ID] = classTag[ID]
      override val evTID: ClassTag[TID] = classTag[TID]

      override def fromString( idrep: String ): ID = {
        AlgorithmCellShardCatalog.ID.fromString( idrep ).disjunction match {
          case \/-( id ) => id
          case -\/( exs ) => {
            exs foreach { ex => logger.error( s"failed to parse id string [${idrep}] into ${evID}", ex ) }
            throw exs.head
          }
        }
      }

      override def nextId: TryV[TID] = -\/( new IllegalStateException("AlgorithmCellShardCatalog does not support nextId") )

      override val idTag: Symbol = Symbol( "shard-cell-catalog" )
      override def idOf( c: AlgorithmCellShardCatalog): TID = c.id.asInstanceOf[TID]
    }
  }
}


object AlgorithmCellShardModule extends LazyLogging {
  type ID = AlgorithmCellShardCatalog#ID
  type TID = AlgorithmCellShardCatalog#TID
  type AlgoTID = AlgorithmModule.TID


  def idFor( plan: OutlierPlan.Summary, algorithmLabel: String ): TID = {
    //todo: this casting bit wrt path dependent types is driving me nuts
    identifying.tag( AlgorithmCellShardCatalog.ID( plan.id, algorithmLabel ).asInstanceOf[identifying.ID] ).asInstanceOf[TID]
  }

  implicit val identifying: Identifying[AlgorithmCellShardCatalog] = AlgorithmCellShardCatalog.identifying


  val module: SimpleAggregateModule[AlgorithmCellShardCatalog] = {
    val b = SimpleAggregateModule.builderFor[AlgorithmCellShardCatalog].make
    import b.P.{ Tag => BTag, Props => BProps, _ }

    b
    .builder
    .set( BTag, identifying.idTag )
    .set( Environment, LocalAggregate )
    .set( BProps, AlgorithmShardingActor.props(_, _) )
    .build()
  }


  object AlgorithmShardingActor {
    def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default(model, rootType) )

    def name( rootType: AggregateRootType ): String = rootType.name + "ShardCatalog"

    private class Default( model: DomainModel, rootType: AggregateRootType )
    extends AlgorithmShardingActor( model, rootType )
    with StackableStreamPublisher
  }

  class AlgorithmShardingActor( override val model: DomainModel, override val rootType: AggregateRootType )
  extends demesne.AggregateRoot[AlgorithmCellShardCatalog, AlgorithmCellShardCatalog#ID]
  with InstrumentedActor
  with demesne.AggregateRoot.Provider {
    outer: EventPublisher =>

    import spotlight.analysis.outlier.algorithm.{ AlgorithmCellShardProtocol => P }

    override lazy val metricBaseName: MetricName = MetricName( classOf[AlgorithmShardingActor] )

    var shardMetrics: Option[ShardMetrics] = None

    class ShardMetrics( plan: OutlierPlan.Summary, id: AlgorithmCellShardCatalog.ID ) extends Instrumented {
      val ShardBaseName = "cell-shard"

      override lazy val metricBaseName: MetricName = {
        MetricName(
          spotlight.BaseMetricName,
          spotlight.analysis.outlier.BaseMetricName,
          "algorithm",
          plan.name,
          id.algorithmLabel,
          ShardBaseName
        )
      }

      val MemoryHistogramName = "memory"
      val SizeHistogramName = "size"
      val RoutingsName = "routings"

      val routingMeter: Meter = metrics.meter( RoutingsName )
      val memoryHistogram: Histogram = metrics.histogram( MemoryHistogramName ) //todo make biased to recent 5 mins
      val sizeHistogram: Histogram = metrics.histogram( SizeHistogramName ) //todo make biased to recent 5 mins

      initializeMetrics()

      def initializeMetrics(): Unit = {
        stripLingeringMetrics()
        metrics.unregisterGauges()
      }

      def stripLingeringMetrics(): Unit = {
        import com.codahale.metrics.{ Metric, MetricFilter }

        metrics.registry.removeMatching(
          new MetricFilter {
            override def matches( name: String, metric: Metric ): Boolean = {
              val isMatch = {
                false && name.contains( ShardBaseName ) &&
                (
                  false
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

    override def parseId( idrep: String ): TID = identifying.safeParseTid[TID]( idrep )( classTag[TID] )

    override var state: AlgorithmCellShardCatalog= _
    override val evState: ClassTag[AlgorithmCellShardCatalog] = classTag[AlgorithmCellShardCatalog]

    override def acceptance: Acceptance = {
      case ( P.Added(tid, p, rt, cells), s ) if Option(s).isEmpty => {
        if ( shardMetrics.isEmpty ) {
          shardMetrics = Some( new ShardMetrics(plan = p, id = tid.id.asInstanceOf[AlgorithmCellShardCatalog.ID] ) )
        }

        AlgorithmCellShardCatalog( p, rt, cells )
      }

//      case ( P.CellAdded(tid, cellId), s ) => s.copy( cells = s.cells :+ cellId )
    }

    def referenceFor( aid: AlgoTID ): ActorRef = model( state.algorithmRootType, aid )


    def dispatchEstimateRequests(): Unit = {
      for {
        s <- Option( state ).toSet[AlgorithmCellShardCatalog]
        allShards = s.cells
        sid <- allShards
        ref = model( s.algorithmRootType, sid )
      } {
        ref !+ AP.EstimateSize( sid.asInstanceOf[AP.EstimateSize#TID] )
      }
    }


    case object UpdateAvailability extends P.ProtocolMessage
    val AvailabilityCheckPeriod: FiniteDuration = 10.seconds

    override def receiveRecover: Receive = {
      case RecoveryCompleted => {
        import scala.concurrent.duration._

        shardMetrics = Option( state ) map { s => new ShardMetrics( s.plan, s.id.id.asInstanceOf[AlgorithmCellShardCatalog.ID] ) }

        dispatchEstimateRequests()

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

    override def receiveCommand: Receive = LoggingReceive { around( routing orElse admin ) }

    import spotlight.analysis.outlier.algorithm.{ AlgorithmProtocol => AP }

    val admin: Receive = {
      case P.Add( id, plan, algorithmRootType, nrCells) if id == aggregateId && Option(state).isEmpty => {
        import scalaz._, Scalaz.{ id => _, _ }

        List.fill( nrCells ){ algorithmRootType.identifying.nextId map { _.asInstanceOf[AlgoTID] } }.sequenceU match {
          case \/-( cells ) => persist( P.Added(id, plan, algorithmRootType, cells.toVector) ) { acceptAndPublish }
          case -\/( ex ) => {
            log.error( ex, "failed to gereate ids for algorithm:[{}]", algorithmRootType.name )
          }
        }
      }

      case e: P.Add if e.targetId == aggregateId && Option(state).nonEmpty => { }

      case UpdateAvailability => dispatchEstimateRequests()
    }

    val routing: Receive = {
      case m: DetectUsing if Option(state).isDefined && state.contains( m.topic ) => {
        val sid = state( m.topic )
        shardMetrics foreach { _.routingMeter.mark() }
        log.debug( "ShardCatalog[{}]: topic [{}] routed to algorithm shard:[{}]", self.path.name, m.topic, sid )
        referenceFor( sid ) forwardEnvelope m.copy( targetId = sid )
      }

      case P.RouteMessage( _, payload ) if routing isDefinedAt payload => {
        log.debug( "ShardCatalog[{}]: known RouteMessage received. extracting payload:[{}]", self.path.name, payload )
        routing( payload )
      }

      case estimate: AP.EstimatedSize => {
        log.info(
          "AlgorithmTopicShardCatalog[{}] shard:[{}] shapes:[{}] estimated average shape size:[{}]",
          self.path.name, estimate.sourceId, estimate.nrShapes, estimate.averageSizePerShape
        )

        shardMetrics foreach { m =>
          m.memoryHistogram += estimate.size.toBytes.toLong
          m.sizeHistogram += estimate.nrShapes
        }
      }

    }
  }
}
