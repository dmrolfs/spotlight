package spotlight.analysis.shard

import scala.concurrent.duration._
import scala.reflect._
import akka.actor.{ ActorRef, Cancellable, Props }
import akka.event.LoggingReceive
import akka.persistence.{ RecoveryCompleted, SnapshotOffer }
import com.persist.logging._
import nl.grons.metrics.scala.{ Histogram, Meter, MetricName }
import omnibus.akka.envelope._
import omnibus.akka.metrics.{ Instrumented, InstrumentedActor }
import omnibus.akka.publish.{ EventPublisher, StackableStreamPublisher }
import omnibus.commons.identifier._
import omnibus.commons.util._
import demesne._
import demesne.module.{ LocalAggregate, SimpleAggregateModule }
import spotlight.analysis.DetectUsing
import spotlight.analysis.algorithm.{ Algorithm, AlgorithmProtocol ⇒ AP }
import spotlight.model.outlier.AnalysisPlan
import spotlight.model.timeseries._

/** Created by rolfsd on 12/21/16.
  */
object CellShardProtocol extends AggregateProtocol[CellShardCatalog#ID] {
  case class Add(
    override val targetId: Add#TID,
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    nrCells: Int,
    nextAlgorithmId: () ⇒ Algorithm.TID
  ) extends Command

  case class Added(
    override val sourceId: Added#TID,
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    nextAlgorithmId: () ⇒ Algorithm.TID,
    cells: Vector[AlgoTID]
  ) extends Event
}

case class CellShardCatalog(
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    override val nextAlgorithmId: () ⇒ Algorithm.TID,
    cells: Vector[AlgoTID]
) extends ShardCatalog with Equals with ClassLogging {
  override def id: TID = ShardCatalog.idFor( plan, algorithmRootType.name )( CellShardCatalog.identifying )
  //  override def id: TID = identifying.tag( ShardCatalog.ID( plan.id, algorithmRootType.name ).asInstanceOf[identifying.ID] ).asInstanceOf[TID]
  override def name: String = plan.name
  override def slug: String = plan.slug
  val size: Int = cells.size

  def apply( t: Topic ): AlgoTID = {
    log.debug(
      Map(
        "@msg" → "#TEST: CellShardCatalog.apply",
        "topic" → Map( "name" → t.toString, "hashcode" → t.## ),
        "cells" → cells.size,
        "cell-assignment" → ( math.abs( t.## ) % cells.size )
      )
    )
    cells( math.abs( t.## ) % size )
  }

  def contains( t: Topic ): Boolean = true

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[CellShardCatalog]

  override def hashCode: Int = 41 + id.##

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: CellShardCatalog ⇒ {
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

object CellShardCatalog {
  implicit val identifying: Identifying.Aux[CellShardCatalog, ShardCatalog.ID] = new ShardCatalog.ShardCatalogIdentifying[CellShardCatalog] {
    override val idTag: Symbol = Symbol( "cell-shard" )
  }
}

object CellShardModule extends ClassLogging {
  type ID = CellShardCatalog#ID
  type TID = CellShardCatalog#TID

  //  implicit val identifying: Identifying[CellShardCatalog] = CellShardCatalog.identifying

  val module: SimpleAggregateModule[CellShardCatalog, CellShardCatalog#ID] = {
    val b = SimpleAggregateModule.builderFor[CellShardCatalog, CellShardCatalog#ID].make
    import b.P.{ Props ⇒ BProps, _ }

    b
      .builder
      .set( Environment, LocalAggregate )
      .set( BProps, ShardingActor.props( _, _ ) )
      .build()
  }

  object ShardingActor {
    def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

    def name( rootType: AggregateRootType ): String = rootType.name + "ShardCatalog"

    private class Default( model: DomainModel, rootType: AggregateRootType )
      extends ShardingActor( model, rootType )
      with StackableStreamPublisher
  }

  class ShardingActor( override val model: DomainModel, override val rootType: AggregateRootType )
      extends demesne.AggregateRoot[CellShardCatalog, CellShardCatalog#ID]
      with InstrumentedActor
      with demesne.AggregateRoot.Provider {
    outer: EventPublisher ⇒

    import spotlight.analysis.shard.{ CellShardProtocol ⇒ P }

    override lazy val metricBaseName: MetricName = MetricName( classOf[ShardingActor] )

    var shardMetrics: Option[ShardMetrics] = None

    class ShardMetrics( plan: AnalysisPlan.Summary, id: ShardCatalog.ID ) extends Instrumented {
      val ShardBaseName = "cell-shard"

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

    // override def tidFromPersistenceId(pid: String): TID = {
    //   val delimPos = pid lastIndexOf ':'
    //   val (rootName, arep) = pid splitAt delimPos
    //   identifying.safeParseTid[TID](arep)(classTag[TID])
    // }

    override var state: CellShardCatalog = _
    //    override val evState: ClassTag[CellShardCatalog] = classTag[CellShardCatalog]

    override def acceptance: Acceptance = {
      case ( CellShardProtocol.Added( tid, p, rt, next, cells ), s ) if Option( s ).isEmpty ⇒ {
        if ( shardMetrics.isEmpty ) {
          shardMetrics = Some( new ShardMetrics( plan = p, id = tid.id.asInstanceOf[ShardCatalog.ID] ) )
        }

        CellShardCatalog( p, rt, next, cells )
      }
    }

    def referenceFor( aid: AlgoTID ): ActorRef = model( state.algorithmRootType, aid )

    def dispatchEstimateRequests(): Unit = {
      for {
        s ← Option( state ).toSet[CellShardCatalog]
        allShards = s.cells
        sid ← allShards
        ref = model( s.algorithmRootType, sid )
      } {
        ref !+ AP.EstimateSize( sid.asInstanceOf[AP.EstimateSize#TID] )
      }
    }

    case object UpdateAvailability extends CellShardProtocol.ProtocolMessage
    val AvailabilityCheckPeriod: FiniteDuration = 10.seconds

    val myReceiveRecover: Receive = {
      case RecoveryCompleted ⇒ {
        shardMetrics = Option( state ) map { s ⇒ new ShardMetrics( s.plan, s.id.id.asInstanceOf[ShardCatalog.ID] ) }

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

    override def receiveRecover: Receive = myReceiveRecover orElse super.receiveRecover

    override def receiveCommand: Receive = LoggingReceive { around( routing orElse admin ) }

    import spotlight.analysis.algorithm.{ AlgorithmProtocol ⇒ AP }

    val admin: Receive = {
      case e: CellShardProtocol.Add if e.targetId == aggregateId && Option( state ).isEmpty ⇒ {
        val cells = List.fill( e.nrCells ) { e.nextAlgorithmId() }
        persist( P.Added( e.targetId, e.plan, e.algorithmRootType, e.nextAlgorithmId, cells.toVector ) ) { acceptAndPublish }
      }

      case e: CellShardProtocol.Add if e.targetId == aggregateId && Option( state ).nonEmpty ⇒ {}

      case UpdateAvailability ⇒ dispatchEstimateRequests()

      case estimate: AP.EstimatedSize ⇒ {
        log.info(
          "AlgorithmCellShardCatalog[{}] Received estimate from shard:[{}] with estimated shapes:[{}] and average shape size:[{}]",
          self.path.name, estimate.sourceId, estimate.nrShapes, estimate.averageSizePerShape
        )

        shardMetrics foreach { m ⇒
          m.memoryHistogram += estimate.size.toBytes.toLong
          m.sizeHistogram += estimate.nrShapes
        }
      }
    }

    val routing: Receive = {
      case m: DetectUsing if Option( state ).isDefined && state.contains( m.topic ) ⇒ {
        val sid = state( m.topic )
        shardMetrics foreach { _.routingMeter.mark() }
        log.debug( "ShardCatalog[{}]: topic [{}] routed to algorithm shard:[{}]", self.path.name, m.topic, sid )
        referenceFor( sid ) forwardEnvelope m.copy( targetId = sid )
      }

      case ShardProtocol.RouteMessage( _, payload ) if routing isDefinedAt payload ⇒ {
        log.debug( "ShardCatalog[{}]: known RouteMessage received. extracting payload:[{}]", self.path.name, payload )
        routing( payload )
      }
    }
  }
}
