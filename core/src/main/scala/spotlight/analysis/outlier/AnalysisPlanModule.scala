package spotlight.analysis.outlier

import scala.reflect._
import akka.NotUsed
import akka.actor._
import akka.stream.Supervision.Decider
import akka.stream.{ActorAttributes, Materializer}
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import shapeless.Lens
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{EntityIdentifying, EntityLensProvider}
import peds.commons.identifier.ShortUUID
import demesne.module.entity.EntityAggregateModule
import demesne.module.entity.EntityAggregateModule.MakeIndexSpec
import demesne.module.entity.{messages => EntityMessages}
import demesne.AggregateMessage
import demesne.index.local.IndexLocalAgent
import demesne.{AggregateProtocol, AggregateRootType, DomainModel}
import demesne.index.{Directive, IndexBusSubscription, StackableIndexBusPublisher}
import demesne.module.LocalAggregate
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import spotlight.analysis.outlier.AnalysisPlanProtocol.{AnalysisFlow, MakeFlow}
import spotlight.analysis.outlier.OutlierDetection.{DetectionResult, DetectionTimedOut}
import spotlight.model.outlier.OutlierPlan.Scope
import spotlight.model.outlier._
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.model.timeseries._


object AnalysisPlanProtocol extends AggregateProtocol[OutlierPlan#ID]{
  sealed trait AnalysisPlanMessage
  sealed abstract class AnalysisPlanCommand extends AnalysisPlanMessage with CommandMessage
  sealed abstract class AnalysisPlanEvent extends AnalysisPlanMessage with EventMessage

  case class MakeFlow(
    override val targetId: AnalysisPlanModule.module.TID,
    parallelism: Int,
    system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ) extends AggregateMessage with AnalysisPlanMessage {
    override type ID = AnalysisPlanModule.module.ID
  }

  case class AnalysisFlow( flow: DetectFlow ) extends AnalysisPlanMessage


  case class AcceptTimeSeries(
    override val targetId: AcceptTimeSeries#TID,
    override val correlationIds: Set[WorkId],
    override val data: TimeSeries,
    override val scope: Option[OutlierPlan.Scope] = None
  ) extends AnalysisPlanCommand with CorrelatedSeries {
    override def withData( newData: TimeSeries ): CorrelatedData[TimeSeries] = this.copy( data = newData )
    override def withCorrelationIds( newIds: Set[WorkId] ): CorrelatedData[TimeSeries] = this.copy( correlationIds = newIds )
    override def withScope( newScope: Option[Scope] ): CorrelatedData[TimeSeries] = this.copy( scope = newScope )
  }

  //todo add info change commands
  //todo reify algorithm
  //      case class AddAlgorithm( override val targetId: OutlierPlan#TID, algorithm: Symbol ) extends Command with AnalysisPlanMessage
  case class ApplyTo( override val targetId: ApplyTo#TID, appliesTo: OutlierPlan.AppliesTo ) extends AnalysisPlanCommand

  case class UseAlgorithms(
    override val targetId: UseAlgorithms#TID,
    algorithms: Set[Symbol],
    algorithmConfig: Config
  ) extends AnalysisPlanCommand

  case class ResolveVia(
    override val targetId: ResolveVia#TID,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers
  ) extends AnalysisPlanCommand


  case class ScopeChanged( override val sourceId: ScopeChanged#TID, appliesTo: OutlierPlan.AppliesTo ) extends AnalysisPlanEvent

  case class AlgorithmsChanged(
    override val sourceId: AlgorithmsChanged#TID,
    algorithms: Set[Symbol],
    algorithmConfig: Config
  ) extends AnalysisPlanEvent

  case class AnalysisResolutionChanged(
    override val sourceId: AnalysisResolutionChanged#TID,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers
  ) extends AnalysisPlanEvent

  case class GetPlan( override val targetId: GetPlan#TID ) extends AnalysisPlanCommand
  case class PlanInfo( override val sourceId: PlanInfo#TID, info: OutlierPlan ) extends AnalysisPlanEvent {
    def toSummary: OutlierPlan.Summary = OutlierPlan.Summary( id = sourceId, name = info.name, appliesTo = info.appliesTo )
  }
}


/**
  * Created by rolfsd on 5/26/16.
  */
object AnalysisPlanModule extends EntityLensProvider[OutlierPlan] with Instrumented with LazyLogging {
  override lazy val metricBaseName: MetricName = MetricName( getClass )

  val droppedSeriesMeter: Meter = metrics.meter( "dropped", "series" )
  val droppedPointsMeter: Meter = metrics.meter( "dropped", "points" )


  implicit val identifying: EntityIdentifying[OutlierPlan] = {
    new EntityIdentifying[OutlierPlan] with ShortUUID.ShortUuidIdentifying[OutlierPlan] {
      override val evEntity: ClassTag[OutlierPlan] = classTag[OutlierPlan]
    }
  }


  override def idLens: Lens[OutlierPlan, OutlierPlan#TID] = OutlierPlan.idLens
  override def nameLens: Lens[OutlierPlan, String] = OutlierPlan.nameLens
  override def slugLens: Lens[OutlierPlan, String] = OutlierPlan.slugLens

  val namedPlanIndex: Symbol = 'NamedPlan

  val indexes: MakeIndexSpec = {
    () => {
      Seq(
        IndexLocalAgent.spec[String, module.TID, OutlierPlan.Summary]( specName = namedPlanIndex, IndexBusSubscription ) {
          case EntityMessages.Added( sid, Some( p: OutlierPlan ) ) => Directive.Record( p.name, sid, p.toSummary )
          case EntityMessages.Added( sid, info ) => {
            logger.error( "AnalysisPlanModule: IGNORING ADDED event since info was not Some OutlierPlan: [{}]", info )
            Directive.Ignore
          }
          case EntityMessages.Disabled( sid, _ ) => Directive.Withdraw( sid )
          case EntityMessages.Renamed( sid, oldName, newName ) => Directive.ReviseKey( oldName, newName )
          case m: EntityMessages.EntityMessage => Directive.Ignore
          case m: AnalysisPlanProtocol.AnalysisPlanMessage => Directive.Ignore
        }
      )
    }
  }

  val module: EntityAggregateModule[OutlierPlan] = {
    val b = EntityAggregateModule.builderFor[OutlierPlan].make
    import b.P.{ Tag => BTag, Props => BProps, _ }

    b
    .builder
    .set( BTag, identifying.idTag )
    .set( Environment, LocalAggregate )
    .set( BProps, AggregateRoot.PlanActor.props(_, _) )
    .set( Indexes, indexes )
    .set( IdLens, OutlierPlan.idLens )
    .set( NameLens, OutlierPlan.nameLens )
    .set( IsActiveLens, Some(OutlierPlan.isActiveLens) )
    .build()
  }


  object AggregateRoot {

    object PlanActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

      private class Default(model: DomainModel, rootType: AggregateRootType)
      extends PlanActor( model, rootType )
      with WorkerProvider
      with FlowConfigurationProvider
      with StackableStreamPublisher
      with StackableIndexBusPublisher {
        override val bufferSize: Int = 1000

        override protected def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
          log.error(
            "Rejected to persist event type [{}] with sequence number [{}] for persistenceId [{}] due to [{}].",
            event.getClass.getName, seqNr, persistenceId, cause
          )
          throw cause
        }
      }


      trait FlowConfigurationProvider {
        def bufferSize: Int
      }

      trait WorkerProvider {
        provider: Actor with ActorLogging =>
        def model: DomainModel

        def plan: OutlierPlan

        def makeRouter()( implicit context: ActorContext ): ActorRef = {
          val algorithmRefs = for {
            name <- plan.algorithms.toSeq
            rt <- DetectionAlgorithmRouter.rootTypeFor( name )( context.dispatcher ).toSeq
          } yield (name, DetectionAlgorithmRouter.RootTypeResolver( rt, model ))

          context.actorOf(
            DetectionAlgorithmRouter.props( Map( algorithmRefs: _* ) ).withDispatcher( DetectionAlgorithmRouter.DispatcherPath ),
            DetectionAlgorithmRouter.name( provider.plan.name )
          )
        }

        def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
          context.actorOf(
            OutlierDetection.props( routerRef ).withDispatcher( OutlierDetection.DispatcherPath ),
            OutlierDetection.name( plan.name )
          )
        }
      }

    }

    class PlanActor( override val model: DomainModel, override val rootType: AggregateRootType )
    extends module.EntityAggregateActor
    with InstrumentedActor
    with demesne.AggregateRoot.Provider {
      outer: PlanActor.FlowConfigurationProvider with PlanActor.WorkerProvider with EventPublisher =>

      import akka.stream.Supervision
      import spotlight.analysis.outlier.{AnalysisPlanProtocol => P}

      override lazy val metricBaseName: MetricName = MetricName( classOf[PlanActor] )
      val failuresMeter: Meter = metrics.meter( "failures" )

      override var state: OutlierPlan = _

      override def plan: OutlierPlan = state

      override val evState: ClassTag[OutlierPlan] = ClassTag( classOf[OutlierPlan] )

      lazy val (detector, router) = {
        val r = outer.makeRouter()
        (outer.makeDetector( r ), r)
      }


      override def acceptance: Acceptance = entityAcceptance orElse {
        case (e: P.ScopeChanged, s) => {
          //todo: cast for expediency. my ideal is to define a Lens in the OutlierPlan trait; minor solace is this module is in the same package
          s.asInstanceOf[OutlierPlan.SimpleOutlierPlan].copy( appliesTo = e.appliesTo )
        }

        case (e: P.AlgorithmsChanged, s) => {
          //todo: cast for expediency. my ideal is to define a Lens in the OutlierPlan trait; minor solace is this module is in the same package
          s.asInstanceOf[OutlierPlan.SimpleOutlierPlan].copy( algorithms = e.algorithms, algorithmConfig = e.algorithmConfig )
        }

        case (e: P.AnalysisResolutionChanged, s) => {
          //todo: cast for expediency. my ideal is to define a Lens in the OutlierPlan trait; minor solace is this module is in the same package
          s.asInstanceOf[OutlierPlan.SimpleOutlierPlan].copy( isQuorum = e.isQuorum, reduce = e.reduce )
        }
      }

      val IdType = identifying.evTID

      import demesne.module.entity.{messages => EntityMessages}

      override def quiescent: Receive = {
        case EntityMessages.Add( IdType( targetId ), info ) if targetId == aggregateId => {
          persist( EntityMessages.Added( targetId, info ) ) { e =>
            acceptAndPublish( e )
            log.debug( "AnalysisPlanModule[{}]: notifying [{}] of {}", state.name, sender().path.name, e )
            sender() !+ e // per akka docs: safe to use sender() here
          }
        }

        case EntityMessages.Add( targetId, info ) => {
          log.info(
            "AnalysisPlanModule caught Add message but targetId[{}] does not match aggregateId:[{}] => [{}][{}]",
            (targetId, targetId.id.getClass),
            (aggregateId, aggregateId.id.getClass),
            targetId == aggregateId,
            targetId.id == aggregateId.id
          )
        }
        case a => {
          log.info( "AnalysisPlanModule ignoring generic message: [{}]", a )
        }
      }

      override def active: Receive = workflow orElse planEntity orElse super.active

      val workflow: Receive = {
        case MakeFlow( _, parallelism, system, timeout, materializer ) => {
          sender() ! AnalysisFlow( makeFlow( parallelism )( system, timeout, materializer ) )
        }
      }

      val planEntity: Receive = {
        case _: P.GetPlan => sender() !+ P.PlanInfo( state.id, state )

        case P.ApplyTo( id, appliesTo ) => persist( P.ScopeChanged( id, appliesTo ) ) {acceptAndPublish}

        case P.UseAlgorithms( id, algorithms, config ) => {
          persist( P.AlgorithmsChanged( id, algorithms, config ) ) { e =>
            acceptAndPublish( e )
          }
        }

        case P.ResolveVia( id, isQuorum, reduce ) => {
          persist( P.AnalysisResolutionChanged( id, isQuorum, reduce ) ) {acceptAndPublish}
        }
      }

      override def unhandled( message: Any ): Unit = {
        val total = active
        log.error(
          "[{}] UNHANDLED: [{}] (workflow,planEntity,super):[{}] total:[{}]",
          aggregateId,
          message,
          ( workflow.isDefinedAt(message), planEntity.isDefinedAt(message), super.active.isDefinedAt(message) ),
          total.isDefinedAt( message )
        )
        super.unhandled( message )
      }


      def makeFlow(
        parallelism: Int
      )(
        implicit system: ActorSystem,
        timeout: Timeout,
        materializer: Materializer
      ): DetectFlow = {
        val entry = Flow[TimeSeries] filter { state.appliesTo }

        val withGrouping = state.grouping map { g => entry.via( batchSeries( g ) ) } getOrElse entry
        withGrouping
//        .buffer( outer.bufferSize, OverflowStrategy.backpressure )
        .via( detectionFlow( state, parallelism ) )
      }

      def batchSeries(
        grouping: OutlierPlan.Grouping
      )(
        implicit tsMerging: Merging[TimeSeries]
      ): Flow[TimeSeries, TimeSeries, NotUsed] = {
        log.debug( "batchSeries grouping = [{}]", grouping )
        Flow[TimeSeries]
        .groupedWithin( n = grouping.limit, d = grouping.window )
        .map {
          _
          .groupBy {_.topic}
          .map { case (_, tss) =>
            tss.tail.foldLeft( tss.head ) { case (acc, ts) => tsMerging.merge( acc, ts ) valueOr { exs => throw exs.head } }
          }
        }
        .mapConcat {identity}
      }

      def detectionFlow(p: OutlierPlan, parallelism: Int)(implicit system: ActorSystem, timeout: Timeout): DetectFlow = {
        Flow[TimeSeries]
        .map { ts => log.debug( "AnalysisPlanModule:FLOW-DETECT: before-filter: [{}]", ts.toString ); ts }
        .map { ts => OutlierDetectionMessage( ts, p ).disjunction }
        .collect { case scalaz.\/-( m ) => m }
        .map { m => log.debug( "AnalysisPlanModule:FLOW-DETECT: on-to-detection-grid: [{}]", m.toString ); m }
        .mapAsync( parallelism ) { m =>( detector ?+ m ) }.withAttributes( ActorAttributes supervisionStrategy detectorDecider )
        .filter {
          case Envelope( DetectionResult( outliers, _ ), _ ) => true
          case DetectionResult( outliers, _ ) => true
          case Envelope( DetectionTimedOut(s, _), _ ) => {
            droppedSeriesMeter.mark()
            droppedPointsMeter.mark( s.points.size )
            false
          }
          case DetectionTimedOut( s, _ ) => {
            droppedSeriesMeter.mark()
            droppedPointsMeter.mark( s.points.size )
            false
          }
        }
        .collect {
          case Envelope( DetectionResult( outliers, _ ), _ ) => outliers
          case DetectionResult( outliers, _ ) => outliers
        }
      }

      val detectorDecider: Decider = new Decider {
        override def apply( ex: Throwable ): Supervision.Directive = {
          log.error( ex, "error in detection dropping series" )
          droppedSeriesMeter.mark()
          Supervision.Resume
        }
      }

      import SupervisorStrategy.{ Stop, Resume }

      override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(){
        case _: ActorInitializationException => Stop
        case _: ActorKilledException => Stop
        case _: DeathPactException => Stop
        case ex: Exception => {
          log.error( ex, "Error during detection calculations. Resuming calculations under [{}]", self.path.name )
          Resume
        }
      }
    }
  }
}
