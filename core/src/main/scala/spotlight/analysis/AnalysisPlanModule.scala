package spotlight.analysis

import scala.reflect._
import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}
import akka.NotUsed
import akka.actor._
import akka.actor.SupervisorStrategy.{Resume, Stop}
import akka.event.LoggingReceive
import akka.stream.Supervision.Decider
import akka.stream.{ActorAttributes, Materializer}
import akka.stream.scaladsl.Flow
import akka.stream.Supervision
import akka.util.Timeout
import com.persist.logging.{ ActorLogging => PersistActorLogging, _ }
import shapeless.{Lens, lens}
import com.typesafe.config.Config
import nl.grons.metrics.scala.{Meter, MetricName}
import peds.akka.envelope._
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityIdentifying, EntityLensProvider}
import peds.commons.identifier.ShortUUID
import peds.akka.supervision.{IsolatedDefaultSupervisor, OneForOneStrategyFactory}
import demesne._
import demesne.index.local.IndexLocalAgent
import demesne.index.{Directive, IndexBusSubscription, StackableIndexBusPublisher}
import demesne.module.LocalAggregate
import demesne.module.entity.{EntityAggregateModule, EntityProtocol}
import demesne.module.entity.EntityAggregateModule.MakeIndexSpec
import spotlight.model.outlier._
import spotlight.model.outlier.AnalysisPlan.Scope
import spotlight.model.timeseries._
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.analysis.AnalysisPlanProtocol.{AnalysisFlow, MakeFlow}
import spotlight.analysis.OutlierDetection.{DetectionResult, DetectionTimedOut}
import spotlight.analysis.{AnalysisPlanProtocol => P}


case class AnalysisPlanState( plan: AnalysisPlan, routes: Map[Symbol, AlgorithmRoute] ) extends Entity {
  override type ID = AnalysisPlan#ID
  override type TID = peds.commons.identifier.TaggedID[ID]
  override def id: TID = plan.id
  override val evID: ClassTag[ID] = classTag[ID]
  override val evTID: ClassTag[TID] = classTag[TID]
  override def name: String = plan.name
  override def canEqual( that: Any ): Boolean = that.isInstanceOf[AnalysisPlanState]
}


object AnalysisPlanProtocol extends EntityProtocol[AnalysisPlanState#ID] {
  case class MakeFlow(
    override val targetId: MakeFlow#TID,
    parallelism: Int,
    system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ) extends Message

  case class AnalysisFlow( flow: DetectFlow ) extends ProtocolMessage


  case class AcceptTimeSeries(
    override val targetId: AcceptTimeSeries#TID,
    override val correlationIds: Set[WorkId],
    override val data: TimeSeries,
    override val scope: Option[AnalysisPlan.Scope] = None
  ) extends Command with CorrelatedSeries {
    override def withData( newData: TimeSeries ): CorrelatedData[TimeSeries] = this.copy( data = newData )
    override def withCorrelationIds( newIds: Set[WorkId] ): CorrelatedData[TimeSeries] = this.copy( correlationIds = newIds )
    override def withScope( newScope: Option[Scope] ): CorrelatedData[TimeSeries] = this.copy( scope = newScope )
  }

  //todo add info change commands
  //todo reify algorithm
  //      case class AddAlgorithm( override val targetId: AnalysisPlan#TID, algorithm: Symbol ) extends Command with AnalysisPlanMessage
  case class ApplyTo( override val targetId: ApplyTo#TID, appliesTo: AnalysisPlan.AppliesTo ) extends Command

  case class UseAlgorithms(
    override val targetId: UseAlgorithms#TID,
    algorithms: Set[Symbol],
    algorithmConfig: Config
  ) extends Command

  case class ResolveVia(
    override val targetId: ResolveVia#TID,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers
  ) extends Command


  case class ScopeChanged( override val sourceId: ScopeChanged#TID, appliesTo: AnalysisPlan.AppliesTo ) extends Event

  case class AlgorithmsChanged(
    override val sourceId: AlgorithmsChanged#TID,
    algorithms: Set[Symbol],
    algorithmConfig: Config,
    added: Set[Symbol],
    dropped: Set[Symbol]
  ) extends Event

  case class AnalysisResolutionChanged(
    override val sourceId: AnalysisResolutionChanged#TID,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers
  ) extends Event

  case class GetPlan( override val targetId: GetPlan#TID ) extends Command
  case class PlanInfo( override val sourceId: PlanInfo#TID, info: AnalysisPlan ) extends Event {
    def toSummary: AnalysisPlan.Summary = {
      AnalysisPlan.Summary( id = sourceId, name = info.name, slug = info.slug, appliesTo = Option(info.appliesTo) )
    }
  }
}


/**
  * Created by rolfsd on 5/26/16.
  */
object AnalysisPlanModule extends EntityLensProvider[AnalysisPlanState] with Instrumented with ClassLogging { moduleOuter =>
  override lazy val metricBaseName: MetricName = {
    MetricName( spotlight.BaseMetricName, spotlight.analysis.BaseMetricName )
  }

  val droppedSeriesMeter: Meter = metrics.meter( "dropped", "series" )
  val droppedPointsMeter: Meter = metrics.meter( "dropped", "points" )

  val InletBaseMetricName = "inlet"
  val inletSeries: Meter = metrics.meter( InletBaseMetricName, "series" )
  val inletPoints: Meter = metrics.meter( InletBaseMetricName, "points" )

  val OutletResultsBaseMetricName = "outlet.results"
  val outletResults: Meter = metrics.meter( OutletResultsBaseMetricName )
  val outletResultsAnomalies: Meter = metrics.meter( OutletResultsBaseMetricName, "anomalies" )
  val outletResultsConformities: Meter = metrics.meter( OutletResultsBaseMetricName, "conformities" )
  val outletResultsPoints: Meter = metrics.meter( OutletResultsBaseMetricName, "points" )
  val outletResultsPointsAnomalies: Meter = metrics.meter( OutletResultsBaseMetricName, "points.anomalies" )
  val outletResultsPointsConformities: Meter = metrics.meter( OutletResultsBaseMetricName, "points.conformities" )



  implicit val identifying: EntityIdentifying[AnalysisPlanState] = {
    new EntityIdentifying[AnalysisPlanState] with ShortUUID.ShortUuidIdentifying[AnalysisPlanState] {
      override val evEntity: ClassTag[AnalysisPlanState] = classTag[AnalysisPlanState]
    }
  }

  val planLens: Lens[AnalysisPlanState, AnalysisPlan] = lens[AnalysisPlanState] >> 'plan
  override def idLens: Lens[AnalysisPlanState, AnalysisPlanState#TID] = AnalysisPlan.idLens compose planLens
  override def nameLens: Lens[AnalysisPlanState, String] = AnalysisPlan.nameLens compose planLens
  override def slugLens: Lens[AnalysisPlanState, String] = AnalysisPlan.slugLens compose planLens
  val isActiveLens: Lens[AnalysisPlanState, Boolean] = AnalysisPlan.isActiveLens compose planLens

  val namedPlanIndex: Symbol = 'NamedPlan

  val indexes: MakeIndexSpec = {
    () => {
      Seq(
        IndexLocalAgent.spec[String, module.TID, AnalysisPlan.Summary]( specName = namedPlanIndex, IndexBusSubscription ) {
          case P.Added( sid, Some(AnalysisPlanState(p: AnalysisPlan, _)) ) => Directive.Record( p, sid, p.toSummary )
          case P.Added( sid, Some( p: AnalysisPlan ) ) => Directive.Record( p.name, sid, p.toSummary )
          case P.Added( sid, info ) => {
//            logger.error( "ignoring added event since info was not an AnalysisPlan: [{}]", info )
            log.error( Map("@msg" -> "ignoring added event since info was not some AnalysisPlan", "info" -> info.toString) )
            Directive.Ignore
          }
          case P.Disabled( sid, _ ) => Directive.Withdraw( sid )
          case P.Renamed( sid, oldName, newName ) => Directive.ReviseKey( oldName, newName )
          case _: P.ProtocolMessage => Directive.Ignore
        }
      )
    }
  }

  val module: EntityAggregateModule[AnalysisPlanState] = {
    val b = EntityAggregateModule.builderFor[AnalysisPlanState, AnalysisPlanProtocol.type].make
    import b.P.{ Tag => BTag, Props => BProps, _ }

    b
    .builder
    .set( BTag, identifying.idTag )
    .set( Environment, LocalAggregate )
    .set( BProps, AggregateRoot.PlanActor.props(_, _) )
    .set( PassivateTimeout, 5.minutes )
    .set( Protocol, AnalysisPlanProtocol )
    .set( Indexes, indexes )
    .set( IdLens, moduleOuter.idLens )
    .set( NameLens, moduleOuter.nameLens )
    .set( IsActiveLens, Some(moduleOuter.isActiveLens) )
    .build()
  }


  object AggregateRoot {

    object PlanActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

      private class Default( model: DomainModel, rootType: AggregateRootType )
      extends PlanActor( model, rootType )
      with WorkerProvider
      with FlowConfigurationProvider
      with StackableStreamPublisher
      with StackableIndexBusPublisher {
        override val bufferSize: Int = 1000

        override protected def onPersistRejected( cause: Throwable, event: Any, seqNr: Long ): Unit = {
          log.error(
            cause,
            "persist rejected for event:[{}] persistenceId:[{}] sequenceNr:[{}]",
            event.getClass.getName, persistenceId, seqNr
          )
//          altLog.error(
//            msg = Map(
//              "@msg" -> "persist event rejected",
//              "event-type" -> event.getClass.getName,
//              "persistence-id" -> persistenceId,
//              "sequence-nr" -> seqNr
//            ),
//            ex = cause
//          )
          throw cause
        }
      }


      trait FlowConfigurationProvider {
        def bufferSize: Int
      }

      trait WorkerProvider {
        provider: Actor with ActorLogging =>
        def model: DomainModel

        def plan: AnalysisPlan
        def routes: Map[Symbol, AlgorithmRoute]

        def routerName: String = DetectionAlgorithmRouter.name( provider.plan.name )
        def detectorName: String = OutlierDetection.name( provider.plan.name )

        def startWorkers(): (ActorRef, ActorRef, ActorRef) = {
          import peds.akka.supervision.IsolatedLifeCycleSupervisor.{ WaitForStart, GetChildren, Children, Started }
          import akka.pattern.ask

          implicit val ec: scala.concurrent.ExecutionContext = context.dispatcher
          implicit val timeout: Timeout = Timeout( 1.second )

          val foreman = context.system.actorOf(
            Props(
              new IsolatedDefaultSupervisor() with OneForOneStrategyFactory {
                override def childStarter() = {
                  val router = makeRouter()
                  val detector = makeDetector( router )
                }
              }
            ),
            s"${plan.name}-foreman"
          )
          log.info( "[{}] created plan foreman:[{}]", self.path, foreman.path )
//          altLog.info( Map("@msg" -> "created plan foreman", "self" -> self.path.toString, "foreman" -> foreman.path.toString) )

          val f = {
            for {
              _ <- ( foreman ? WaitForStart ).mapTo[Started.type]
              cs <- ( foreman ? GetChildren ).mapTo[Children]
            } yield {
              val actors = {
                for {
                  router <- cs.children collectFirst { case c if c.name contains provider.routerName => c.child }
                  detector <- cs.children collectFirst { case c if c.name contains provider.detectorName => c.child }
                } yield ( foreman, detector, router )
              }

              actors.get
            }
          }

          Await.result( f, 1.second )
        }

        def makeRouter()( implicit context: ActorContext ): ActorRef = {
//          val algorithmRefs = for {
//            name <- plan.algorithms.toSeq
//            rt <- DetectionAlgorithmRouter.rootTypeFor( name )( context.dispatcher ).toSeq
//          } yield {
//            ( name, AlgorithmRoute.routeFor(plan, rt)(model) )
//          }
//
//          val routerProps = DetectionAlgorithmRouter.props( plan, Map( algorithmRefs:_* ) )
          val routerProps = DetectionAlgorithmRouter.props( plan, routes )
          context.actorOf( routerProps.withDispatcher( DetectionAlgorithmRouter.DispatcherPath ), provider.routerName )
        }

        def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
          context.actorOf(
            OutlierDetection.props( routerRef ).withDispatcher( OutlierDetection.DispatcherPath ),
            provider.detectorName
          )
        }
      }
    }

    //todo convert to perist logging w scala 2.12
    class PlanActor( override val model: DomainModel, override val rootType: AggregateRootType )
    extends module.EntityAggregateActor
//    with AltActorLogging
    with InstrumentedActor
    with demesne.AggregateRoot.Provider {
      actorOuter: PlanActor.FlowConfigurationProvider with PlanActor.WorkerProvider with EventPublisher =>

      override lazy val metricBaseName: MetricName = MetricName( classOf[PlanActor] )
      val failuresMeter: Meter = metrics.meter( "failures" )

      override var state: AnalysisPlanState = _
      override val evState: ClassTag[AnalysisPlanState] = classTag[AnalysisPlanState]
      override def plan: AnalysisPlan = state.plan
      override def routes: Map[Symbol, AlgorithmRoute] = state.routes


      var detector: ActorRef = _

      override def acceptance: Acceptance = myAcceptance orElse entityAcceptance

      val myAcceptance: Acceptance = {
        case (P.Added(_, info), s) => {
          preActivate()
          context become LoggingReceive { around( active ) }
          info match {
            case Some( ps: AnalysisPlanState ) => ps
            case Some( p: AnalysisPlan ) => AnalysisPlanState( plan = p, routes = makeRoutes(p)() )
            case i => log.error( "ignoring Added command with unrecognized info:[{}]", info );  s
//            case i => altLog.error( Map("@msg" -> "ignoring Added command with unrecognized info", "info" -> i.toString) ); s
          }
        }

        case (e: P.ScopeChanged, s) => {
          //todo: cast for expediency. my ideal is to define a Lens in the AnalysisPlan trait; minor solace is this module is in the same package
          s.copy( plan = s.plan.asInstanceOf[AnalysisPlan.SimpleAnalysisPlan].copy(appliesTo = e.appliesTo) )
        }

        case (e: P.AlgorithmsChanged, s) => {
          implicit val ec: scala.concurrent.ExecutionContext = context.dispatcher
          val addedRoutes = makeRoutes( plan )( e.added )

          //todo: cast for expediency. my ideal is to define a Lens in the AnalysisPlan trait; minor solace is this module is in the same package
          s.copy(
            plan = s.plan.asInstanceOf[AnalysisPlan.SimpleAnalysisPlan].copy(
              algorithms = e.algorithms,
              algorithmConfig = e.algorithmConfig
            ),
            routes = s.routes -- e.dropped ++ addedRoutes
          )
        }

        case (e: P.AnalysisResolutionChanged, s) => {
          //todo: cast for expediency. my ideal is to define a Lens in the AnalysisPlan trait; minor solace is this module is in the same package
          s.copy( plan = s.plan.asInstanceOf[AnalysisPlan.SimpleAnalysisPlan].copy(isQuorum = e.isQuorum, reduce = e.reduce) )
        }
      }

      private def makeRoutes(
        plan: AnalysisPlan
      )(
        algorithms: Set[Symbol] = allAlgorithms(plan.algorithms, plan.algorithmConfig)
      ): Map[Symbol, AlgorithmRoute] = {
        implicit val ec: scala.concurrent.ExecutionContext = context.dispatcher

        def makeRoute( plan: AnalysisPlan )( algorithm: Symbol ): Option[AlgorithmRoute] = {
          DetectionAlgorithmRouter.Registry.rootTypeFor( algorithm ).map{ rt => AlgorithmRoute.routeFor( plan, rt )( model ) }
        }

        val routes = algorithms.toSeq.map{ a => (a, makeRoute(plan)(a) ) }.collect{ case (a, Some(r)) => ( a, r ) }
        Map( routes:_* )
      }

      val IdType = identifying.evTID

      override def quiescent: Receive = {
        case P.Add( IdType(targetId), info ) if targetId == aggregateId => {
          persist( P.Added( targetId, info ) ) { e =>
            acceptAndPublish( e )
            val (_, d, _) = startWorkers()
            actorOuter.detector = d
            sender() !+ e // per akka docs: safe to use sender() here
          }
        }

        case P.Add( targetId, info ) => {
          log.error(
            "ignoring received Add message with unrecognized " +
            "targetId:[{}] targetId-class:[{}] " +
            "aggregateId:[{}] aggregateId-class:[{}]",
            targetId, targetId.id.getClass.getName,
            aggregateId, aggregateId.getClass.getName
          )
//          altLog.error(
//            Map(
//              "@msg" -> "ignoring received Add message with unrecognized targetId",
//              "targetId" -> targetId.toString,
//              "targetId-class" -> targetId.id.getClass.getName,
//              "aggregateId" -> aggregateId.toString,
//              "aggregateId-class" -> aggregateId.getClass.getName
//            )
//          )
        }

        case m => log.error( "ignoring unrecognized message[{}]", m )
//        case m => altLog.error( Map("@msg" -> "ignoring unrecognized message", "message" -> m.toString) )
      }

      override def active: Receive = workflow orElse planEntity orElse super.active

      val workflow: Receive = {
        case MakeFlow( _, parallelism, system, timeout, materializer ) => {
          sender() ! AnalysisFlow( makeFlow( parallelism )( system, timeout, materializer ) )
        }
      }

      val planEntity: Receive = {
        case _: P.GetPlan => sender() !+ P.PlanInfo( state.id, state.plan )

        case P.ApplyTo( id, appliesTo ) => persist( P.ScopeChanged( id, appliesTo ) ) { acceptAndPublish }

        case P.UseAlgorithms( id, algorithms, config ) => persist( changeAlgorithms(algorithms, config) ) { acceptAndPublish }

        case P.ResolveVia( id, isQuorum, reduce ) => {
          persist( P.AnalysisResolutionChanged( id, isQuorum, reduce ) ) { acceptAndPublish }
        }
      }

      def changeAlgorithms( algorithms: Set[Symbol], algorithmSpec: Config ): P.AlgorithmsChanged = {
        val newAlgorithms = allAlgorithms( algorithms, algorithmSpec )
        val myRoutes = state.routes
        val dropped = myRoutes.keySet -- newAlgorithms
        val added = newAlgorithms -- myRoutes.keySet
        P.AlgorithmsChanged(
          sourceId = aggregateId,
          algorithms = newAlgorithms,
          algorithmConfig = algorithmSpec,
          added = added,
          dropped = dropped
        )
      }

      private def allAlgorithms( algorithms: Set[Symbol], algorithmSpec: Config ): Set[Symbol] = {
        import scala.collection.immutable
        import scala.collection.JavaConversions._
        val inSpec = algorithmSpec.root.entrySet.to[immutable.Set] map { e => Symbol(e.getKey) }
        algorithms ++ inSpec
      }

      override def unhandled( message: Any ): Unit = {
//        altLog.error( Map("@msg" -> "unhandled message", "aggregateId" -> aggregateId.toString, "message" -> message.toString) )
        log.error( "[{}] unhandled message:[{}]", aggregateId, message )
        super.unhandled( message )
      }


      def makeFlow(
        parallelism: Int
      )(
        implicit system: ActorSystem,
        timeout: Timeout,
        materializer: Materializer
      ): DetectFlow = {
        val entry = Flow[TimeSeries] filter { state.plan.appliesTo }

        val withGrouping = state.plan.grouping map { g => entry.via( batchSeries( g ) ) } getOrElse entry
        withGrouping
//        .buffer( actorOuter.bufferSize, OverflowStrategy.backpressure )
        .via( detectionFlow( state.plan, parallelism ) )
      }

      def batchSeries(
        grouping: AnalysisPlan.Grouping
      )(
        implicit tsMerging: Merging[TimeSeries]
      ): Flow[TimeSeries, TimeSeries, NotUsed] = {
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

      def detectionFlow( p: AnalysisPlan, parallelism: Int )( implicit system: ActorSystem, timeout: Timeout ): DetectFlow = {
        import peds.akka.envelope.pattern.ask

        implicit val ec: scala.concurrent.ExecutionContext = system.dispatcher

        Flow[TimeSeries]
        .map { ts => OutlierDetectionMessage( ts, p ).disjunction }
        .collect { case scalaz.\/-( m ) => m }
        .map { m =>
          inletSeries.mark()
          inletPoints.mark( m.source.points.size )
          m
        }
        .mapAsyncUnordered( parallelism ) { m =>
          ( detector ?+ m )
          .recover {
            case ex: TimeoutException => {
              log.error(
                ex,
                "timeout[{}] exceeded waiting for detection for plan:[{}] topic:[{}]",
                timeout.duration.toCoarsest, m.plan, m.topic
              )
//              altLog.error(
//                Map(
//                  "@msg" -> "timeout exceeded waiting for detection",
//                  "timeout" -> timeout.duration.toCoarsest.toString,
//                  "plan" -> m.plan.name,
//                  "topic" -> m.topic.toString
//                ),
//                ex
//              )

              DetectionTimedOut( m.source, m.plan )
            }
          }
        }.withAttributes( ActorAttributes supervisionStrategy detectorDecider )
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
        .map {
          case m => {
            val nrPoints = m.source.size
            val nrAnomalyPoints = m.anomalySize
            outletResults.mark()
            outletResultsPoints.mark( nrPoints )
            if ( m.hasAnomalies ) outletResultsAnomalies.mark() else outletResultsConformities.mark()
            outletResultsPointsAnomalies.mark( nrAnomalyPoints )
            outletResultsPointsConformities.mark( nrPoints - nrAnomalyPoints )
            m
          }
        }
      }

      val detectorDecider: Decider = new Decider {
        override def apply( ex: Throwable ): Supervision.Directive = {
          log.error( ex, "error in detection dropping series")
          droppedSeriesMeter.mark()
          Supervision.Resume
        }
      }

      override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(){
        case _: ActorInitializationException => Stop
        case _: ActorKilledException => Stop
        case _: DeathPactException => Stop
        case ex: Exception => {
          log.error( ex, "resuming stream after error during detection calculations" )
          Resume
        }
      }
    }
  }
}
