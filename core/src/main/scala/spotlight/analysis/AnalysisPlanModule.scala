package spotlight.analysis

import scala.reflect._
import scala.concurrent.duration._
import scala.concurrent.{ Await, TimeoutException }
import akka.NotUsed
import akka.actor._
import akka.actor.SupervisorStrategy.{ Resume, Stop }
import akka.event.LoggingReceive
import akka.stream.Supervision.Decider
import akka.stream.{ ActorAttributes, Materializer }
import akka.stream.scaladsl.Flow
import akka.stream.Supervision
import akka.util.Timeout
import com.persist.logging._
import shapeless.{ Lens, lens }
import com.typesafe.config.Config
import nl.grons.metrics.scala.{ Meter, MetricName }
import omnibus.akka.envelope._
import omnibus.akka.metrics.{ Instrumented, InstrumentedActor }
import omnibus.akka.publish.{ EventPublisher, StackableStreamPublisher }
import omnibus.archetype.domain.model.core.{ Entity, EntityIdentifying, EntityLensProvider }
import omnibus.commons.identifier.ShortUUID
import omnibus.akka.supervision.{ IsolatedDefaultSupervisor, OneForOneStrategyFactory }
import demesne._
import demesne.module.LocalAggregate
import demesne.module.entity.{ EntityAggregateModule, EntityProtocol }
import spotlight.model.outlier._
import spotlight.model.outlier.AnalysisPlan.Scope
import spotlight.model.timeseries._
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.analysis.AnalysisPlanProtocol.{ AnalysisFlow, MakeFlow }
import spotlight.analysis.OutlierDetection.{ DetectionResult, DetectionTimedOut }
import spotlight.analysis.{ AnalysisPlanProtocol ⇒ P }

object AnalysisPlanProtocol extends EntityProtocol[AnalysisPlanState#ID] {
  case class MakeFlow(
    override val targetId: MakeFlow#TID,
    parallelism: Int,
    system: ActorSystem,
    timeout: Timeout,
    materializer: Materializer
  ) extends Message

  case class AnalysisFlow( flow: DetectFlow ) extends ProtocolMessage with ClassLogging {
    log.warn( Map( "@msg" → "Made analysis plan flow", "flow" → flow.toString ) )
  }

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

  case class UseAlgorithms( override val targetId: UseAlgorithms#TID, algorithms: Map[String, Config] ) extends Command

  case class ResolveVia(
    override val targetId: ResolveVia#TID,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers
  ) extends Command

  override def tags: Set[String] = Set( AnalysisPlanModule.module.rootType.name )

  case class ScopeChanged( override val sourceId: ScopeChanged#TID, appliesTo: AnalysisPlan.AppliesTo ) extends TaggedEvent

  case class AlgorithmsChanged(
    override val sourceId: AlgorithmsChanged#TID,
    algorithms: Map[String, Config],
    added: Set[String],
    dropped: Set[String]
  ) extends TaggedEvent

  case class AnalysisResolutionChanged(
    override val sourceId: AnalysisResolutionChanged#TID,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers
  ) extends TaggedEvent

  case class GetPlan( override val targetId: GetPlan#TID ) extends Command
  case class PlanInfo( override val sourceId: PlanInfo#TID, info: AnalysisPlan ) extends Event {
    def toSummary: AnalysisPlan.Summary = {
      AnalysisPlan.Summary( id = sourceId, name = info.name, slug = info.slug, appliesTo = Option( info.appliesTo ) )
    }
  }
}

case class AnalysisPlanState( plan: AnalysisPlan ) extends Entity {
  override type ID = plan.ID
  override type TID = plan.TID
  override def id: TID = plan.id
  override def name: String = plan.name
  override def canEqual( that: Any ): Boolean = that.isInstanceOf[AnalysisPlanState]

  def algorithms: Map[String, Config] = plan.algorithms

  def routes( implicit model: DomainModel ): Map[String, AlgorithmRoute] = {
    implicit val ec = model.system.dispatcher

    def makeRoute( plan: AnalysisPlan )( algorithm: String ): Option[AlgorithmRoute] = {
      DetectionAlgorithmRouter.Registry.rootTypeFor( algorithm ).map { rt ⇒ AlgorithmRoute.routeFor( plan, rt )( model ) }
    }

    val routes = algorithms.keySet.toSeq.map { a ⇒ ( a, makeRoute( plan )( a ) ) }.collect { case ( a, Some( r ) ) ⇒ ( a, r ) }
    Map( routes: _* )
  }

}

//object AnalysisPlanState {
//  def allAlgorithms( algorithms: Set[String], algorithmSpec: Config ): Set[String] = {
//    import scala.collection.immutable
//    import scala.collection.JavaConverters._
//
//    val inSpec = algorithmSpec.root.entrySet.asScala.to[immutable.Set] map { _.getKey }
//    algorithms ++ inSpec
//  }
//}

/** Created by rolfsd on 5/26/16.
  */
object AnalysisPlanModule extends EntityLensProvider[AnalysisPlanState] with Instrumented with ClassLogging { moduleOuter ⇒
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
      override lazy val idTag: Symbol = AnalysisPlan.analysisPlanIdentifying.idTag
    }
  }

  val planLens: Lens[AnalysisPlanState, AnalysisPlan] = lens[AnalysisPlanState] >> 'plan
  override def idLens: Lens[AnalysisPlanState, AnalysisPlanState#TID] = AnalysisPlan.idLens compose planLens
  override def nameLens: Lens[AnalysisPlanState, String] = AnalysisPlan.nameLens compose planLens
  override def slugLens: Lens[AnalysisPlanState, String] = AnalysisPlan.slugLens compose planLens
  val isActiveLens: Lens[AnalysisPlanState, Boolean] = AnalysisPlan.isActiveLens compose planLens

  val module: EntityAggregateModule[AnalysisPlanState] = {
    val b = EntityAggregateModule.builderFor[AnalysisPlanState, AnalysisPlanProtocol.type].make
    import b.P.{ Props ⇒ BProps, _ }

    b
      .builder
      .set( Environment, LocalAggregate )
      .set( BProps, AggregateRoot.PlanActor.props( _, _ ) )
      .set( PassivateTimeout, 5.minutes )
      .set( Protocol, AnalysisPlanProtocol )
      .set( IdLens, moduleOuter.idLens )
      .set( NameLens, moduleOuter.nameLens )
      .set( IsActiveLens, Some( moduleOuter.isActiveLens ) )
      .build()
  }

  object AggregateRoot {

    object PlanActor {
      def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

      private class Default( model: DomainModel, rootType: AggregateRootType )
          extends PlanActor( model, rootType )
          with WorkerProvider
          with FlowConfigurationProvider
          with StackableStreamPublisher {
        override val bufferSize: Int = 1000

        override protected def onPersistRejected( cause: Throwable, event: Any, seqNr: Long ): Unit = {
          altLog.error(
            msg = Map(
              "@msg" → "persist event rejected",
              "event-type" → event.getClass.getName,
              "persistence-id" → aggregateId,
              "sequence-nr" → seqNr
            ),
            ex = cause
          )
          throw cause
        }
      }

      trait FlowConfigurationProvider {
        def bufferSize: Int
      }

      trait WorkerProvider {
        provider: Actor with AltActorLogging ⇒
        def model: DomainModel

        def routerName( p: AnalysisPlan ): String = DetectionAlgorithmRouter.name( p.name )
        def detectorName( p: AnalysisPlan ): String = OutlierDetection.name( p.name )

        def startWorkers( p: AnalysisPlan, routes: Map[String, AlgorithmRoute] ): ( ActorRef, ActorRef, ActorRef ) = {
          import omnibus.akka.supervision.IsolatedLifeCycleSupervisor.{ WaitForStart, GetChildren, Children, Started }
          import akka.pattern.ask

          altLog.info(
            Map(
              "@msg" → "starting analysis plan foreman and workers(router and detector) with routes",
              "plan" → p.name,
              "routes" → routes.map { case ( s, r ) ⇒ s"${s}->${r}" }.mkString( ", " )
            )
          )

          implicit val ec: scala.concurrent.ExecutionContext = context.dispatcher
          implicit val timeout: Timeout = Timeout( 1.second )

          val foreman = context.system.actorOf(
            Props(
              new IsolatedDefaultSupervisor() with OneForOneStrategyFactory {
                override def childStarter() = {
                  val router = makeRouter( p, routes )
                  val detector = makeDetector( p, router )
                }
              }
            ),
            s"${p.name}-foreman"
          )
          altLog.info( Map( "@msg" → "created plan foreman", "foreman" → foreman.path ) )

          val f = {
            for {
              _ ← ( foreman ? WaitForStart ).mapTo[Started.type]
              cs ← ( foreman ? GetChildren ).mapTo[Children]
            } yield {
              altLog.info(
                Map(
                  "@msg" → "plan foreman started with children",
                  "plan" → p.name,
                  "children" → cs.children.map( _.name ).mkString( ", " )
                )
              )
              val actors = {
                for {
                  router ← cs.children collectFirst { case c if c.name contains provider.routerName( p ) ⇒ c.child }
                  detector ← cs.children collectFirst { case c if c.name contains provider.detectorName( p ) ⇒ c.child }
                } yield ( foreman, detector, router )
              }

              actors.get
            }
          }

          altLog.debug( Map( "@msg" → "plan starting foreman...", "plan" → p.name ) )
          val r = Await.result( f, 1.second )

          altLog.debug(
            Map(
              "@msg" → "plan foreman and workers started",
              "plan" → p.name,
              "workers" → Map( "foreman" → r._1.path, "detector" → r._2.path, "router" → r._3.path )
            )
          )

          r
        }

        def makeRouter( p: AnalysisPlan, routes: Map[String, AlgorithmRoute] )( implicit context: ActorContext ): ActorRef = {
          val routerProps = DetectionAlgorithmRouter.props( p, routes )
          context.actorOf( routerProps.withDispatcher( DetectionAlgorithmRouter.DispatcherPath ), provider.routerName( p ) )
        }

        def makeDetector( p: AnalysisPlan, routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
          context.actorOf(
            OutlierDetection.props( routerRef ).withDispatcher( OutlierDetection.DispatcherPath ),
            provider.detectorName( p )
          )
        }
      }
    }

    //todo convert to perist logging w scala 2.12
    class PlanActor( override val model: DomainModel, override val rootType: AggregateRootType )
        extends module.EntityAggregateActor
        with AltActorLogging
        with InstrumentedActor
        with demesne.AggregateRoot.Provider {
      actorOuter: PlanActor.FlowConfigurationProvider with PlanActor.WorkerProvider with EventPublisher ⇒

      override lazy val metricBaseName: MetricName = MetricName( classOf[PlanActor] )
      val failuresMeter: Meter = metrics.meter( "failures" )

      override var state: AnalysisPlanState = _

      var detector: ActorRef = _

      override def acceptance: Acceptance = myAcceptance orElse entityAcceptance

      val myAcceptance: Acceptance = {
        case ( P.Added( _, info ), s ) ⇒ {
          preActivate()
          context become LoggingReceive { around( active ) }
          val newState = info match {
            case Some( ps: AnalysisPlanState ) ⇒ ps
            case Some( p: AnalysisPlan ) ⇒ AnalysisPlanState( p )
            case i ⇒ altLog.error( Map( "@msg" → "ignoring Added command with unrecognized info", "info" → i.toString ) ); s
          }

          val ( _, d, _ ) = startWorkers( newState.plan, newState.routes( model ) )
          actorOuter.detector = d

          altLog.debug(
            Map(
              "@msg" → "Plan added",
              "plan" → newState.plan.name,
              "state" → newState.toString(),
              "detector" → actorOuter.detector.path
            )
          )

          newState
        }

        case ( e: P.ScopeChanged, s ) ⇒ {
          //todo: cast for expediency. my ideal is to define a Lens in the AnalysisPlan trait; minor solace is this module is in the same package
          s.copy( plan = s.plan.asInstanceOf[AnalysisPlan.SimpleAnalysisPlan].copy( appliesTo = e.appliesTo ) )
        }

        case ( e: P.AlgorithmsChanged, s ) ⇒ {
          //todo: cast for expediency. my ideal is to define a Lens in the AnalysisPlan trait; minor solace is this module is in the same package
          s.copy(
            plan = s.plan.asInstanceOf[AnalysisPlan.SimpleAnalysisPlan].copy(
              algorithms = e.algorithms //,
            //              algorithmConfig = e.algorithmConfig
            )
          )
        }

        case ( e: P.AnalysisResolutionChanged, s ) ⇒ {
          //todo: cast for expediency. my ideal is to define a Lens in the AnalysisPlan trait; minor solace is this module is in the same package
          s.copy( plan = s.plan.asInstanceOf[AnalysisPlan.SimpleAnalysisPlan].copy( isQuorum = e.isQuorum, reduce = e.reduce ) )
        }
      }

      val IdType = classTag[identifying.TID]

      override def quiescent: Receive = {
        case P.Add( IdType( targetId ), info ) if targetId == aggregateId ⇒ {
          persist( P.Added( targetId, info ) ) { e ⇒
            acceptAndPublish( e )
            sender() !+ e // per akka docs: safe to use sender() here
          }
        }

        case P.Add( targetId, info ) ⇒ {
          altLog.error(
            Map(
              "@msg" → "ignoring received Add message with unrecognized targetId",
              "targetId" → targetId.toString,
              "targetId-class" → targetId.id.getClass.getName,
              "aggregateId" → aggregateId.toString,
              "aggregateId-class" → aggregateId.getClass.getName
            )
          )
        }

        case m ⇒ altLog.error( Map( "@msg" → "ignoring unrecognized message", "message" → m.toString ) )
      }

      override def active: Receive = workflow orElse planEntity orElse super.active

      val workflow: Receive = {
        case MakeFlow( _, parallelism, system, timeout, materializer ) ⇒ {
          sender() ! AnalysisFlow( makeFlow( parallelism )( system, timeout, materializer ) )
        }
      }

      val planEntity: Receive = {
        case _: P.GetPlan ⇒ sender() !+ P.PlanInfo( state.id, state.plan )

        case P.ApplyTo( id, appliesTo ) ⇒ persist( P.ScopeChanged( id, appliesTo ) ) { acceptAndPublish }

        case P.UseAlgorithms( id, algorithms ) ⇒ persist( changeAlgorithms( algorithms ) ) { acceptAndPublish }

        case P.ResolveVia( id, isQuorum, reduce ) ⇒ {
          persist( P.AnalysisResolutionChanged( id, isQuorum, reduce ) ) { acceptAndPublish }
        }
      }

      def changeAlgorithms( algorithms: Map[String, Config] ): P.AlgorithmsChanged = {
        val myAlgorithms = state.algorithms.keySet
        //        val newAlgorithms = AnalysisPlanState.allAlgorithms( algorithms, algorithmSpec )

        P.AlgorithmsChanged(
          sourceId = aggregateId,
          algorithms = algorithms,
          //          algorithmConfig = algorithmSpec,
          added = algorithms.keySet -- myAlgorithms,
          dropped = myAlgorithms -- algorithms.keySet
        )
      }

      override def unhandled( message: Any ): Unit = {
        altLog.error( Map( "@msg" → "unhandled message", "aggregateId" → aggregateId.toString, "message" → message.toString ) )
        super.unhandled( message )
      }

      def makeFlow(
        parallelism: Int
      )(
        implicit
        system: ActorSystem,
        timeout: Timeout,
        materializer: Materializer
      ): DetectFlow = {
        val entry = Flow[TimeSeries].filter { state.plan.appliesTo }
        val withGrouping = state.plan.grouping map { g ⇒ entry.via( batchSeries( g ) ) } getOrElse entry

        withGrouping
          .via( detectionFlow( state.plan, parallelism ) )
          .named( s"AnalysisPlan:${state.plan.name}@${state.plan.id.id}" )
      }

      def batchSeries(
        grouping: AnalysisPlan.Grouping
      )(
        implicit
        tsMerging: Merging[TimeSeries]
      ): Flow[TimeSeries, TimeSeries, NotUsed] = {
        Flow[TimeSeries]
          .groupedWithin( n = grouping.limit, d = grouping.window )
          .map {
            _
              .groupBy { _.topic }
              .map {
                case ( _, tss ) ⇒
                  tss.tail.foldLeft( tss.head ) { case ( acc, ts ) ⇒ tsMerging.merge( acc, ts ) valueOr { exs ⇒ throw exs.head } }
              }
          }
          .mapConcat { identity }
      }

      def detectionFlow( p: AnalysisPlan, parallelism: Int )( implicit system: ActorSystem, timeout: Timeout ): DetectFlow = {
        import omnibus.akka.envelope.pattern.ask

        implicit val ec: scala.concurrent.ExecutionContext = system.dispatcher

        if ( detector == null && detector != context.system.deadLetters ) {
          val ex = new IllegalStateException( s"analysis plan [${p.name}] flow invalid detector reference:[${detector}]" )

          altLog.error(
            Map(
              "@msg" → "analysis plan [${p.name}] flow created missing valid detector reference:[${detector}]",
              "plan" → p.name,
              "detector" → detector.path
            ),
            ex
          )

          throw ex
        }

        Flow[TimeSeries]
          .map { ts ⇒ OutlierDetectionMessage( ts, p ).disjunction }
          .collect { case scalaz.\/-( m ) ⇒ m }
          .map { m ⇒
            inletSeries.mark()
            inletPoints.mark( m.source.points.size )
            m
          }
          .mapAsyncUnordered( parallelism ) { m ⇒
            ( detector ?+ m )
              .recover {
                case ex: TimeoutException ⇒ {
                  altLog.error(
                    Map(
                      "@msg" → "timeout exceeded waiting for detection",
                      "timeout" → timeout.duration.toCoarsest.toString,
                      "plan" → m.plan.name,
                      "topic" → m.topic.toString
                    ),
                    ex
                  )

                  DetectionTimedOut( m.source, m.plan )
                }
              }
          }.withAttributes( ActorAttributes supervisionStrategy detectorDecider )
          .filter {
            case Envelope( DetectionResult( outliers, _ ), _ ) ⇒ true
            case DetectionResult( outliers, _ ) ⇒ true
            case Envelope( DetectionTimedOut( s, _ ), _ ) ⇒ {
              droppedSeriesMeter.mark()
              droppedPointsMeter.mark( s.points.size )
              false
            }
            case DetectionTimedOut( s, _ ) ⇒ {
              droppedSeriesMeter.mark()
              droppedPointsMeter.mark( s.points.size )
              false
            }
          }
          .collect {
            case Envelope( DetectionResult( outliers, _ ), _ ) ⇒ outliers
            case DetectionResult( outliers, _ ) ⇒ outliers
          }
          .map {
            case m ⇒ {
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
          altLog.error( "error in detection dropping series", ex )
          droppedSeriesMeter.mark()
          Supervision.Resume
        }
      }

      override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
        case _: ActorInitializationException ⇒ Stop
        case _: ActorKilledException ⇒ Stop
        case _: DeathPactException ⇒ Stop
        case ex: Exception ⇒ {
          altLog.error( "resuming stream after error during detection calculations", ex )
          Resume
        }
      }
    }
  }
}
