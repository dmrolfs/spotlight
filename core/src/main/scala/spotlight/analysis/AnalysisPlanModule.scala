package spotlight.analysis

import scala.reflect._
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor._
import akka.actor.SupervisorStrategy.{ Resume, Stop }
import akka.cluster.sharding.ClusterShardingSettings
import akka.event.LoggingReceive
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
import demesne.module.{ ClusteredAggregate, LocalAggregate }
import demesne.module.entity.EntityAggregateModule
import spotlight.model.outlier._
import spotlight.analysis.algorithm.AlgorithmRoute
import spotlight.analysis.{ AnalysisPlanProtocol ⇒ P }
import spotlight.infrastructure.ClusterRole

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

/** Created by rolfsd on 5/26/16.
  */
object AnalysisPlanModule extends EntityLensProvider[AnalysisPlanState] with Instrumented with ClassLogging { moduleOuter ⇒

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

    val toSettings = ( s: ActorSystem ) ⇒ ClusterShardingSettings( s ).withRole( ClusterRole.Analysis.entryName )

    b.builder
      //      .set( Environment, LocalAggregate )
      .set( Environment, ClusteredAggregate( toSettings ) )
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
        case P.RouteDetection( id, m ) if detector == null ⇒ {
          altLog.error(
            Map(
              "@msg" → "ignoring outlier detection request since detection workers are not initialized for plan",
              "id" → id.toString,
              "message" → Map( "plan" → m.plan.name, "topic" → m.topic.toString )
            )
          )
        }

        case P.RouteDetection( _, m ) ⇒ detector forward m

        case m: P.MakeFlow ⇒ sender() !+ P.AnalysisFlow( state.id, new AnalysisFlowFactory( state.plan ) )
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

        P.AlgorithmsChanged(
          sourceId = aggregateId,
          algorithms = algorithms,
          added = algorithms.keySet -- myAlgorithms,
          dropped = myAlgorithms -- algorithms.keySet
        )
      }

      override def unhandled( message: Any ): Unit = {
        altLog.error( Map( "@msg" → "unhandled message", "aggregateId" → aggregateId.toString, "message" → message.toString ) )
        super.unhandled( message )
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
