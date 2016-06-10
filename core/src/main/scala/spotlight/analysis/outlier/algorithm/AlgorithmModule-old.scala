//package spotlight.analysis.outlier.algorithm
//
//import akka.actor.{ActorPath, ActorRef, Props}
//import akka.event.LoggingReceive
//import demesne.{AggregateRootType, DomainModel}
//
//import scalaz._
//import Scalaz._
//import scalaz.Kleisli.{ask, kleisli}
//import demesne.module.EntityAggregateModule
//import demesne.register.AggregateIndexSpec
//import nl.grons.metrics.scala.{MetricName, Timer}
//import org.apache.commons.math3.ml.clustering.DoublePoint
//import peds.akka.metrics.InstrumentedActor
//import peds.akka.publish.EventPublisher
//import peds.archetype.domain.model.core.Entity
//import peds.commons.{KOp, TryV}
//import peds.commons.identifier.{ShortUUID, TaggedID}
//import shapeless.Lens
//import spotlight.analysis.outlier.{DetectUsing, DetectionAlgorithmRouter, HistoricalStatistics, UnrecognizedPayload}
//import spotlight.model.outlier.OutlierPlan
//import spotlight.model.timeseries._
//
//import scala.reflect.ClassTag
//
//
///**
//  * Created by rolfsd on 6/1/16.
//  */
//object AlgorithmModule {
//  val ScopeIndex: Symbol = 'scope
//  val idTag: Symbol = 'algorithm
//  val nextId: AlgorithmModule#AnalysisState#TID = TaggedID( idTag, ShortUUID() )
//}
//
//trait AlgorithmModule {
//  type Context <: AlgorithmContext
//  type State <: AnalysisState
//  implicit def evState: ClassTag[State]
//
//  def makeContext( msg: DetectUsing ): Context
//
//
//  trait AlgorithmContext {
//    def message: DetectUsing
//    def data: Seq[DoublePoint]
//    def plan: OutlierPlan = message.payload.plan
//    //    def historyKey: OutlierPlan.Scope = OutlierPlan.Scope( plan, topic )
//    def source: TimeSeriesBase = message.source
////    def messageConfig: Config = message.properties //should configure into specific AlgoConfig via Add
//    def state: State
//  }
//
//
//  trait AnalysisState extends Entity {
//    override type ID = ShortUUID
//    override def idClass: Class[_] = classOf[ShortUUID]
//    def scope: OutlierPlan.Scope // this could (and should) be ID but need to change demesne to allow other ID types for Entity
//    def algorithm: Symbol = Symbol(name)
//    def topic: Topic = scope.topic
//
//    def history: HistoricalStatistics
//
//    def thresholds: Seq[ThresholdBoundary]
//    def addThreshold( threshold: ThresholdBoundary ): State
//
//    def tolerance: Double
//  }
//
//
//  def idTag: Symbol
//  implicit def tag( id: State#ID ): State#TID = TaggedID( idTag, id )
//
//  def idLens: Lens[State, State#TID]
//  def nameLens: Lens[State, String]
//  def slugLens: Lens[State, String]
//
//
//  trait AggregateRoot {
//    val module: EntityAggregateModule[State] = {
//      val b = EntityAggregateModule.builderFor[State].make
//      import b.P.{ Tag => BTag, Props => BProps, _ }
//
//      b.builder
//      .set( BTag, idTag )
//      .set( BProps, actorCompanion.props(_, _) )
//      .set( Indexes, myIndexes )
//      .set( IdLens, idLens )
//      .set( NameLens, nameLens )
//      .set( SlugLens, Some(slugLens) )
//      .build()
//    }
//
//    import AlgorithmModule.ScopeIndex
//
//    val myIndexes: () => Seq[AggregateIndexSpec[_, _]] = () => {
//      Seq(
//        demesne.register.local.RegisterLocalAgent.spec[OutlierPlan.Scope, AnalysisState#TID]( ScopeIndex ) {
//          case module.EntityProtocol.Added( info ) => {
//            val e = module.infoToEntity( info )
//            demesne.register.Directive.Record( e.scope, module.idLens.get(e) )
//          }
////           case module.EntityProtocol.Disabled( id, _ ) => demesne.register.Directive.Withdraw( id )
////           case module.EntityProtocol.Enabled( id, slug ) => demesne.register.Directive.Record( slug, id )
//        }
//      )
//    }
//
//    trait Protocol extends module.Protocol {
//      val Algorithm = AlgorithmProtocol
//    }
//
//    object AlgorithmProtocol {
//      sealed trait AlgorithmMessage
//
//      case class Register( override val targetId: Register#TID, routerRef: ActorRef )
//        extends module.Protocol#Command with AlgorithmMessage
//
//      case class Registered( override val sourceId: Registered#TID )
//        extends module.Protocol#Event with AlgorithmMessage
//
//
//      case class AlgorithmUsedBeforeRegistrationError( algorithm: Symbol, path: ActorPath )
//        extends IllegalStateException( s"actor [${path}] not registered algorithm [${algorithm.name}] before use" )
//                with OutlierAlgorithmError
//    }
//
//
//    def actorCompanion: AlgorithmActorCompanion
//
//    trait AlgorithmActorCompanion {
//      def props( model: DomainModel, meta: AggregateRootType ): Props
//    }
//
//    trait AlgorithmActor extends module.EntityAggregateActor with InstrumentedActor {
//      publisher: EventPublisher =>
//      override lazy val metricBaseName: MetricName = MetricName( "spotlight.analysis.outlier.algorithm" )
//      lazy val algorithmTimer: Timer = metrics timer state.name
//
//      override def quiescent: Receive = {
//        case module.EntityProtocol.Add( info ) => {
//          persist( module.EntityProtocol.Added( info ) ) { e =>
//            acceptAndPublish( e )
//            context become LoggingReceive { around( waitingToRegister() ) }
//          }
//        }
//      }
//
//      def waitingToRegister(controller: Option[ActorRef] = None): Receive = {
//        case AlgorithmProtocol.Register( scopeId, routerRef ) => {
//          routerRef ! DetectionAlgorithmRouter.RegisterDetectionAlgorithm( state.algorithm, self )
//          context become LoggingReceive {around( waitingToRegister( Some( sender( ) ) ) )}
//        }
//
//        case msg@DetectionAlgorithmRouter.AlgorithmRegistered( a ) if controller.isDefined && a == state.algorithm => {
//          controller foreach {_ ! AlgorithmProtocol.Registered( state.id )}
//          context become LoggingReceive {around( active )}
//        }
//
//        case m: DetectUsing => throw AlgorithmProtocol.AlgorithmUsedBeforeRegistrationError( state.algorithm, self.path )
//      }
//
//      override def active: Receive = detect orElse super.active
//
//      def detect: Receive
//
//      override def unhandled(message: Any): Unit = {
//        message match {
//          case m: DetectUsing => {
//            log.error( "algorithm [{}] does not recognize requested payload: [{}]", state.algorithm, m )
//            m.aggregator ! UnrecognizedPayload( state.algorithm, m )
//          }
//
//          case m => super.unhandled( m )
//        }
//      }
//
//
//      // -- algorithm functional operations --
//
//      val algorithmContext: KOp[DetectUsing, Context] = kleisli[TryV, DetectUsing, Context] { msg => makeContext( msg ).right }
//
//      /**
//        * Some algorithms require a minimum number of points in order to determine an anomaly. To address this circumstance, these
//        * algorithms can use fillDataFromHistory to draw from history the points necessary to create an appropriate group.
//        *
//        * @param minimalSize of the data grouping
//        * @return
//        */
//      def fillDataFromHistory(minimalSize: Int = HistoricalStatistics.LastN): KOp[Context, Seq[DoublePoint]] = {
//        for {
//          ctx <- ask[TryV, Context]
//        } yield {
//          val original = ctx.data
//          if ( minimalSize < original.size ) original
//          else {
//            val inHistory = ctx.state.history.lastPoints.size
//            val needed = minimalSize + 1 - original.size
//            val past = ctx.state.history.lastPoints.drop( inHistory - needed )
//            past.toDoublePoints ++ original
//          }
//        }
//      }
//    }
//  }
//}
