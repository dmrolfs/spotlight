//package lineup.analysis.outlier
//
//import scala.concurrent.duration._
//import akka.actor.{ ActorRef, Props }
//import akka.event.LoggingReceive
//import shapeless._
//import demesne.{ AggregateRootType, DomainModel }
//import demesne.module.EntityAggregateModule
//import demesne.register.StackableRegisterBusPublisher
//import peds.akka.envelope._
//import peds.akka.publish.{ EventPublisher, StackableStreamPublisher }
//import peds.commons.log.Trace
//import peds.commons.identifier.ShortUUID
//import lineup.model.outlier.{ ReduceOutliers, IsQuorum, OutlierPlan }
//
//
///**
// * Created by rolfsd on 9/29/15.
// */
//object OutlierPlanAggregateRoot {
//  val trace = Trace[OutlierPlanAggregateRoot.type]
//
//  val module: EntityAggregateModule[OutlierPlan] = {
//    val b = EntityAggregateModule.builderFor[OutlierPlan].make
//    import b.P.{ Props => BProps, _ }
//    b.builder
//     .set( Tag, OutlierPlan.idTag )
//     .set( BProps, OutlierPlanActor.props(_: DomainModel, _: AggregateRootType) )
//     .set( IdLens, OutlierPlan.idLens )
//     .set( NameLens, OutlierPlan.nameLens )
//     .set( SlugLens, Some(OutlierPlan.slugLens) )
//     .build
//  }
//
//
//  sealed trait OutlierPlanMessage
//
//  sealed trait Command extends OutlierPlanMessage {
//    def targetId: OutlierPlan#TID
//  }
//
//  sealed trait Event extends OutlierPlanMessage {
//    def sourceId: OutlierPlan#TID
//  }
//
//  case class AddAlgorithms( override val targetId: OutlierPlan#TID, algorithms: Set[String] ) extends Command
//  case class RemoveAlgorithms( override val targetId: OutlierPlan#TID, algorithms: Set[String] ) extends Command
//  case class AlgorithmsAdded( override val sourceId: OutlierPlan#TID, algorithms: Set[String]  ) extends Event
//  case class AlgorithmsRemoved( override val sourceId: OutlierPlan#TID, algortihms: Set[String] ) extends Event
//
//  case class AddAlgorithmProperties( override val targetId: OutlierPlan#TID, properties: Map[String, Any] ) extends Command
//  case class RemoveAlgorithmProperties( override val targetId: OutlierPlan#TID, keys: Set[String] ) extends Command
//  case class AlgorithmPropertiesAdded( override val sourceId: OutlierPlan#TID, properties: Map[String, Any] ) extends Event
//  case class AlgorithmPropertiesRemoved( override val sourceId: OutlierPlan#TID, keys: Set[String] ) extends Event
//
//  case class AssignQuorumHandling(
//    override val targetId: OutlierPlan#TID,
//    isQuorum: Option[IsQuorum] = None,
//    reduce: Option[ReduceOutliers] = None,
//    timeout: Option[FiniteDuration] = None
//  ) extends Command
//
//  case class QuorumStrategyChanged(
//    override val sourceId: OutlierPlan#TID,
//    isQuorum: Option[IsQuorum],
//    reduce: Option[ReduceOutliers],
//    timeout: Option[FiniteDuration]
//  ) extends Event
//
//  object QuorumStrategyChanged {
//    def fromCommand( c: AssignQuorumHandling ): QuorumStrategyChanged = {
//      QuorumStrategyChanged(
//        sourceId = c.targetId,
//        isQuorum = c.isQuorum,
//        reduce = c.reduce,
//        timeout = c.timeout
//      )
//    }
//  }
//
//
//  object OutlierPlanActor {
//    def props( model: DomainModel, meta: AggregateRootType ): Props = {
//      val router = module.moduleProperties( DetectionAlgorithmRouter.ContextKey ).asInstanceOf[ActorRef]
//      Props( new OutlierPlanActor( model, meta, router ) with StackableStreamPublisher with StackableRegisterBusPublisher )
//    }
//  }
//
//  class OutlierPlanActor( override val model: DomainModel, override val meta: AggregateRootType, router: ActorRef )
//  extends module.EntityAggregateActor { publisher: EventPublisher =>
//    override val acceptance: Acceptance = outlierAcceptance orElse entityAcceptance
//
//    override var state: OutlierPlan = _
//
//    def outlierAcceptance: Acceptance = {
//      case (AlgorithmsAdded(id, algorithms), s) => {
//        val lens = OutlierPlan.algorithmsLens
//        val existing = lens get s
//        val result = lens.set( s )( existing ++ algorithms )
//        if ( lens.get(result).nonEmpty ) context.become( around( processor orElse active ) )
//        result
//      }
//
//      case (AlgorithmsRemoved(id, algortihms), s) => {
//        val lens = OutlierPlan.algorithmsLens
//        val existing = lens get s
//        val result = lens.set( s )( existing -- algortihms )
//        if ( lens.get(result).nonEmpty ) context.become( around( processor orElse active ) )
//        result
//      }
//
//      case (AlgorithmPropertiesAdded(id, properties), s) => {
//        val lens = OutlierPlan.algorithmPropertiesLens
//        val existing = lens get s
//        lens.set( s )( existing ++ properties )
//      }
//
//      case (AlgorithmPropertiesRemoved(id, keys), s) => {
//        val lens = OutlierPlan.algorithmPropertiesLens
//        val existing = lens get s
//        lens.set( s )( existing -- keys )
//      }
//
//      case (QuorumStrategyChanged(id, isQuorum, reduce, timeout), s) => {
//        def updateValue[T]( value: Option[T], lens: Lens[OutlierPlan, T] )( op: OutlierPlan ): OutlierPlan = {
//          val result = value map { v => lens.set( op )( v ) }
//          result getOrElse op
//        }
//
//        val u1 = updateValue( isQuorum, OutlierPlan.isQuorumLens )_
//        val u2 = updateValue( reduce, OutlierPlan.reduceLens )_
//        val u3 = updateValue( timeout, OutlierPlan.timeoutLens )_
//        val update = u3 compose u2 compose u1
//        update( s )
//      }
//    }
//
//
//    val processor: Receive = LoggingReceive {
//      case series: DetectOutliersInSeries => {
//        dispatch( series, context.actorOf( OutlierQuorumAggregator.props(state, model), aggregatorName(series) ) )
//      }
//
//      case cohort: DetectOutliersInCohort => {
//        dispatch( cohort, context.actorOf( OutlierQuorumAggregator.props(state, model), aggregatorName(cohort) ) )
//      }
//    }
//
//    def aggregatorName( m: OutlierDetectionMessage ): String = {
//      val name = m match {
//        case DetectOutliersInSeries( data ) => data.name
//        case DetectOutliersInCohort( data ) => data.name
//        case m: DetectUsing => aggregatorName( m.payload )
//      }
//
//      s"quorum-${state.slug}-${name}-${ShortUUID()}"
//    }
//
//    override def active: Receive = super.active orElse LoggingReceive {
//      case AddAlgorithms(id, algorithms) => persistAsync( AlgorithmsAdded(id, algorithms) ) { e => acceptAndPublish( e ) }
//
//      case RemoveAlgorithms(id, algorithms) => persistAsync( AlgorithmsRemoved(id, algorithms) ) { e => acceptAndPublish( e ) }
//
//      case AddAlgorithmProperties(id, properties) => persistAsync( AlgorithmPropertiesAdded(id, properties) ) { e =>
//        acceptAndPublish( e )
//      }
//
//      case RemoveAlgorithmProperties(id, keys) => persistAsync( AlgorithmPropertiesRemoved(id, keys) ) { e =>
//        acceptAndPublish( e )
//      }
//
//      case c: AssignQuorumHandling => persistAsync( QuorumStrategyChanged fromCommand c ) { e => acceptAndPublish( e ) }
//    }
//
//    def dispatch( m: OutlierDetectionMessage, destination: ActorRef ): Unit = trace.block( s"dispatch(${destination.path.name}, $m" ) {
//      state.algorithms foreach { a =>
//        router !+ DetectUsing( algorithm = a, destination = destination, payload = m, properties = state.algorithmProperties )
//      }
//    }
//  }
//}
