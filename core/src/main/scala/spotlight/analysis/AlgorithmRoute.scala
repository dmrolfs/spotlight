package spotlight.analysis

import scala.reflect._
import akka.actor.{ActorContext, ActorRef}
import com.persist.logging._
import demesne.{AggregateRootType, DomainModel}
import peds.akka.envelope._
import peds.commons.identifier.{Identifying, ShortUUID, TaggedID}
import peds.commons.util._
import spotlight.analysis.shard.{ShardCatalog, ShardProtocol, ShardingStrategy}
import spotlight.model.outlier.AnalysisPlan


sealed abstract class AlgorithmRoute {
  def forward( message: Any )( implicit sender: ActorRef, context: ActorContext ): Unit = {
    referenceFor( message ) forwardEnvelope message
  }

  def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef
}

object AlgorithmRoute extends ClassLogging {
  def routeFor(
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType
  )(
    implicit model: DomainModel
  ): AlgorithmRoute = {
    val strategy = ShardingStrategy from plan

    val route = {
      strategy
      .map { strategy => ShardedRoute( plan, algorithmRootType, strategy, model ) }
      .getOrElse { RootTypeRoute( algorithmRootType, model ) }
    }

    log.debug(
      Map(
        "@msg" -> "determined algorithm route",
        "strategy" -> strategy.map{ _.key }.toString,
        "plan" -> plan.name,
        "algorithm" -> algorithmRootType.name,
        "route" -> route.toString
      )
    )

    route
  }


  case class DirectRoute( reference: ActorRef ) extends AlgorithmRoute {
    override def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef = reference
  }

  case class RootTypeRoute( algorithmRootType: AggregateRootType, model: DomainModel ) extends AlgorithmRoute {
    val TidType: ClassTag[TaggedID[ShortUUID]] = classTag[TaggedID[ShortUUID]]

    override def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef = {
      message match {
        case m: OutlierDetectionMessage => model( algorithmRootType, m.plan.id )
        case id: ShortUUID => model( algorithmRootType, id )
        case TidType(tid) => model( algorithmRootType, tid )
        case _ => model.system.deadLetters
      }
    }
  }

  case class ShardedRoute(
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    strategy: ShardingStrategy,
    implicit val model: DomainModel
  ) extends AlgorithmRoute {
    implicit val scIdentifying: Identifying[ShardCatalog.ID] = strategy.identifying
    val shardingId: ShardCatalog#TID = strategy.idFor( plan, algorithmRootType.name )
    val shardingRef = strategy.actorFor( plan, algorithmRootType )( model )

    override def forward( message: Any )( implicit sender: ActorRef, context: ActorContext ): Unit = {
      referenceFor(message) forwardEnvelope ShardProtocol.RouteMessage( shardingId, message )
    }

    override def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef = shardingRef
    override def toString: String = {
      s"${getClass.safeSimpleName}( " +
        s"plan:[${plan.name}] " +
        s"algorithmRootType:[${algorithmRootType.name}] " +
        s"strategy:[${strategy}] " +
        s"model:[${model}] )"
    }
  }
}
