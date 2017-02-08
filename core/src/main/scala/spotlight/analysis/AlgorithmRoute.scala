package spotlight.analysis

import scala.reflect._
import akka.actor.{ActorContext, ActorRef}
import scalaz.\/
import com.persist.logging._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValueType}
import demesne.{AggregateRootType, DomainModel}
import peds.akka.envelope._
import peds.commons.identifier.{Identifying, ShortUUID, TaggedID}
import peds.commons.util._
import spotlight.Settings
import spotlight.analysis.algorithm.{AlgorithmIdentifier, AlgorithmModule, AlgorithmProtocol}
import spotlight.analysis.shard._
import spotlight.model.outlier.AnalysisPlan
import squants.information.{Bytes, Information, Megabytes}


sealed abstract class AlgorithmRoute {
  def forward( message: Any )( implicit sender: ActorRef, context: ActorContext ): Unit = {
    referenceFor( message ) forwardEnvelope message
  }

  def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef
}

object AlgorithmRoute extends ClassLogging {
  import ShardedRoute.Strategy

  def routeFor(
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType
  )(
    implicit model: DomainModel
  ): AlgorithmRoute = {
    val strategy = Strategy from plan

    val route = {
      strategy
      .map { strategy => ShardedRoute( plan, algorithmRootType, strategy, model ) }
      .getOrElse { RootTypeRoute( plan, algorithmRootType, model ) }
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

  case class RootTypeRoute(
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    model: DomainModel
  ) extends AlgorithmRoute with ClassLogging with com.typesafe.scalalogging.StrictLogging {
    implicit lazy val algorithmIdentifying: Identifying[_] = algorithmRootType.identifying

    override def forward( message: Any )( implicit sender: ActorRef, context: ActorContext ): Unit = {
      referenceFor( message ) forwardEnvelope AlgorithmProtocol.RouteMessage( targetId = algorithmIdFor( message ), message )
    }

    def algorithmIdFor( message: Any ): AlgorithmModule.TID = {
      val result = message match {
        case m: DetectUsing => {
          algorithmIdentifying.tag(
            AlgorithmIdentifier(
              planName = plan.name,
              planId = plan.id.id.toString,
              spanType = AlgorithmIdentifier.TopicSpan,
              span = m.topic.toString
            ).asInstanceOf[algorithmIdentifying.ID]
          )
        }

        case m if algorithmRootType.aggregateIdFor isDefinedAt m => {
          import scalaz.{ \/-, -\/ }

          val (aid, _) = algorithmRootType.aggregateIdFor( m )
          AlgorithmIdentifier.fromAggregateId( aid ).disjunction match {
            case \/-( pid ) => algorithmIdentifying.tag( pid.asInstanceOf[algorithmIdentifying.ID] )
            case -\/( exs ) => {
              exs foreach { ex =>
                log.error(
                  Map(
                    "@msg" -> "failed to parse algorithm identifier from aggregate id",
                    "aggregateId" -> aid,
                    "message" -> m.toString
                  ),
                  ex
                )
              }

              throw exs.head
            }
          }
        }

        case m => {
          val ex = new IllegalStateException(
            s"failed to extract algorithm[${algorithmRootType.name}] aggregate id from message:[${m}]"
          )

          log.error(
            Map(
              "@msg" -> "failed to extract algorithm aggregate id from message",
              "algorithm" -> algorithmRootType.name,
              "message-class" -> m.getClass.getName
            ),
            ex
          )

          throw ex
        }
      }

      result.asInstanceOf[AlgorithmModule.TID]
    }

    override def referenceFor( message: Any )( implicit context: ActorContext ): ActorRef = {
      \/
      .fromTryCatchNonFatal { model( algorithmRootType, algorithmIdFor(message) ) }
      .valueOr { ex =>
        log.error(
          Map(
            "@msg" -> "failed to find aggregate reference for algorithm id from message - returning dead letter actor",
            "model" -> model.toString,
            "algorithm-id" -> algorithmIdFor( message ),
            "message-class" -> message.getClass.getName
          )
        )
        model.system.deadLetters
      }
    }
  }

  case class ShardedRoute(
    plan: AnalysisPlan.Summary,
    algorithmRootType: AggregateRootType,
    strategy: Strategy,
    implicit val model: DomainModel
  ) extends AlgorithmRoute {
    implicit val scIdentifying: Identifying[ShardCatalog.ID] = strategy.identifying
    val shardingId: ShardCatalog#TID = strategy.idFor( plan, algorithmRootType.name )
    val shardingRef = strategy.actorFor( plan, algorithmRootType )( model )

    override def forward( message: Any )( implicit sender: ActorRef, context: ActorContext ): Unit = {
      referenceFor( message ) forwardEnvelope ShardProtocol.RouteMessage( shardingId, message )
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

  object ShardedRoute {
    sealed trait Strategy {
      val rootType: AggregateRootType
      def key: String
      def makeAddCommand( plan: AnalysisPlan.Summary, algorithmRootType: AggregateRootType ): Option[Any]

      implicit lazy val identifying: Identifying[ShardCatalog.ID] = rootType.identifying.asInstanceOf[Identifying[ShardCatalog.ID]]

      def actorFor(
        plan: AnalysisPlan.Summary,
        algorithmRootType: AggregateRootType
      )(
        implicit model: DomainModel
      ): ActorRef = {
        val sid = idFor( plan, algorithmRootType.name )
        val ref = model( rootType, sid )
        val add = makeAddCommand( plan, algorithmRootType )
        add foreach { ref !+ _ }
        ref
      }

      def nextAlgorithmId( plan: AnalysisPlan.Summary, algorithmRootType: AggregateRootType ): () => AlgorithmModule.TID = { () =>
        algorithmRootType.identifying.tag(
          AlgorithmIdentifier(
            planName = plan.name,
            planId = plan.id.id.toString(),
            spanType = AlgorithmIdentifier.GroupSpan,
            span = ShortUUID().toString()
          ).asInstanceOf[algorithmRootType.identifying.ID]
        ).asInstanceOf[AlgorithmModule.TID]
      }

      def idFor(
        plan: AnalysisPlan.Summary,
        algorithmLabel: String
      )(
        implicit identifying: Identifying[ShardCatalog#ID]
      ): ShardCatalog#TID = {
        identifying.tag( ShardCatalog.ID( plan.id, algorithmLabel ).asInstanceOf[identifying.ID] ).asInstanceOf[ShardCatalog#TID]
      }
    }

    object Strategy extends ClassLogging {
      val PlanShardPath = "shard"

      type ShardingFactory = Config => Strategy

      private val DefaultFactory: ShardingFactory = CellStrategy.make
      private def DefaultStrategy: Strategy = DefaultFactory( ConfigFactory.empty )

      private val strategyFactories: Map[String, ShardingFactory] = Map(
        CellStrategy.key -> CellStrategy.make,
        LookupStrategy.key -> LookupStrategy.make
      )

      def from( plan: AnalysisPlan.Summary )( implicit model: DomainModel ): Option[Strategy] = {
        import scala.collection.JavaConversions._
        import shapeless.syntax.typeable._

        for {
          v <- Settings.detectionPlansConfigFrom( model.configuration ).root.find{ _._1 == plan.name }.map{ _._2 }
          co <- v.cast[ConfigObject]
          c = co.toConfig if c hasPath PlanShardPath
          shardValue = c getValue PlanShardPath
        } yield {
          val DefaultFactory = CellStrategy.make

          shardValue.valueType match {
            case ConfigValueType.STRING => {
              shardValue
              .unwrapped.cast[String]
              .map { key => makeStrategy( key ) }
              .getOrElse{ DefaultStrategy }
            }

            case ConfigValueType.OBJECT => {
              import scala.reflect._
              import scala.collection.JavaConversions._
              val ConfigObjectType = classTag[ConfigObject]
              val specs = c.getConfig( PlanShardPath ).root

              if ( specs.size == 1 ) {
                val ( key, ConfigObjectType(value)) = specs.head
                makeStrategy( key, value.toConfig )
              } else {
                val ex = new IllegalStateException(
                  s"too many shard declarations [${specs.size}] for plan. refer to error log for details"
                )

                log.error(
                  Map(
                    "@msg" -> "too many shard declarations for plan",
                    "plan" -> plan.name,
                    "origin" -> shardValue.origin().toString
                  ),
                  ex
                )

                throw ex
              }
            }

            case t => {
              val ex = new IllegalStateException( s"invalid shard specification: [${t}" )

              log.error(
                Map(
                  "@msg" -> "invalid shard specification - use either string or configuration object",
                  "plan" -> plan.name,
                  "origin" -> shardValue.origin().toString
                )
              )

              throw ex
            }
          }
        }
      }

      def makeStrategy( key: String, spec: Config = ConfigFactory.empty ): Strategy = {
        strategyFactories.getOrElse( key, DefaultFactory ) apply spec
      }
    }

    object CellStrategy {
      val key: String = "cell"

      val SizePath = "size"
      val DefaultSize: Int = 3000

      val make: (Config) => Strategy = { c =>
        val sz = if ( c hasPath SizePath ) c getInt SizePath else DefaultSize
        CellStrategy( nrCells = sz )
      }
    }

    case class CellStrategy( nrCells: Int ) extends Strategy {
      override def key: String = CellStrategy.key
      override val rootType: AggregateRootType = CellShardModule.module.rootType

      override def makeAddCommand( plan: AnalysisPlan.Summary, algorithmRootType: AggregateRootType ): Option[Any] = {
        Some(
          CellShardProtocol.Add(
            targetId = idFor(plan, algorithmRootType.name),
            plan,
            algorithmRootType,
            nrCells = nrCells,
            nextAlgorithmId(plan, algorithmRootType)
          )
        )
      }
    }

    object LookupStrategy {
      val key: String = "lookup"

      val ExpectedNrTopicsPath = "expected-topics-nr"
      val DefaultExpectedNrTopics: Int = 3000000

      val ControlBySizePath = "control-by-size"
      val DefaultControlBySize: Information = Megabytes( 3 )

      val make: (Config) => Strategy = { c =>
        val topics = if ( c hasPath ExpectedNrTopicsPath ) c getInt ExpectedNrTopicsPath else DefaultExpectedNrTopics
        val bySize = if ( c hasPath ControlBySizePath ) Bytes( c.getMemorySize(ControlBySizePath).toBytes ) else DefaultControlBySize
        LookupStrategy( expectedNrTopics = topics, bySize = bySize )
      }
    }

    case class LookupStrategy( expectedNrTopics: Int, bySize: Information ) extends Strategy {
      override def key: String = LookupStrategy.key
      override val rootType: AggregateRootType = LookupShardModule.rootType

      override def makeAddCommand( plan: AnalysisPlan.Summary, algorithmRootType: AggregateRootType ): Option[Any] = {
        Some(
          LookupShardProtocol.Add(
            targetId = idFor(plan, algorithmRootType.name),
            plan,
            algorithmRootType,
            expectedNrTopics,
            bySize,
            nextAlgorithmId( plan, algorithmRootType )
          )
        )
      }
    }
  }
}
