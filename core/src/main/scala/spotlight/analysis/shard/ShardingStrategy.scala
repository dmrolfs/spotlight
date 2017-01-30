package spotlight.analysis.shard

import akka.actor.ActorRef
import com.persist.logging._
import com.typesafe.config._
import demesne.{AggregateRootType, DomainModel}
import peds.commons.identifier.Identifying
import peds.akka.envelope._
import spotlight.Settings
import spotlight.model.outlier.AnalysisPlan
import spotlight.model.outlier.AnalysisPlan.Summary
import squants.information.{Bytes, Information, Megabytes}

/**
  * Created by rolfsd on 1/20/17.
  */
sealed trait ShardingStrategy {
  val rootType: AggregateRootType
  def key: String
  def makeAddCommand( plan: AnalysisPlan.Summary, algorithmRootType: AggregateRootType ): Option[Any]

  implicit lazy val identifying: Identifying[ShardCatalog.ID] = rootType.identifying.asInstanceOf[Identifying[ShardCatalog.ID]]

  def actorFor( plan: AnalysisPlan.Summary, algorithmRootType: AggregateRootType )( implicit model: DomainModel ): ActorRef = {
    val sid = idFor( plan, algorithmRootType.name )
    val ref = model( rootType, sid )
    val add = makeAddCommand( plan, algorithmRootType )
    add foreach { ref !+ _ }
    ref
  }

  def idFor( plan: AnalysisPlan.Summary, algorithmLabel: String )( implicit identifying: Identifying[ShardCatalog#ID] ): ShardCatalog#TID = {
    identifying.tag( ShardCatalog.ID( plan.id, algorithmLabel ).asInstanceOf[identifying.ID] ).asInstanceOf[ShardCatalog#TID]
  }
}

object ShardingStrategy extends ClassLogging {
  val PlanShardPath = "shard"

  type ShardingFactory = Config => ShardingStrategy

  private val DefaultFactory: ShardingFactory = CellShardingStrategy.make
  private def DefaultStrategy: ShardingStrategy = DefaultFactory( ConfigFactory.empty )

  private val strategyFactories: Map[String, ShardingFactory] = Map(
    CellShardingStrategy.key -> CellShardingStrategy.make,
    LookupShardingStrategy.key -> LookupShardingStrategy.make
  )

  def from( plan: AnalysisPlan.Summary )( implicit model: DomainModel ): Option[ShardingStrategy] = {
    import scala.collection.JavaConversions._
    import shapeless.syntax.typeable._

    for {
      v <- Settings.detectionPlansConfigFrom( model.configuration ).root.find{ _._1 == plan.name }.map{ _._2 }
      co <- v.cast[ConfigObject]
      c = co.toConfig if c hasPath PlanShardPath
      shardValue = c getValue PlanShardPath
    } yield {
      val DefaultFactory = CellShardingStrategy.make

      shardValue.valueType match {
        case ConfigValueType.STRING => {
          shardValue
          .unwrapped.cast[String]
          .map { key => makeShardingStrategy( key ) }
          .getOrElse{ DefaultStrategy }
        }

        case ConfigValueType.OBJECT => {
          import scala.reflect._
          import scala.collection.JavaConversions._
          val ConfigObjectType = classTag[ConfigObject]
          val specs = c.getConfig( PlanShardPath ).root

          if ( specs.size == 1 ) {
            val ( key, ConfigObjectType(value)) = specs.head
            makeShardingStrategy( key, value.toConfig )
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

  def makeShardingStrategy( key: String, spec: Config = ConfigFactory.empty ): ShardingStrategy = {
    strategyFactories.getOrElse( key, DefaultFactory ) apply spec
  }
}

object CellShardingStrategy {
  val key: String = "cell"

  val SizePath = "size"
  val DefaultSize: Int = 3000

  val make: (Config) => ShardingStrategy = { c =>
    val sz = if ( c hasPath SizePath ) c getInt SizePath else DefaultSize
    CellShardingStrategy( nrCells = sz )
  }
}

case class CellShardingStrategy( nrCells: Int ) extends ShardingStrategy {
  override def key: String = CellShardingStrategy.key
  override val rootType: AggregateRootType = CellShardModule.module.rootType

  override def makeAddCommand( plan: Summary, algorithmRootType: AggregateRootType ): Option[Any] = {
    Some( CellShardProtocol.Add( targetId = idFor(plan, algorithmRootType.name), plan, algorithmRootType, nrCells = 5000 ) )
  }
}

object LookupShardingStrategy {
  val key: String = "lookup"

  val ExpectedNrTopicsPath = "expected-topics-nr"
  val DefaultExpectedNrTopics: Int = 3000000

  val ControlBySizePath = "control-by-size"
  val DefaultControlBySize: Information = Megabytes( 3 )

  val make: (Config) => ShardingStrategy = { c =>
    val topics = if ( c hasPath ExpectedNrTopicsPath ) c getInt ExpectedNrTopicsPath else DefaultExpectedNrTopics
    val bySize = if ( c hasPath ControlBySizePath ) Bytes( c.getMemorySize(ControlBySizePath).toBytes ) else DefaultControlBySize
    LookupShardingStrategy( expectedNrTopics = topics, bySize = bySize )
  }
}

case class LookupShardingStrategy( expectedNrTopics: Int, bySize: Information ) extends ShardingStrategy {
  override def key: String = LookupShardingStrategy.key
  override val rootType: AggregateRootType = LookupShardModule.rootType

  override def makeAddCommand( plan: Summary, algorithmRootType: AggregateRootType ): Option[Any] = {
    Some(
      LookupShardProtocol.Add( targetId = idFor(plan, algorithmRootType.name), plan, algorithmRootType, expectedNrTopics, bySize )
    )
  }
}
