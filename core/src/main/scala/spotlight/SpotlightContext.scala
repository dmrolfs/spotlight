package spotlight

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import akka.actor.{ ActorSystem, Terminated }
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.persist.logging._
import com.persist.logging.LoggingLevels.{ DEBUG, Level }
import net.ceedubs.ficus.Ficus._
import demesne.{ AggregateRootType, StartTask }
import omnibus.commons.Valid
import omnibus.commons.builder.HasBuilder
import shapeless.{ Generic, HNil }

/** Created by rolfsd on 3/24/17.
  */
sealed abstract class SpotlightContext {
  def name: String
  def system: ActorSystem
  def settings: Settings
  def rootTypes: Set[AggregateRootType]
  def startTasks: Set[StartTask]
  def resources: Map[Symbol, Any]
  def timeout: Timeout
  def terminate()( implicit ec: ExecutionContext ): Future[Terminated]
}

object SpotlightContext extends ClassLogging {
  val SystemName = "Spotlight"
  val SystemLogCategory = "system"

  object Builder extends HasBuilder[SimpleSpotlightContext] {
    object Name extends OptParam[String]( SystemName )
    object System extends OptParam[Option[ActorSystem]]( None )
    object RootTypes extends OptParam[Set[AggregateRootType]]( Set.empty[AggregateRootType] )
    object Resources extends OptParam[Map[Symbol, Any]]( Map.empty[Symbol, Any] )
    object StartTasks extends OptParam[Set[StartTask]]( Set.empty[StartTask] )
    object Timeout extends OptParam[Timeout]( 30.seconds )
    object Arguments extends OptParam[Array[String]]( Array.empty[String] )

    // Establish HList <=> SpotlightContext isomorphism
    val gen = Generic[SimpleSpotlightContext]
    // Establish Param[_] <=> constructor parameter correspondence
    override val fieldsContainer = createFieldsContainer(
      Name ::
        System ::
        RootTypes ::
        StartTasks ::
        Resources ::
        Timeout ::
        Arguments ::
        HNil
    )
  }

  final case class SimpleSpotlightContext private[SpotlightContext] (
      override val name: String = Settings.ActorSystemName,
      applicationSystem: Option[ActorSystem],
      override val rootTypes: Set[AggregateRootType],
      override val startTasks: Set[StartTask],
      override val resources: Map[Symbol, Any],
      override val timeout: Timeout,
      applicationArguments: Array[String]
  ) extends SpotlightContext {
    @transient override lazy val system: ActorSystem = applicationSystem getOrElse ActorSystem( name, settings.config )

    @transient override val settings: Settings = {
      val spotlightConfig: String = {
        Option( java.lang.System.getProperty( "config.resource" ) )
          .orElse { Option( java.lang.System.getProperty( "config.file" ) ) }
          .orElse { Option( java.lang.System.getProperty( "config.url" ) ) }
          .getOrElse { "application.conf" }
      }

      log.alternative(
        SystemLogCategory,
        Map(
          "@msg" → "spotlight config",
          "config" → spotlightConfig.toString,
          "url" → scala.util.Try { Thread.currentThread.getContextClassLoader.getResource( spotlightConfig ) }
        )
      )

      Valid.unsafeGet( Settings( applicationArguments, systemName = name, config = ConfigFactory.load() ) )
    }

    override def terminate()( implicit ec: ExecutionContext ): Future[Terminated] = {
      for {
        _ ← _loggingSystem map { _.stop } getOrElse { Future.successful( () ) }
        t ← system.terminate()
      } yield t
    }

    @transient var _loggingSystem: Option[LoggingSystem] = None
    def startLogging( s: ActorSystem ): Unit = {
      _loggingSystem = Option(
        LoggingSystem(
          system = s,
          serviceName = spotlight.BuildInfo.name,
          serviceVersion = spotlight.BuildInfo.version,
          host = java.net.InetAddress.getLocalHost.getHostName
        )
      )

      _loggingSystem foreach { ls ⇒ activateLoggingFilter( ls, s ) }
    }

    private def activateLoggingFilter( loggingSystem: LoggingSystem, s: ActorSystem ): Unit = {
      val systemConfig = s.settings.config

      for {
        isActive ← systemConfig.as[Option[Boolean]]( "spotlight.logging.filter.active" ) if isActive == true
      } {
        val watched = {
          systemConfig
            .as[Option[Set[String]]]( "spotlight.logging.filter.include-classname-segments" )
            .getOrElse { Set.empty[String] }
        }

        log.warn(
          Map(
            "@msg" → "logging started",
            "loglevel" → loggingSystem.logLevel.toString,
            "log-debug-for" → watched.mkString( "[", ", ", "]" )
          )
        )

        if ( watched.nonEmpty ) {
          val loggingLevel = loggingSystem.logLevel
          def inWatched( fqn: String ): Boolean = watched exists { fqn.contains }

          def filter( fields: Map[String, RichMsg], level: Level ): Boolean = {
            fields
              .get( "class" )
              .collect { case fqn: String if inWatched( fqn ) ⇒ level >= DEBUG }
              .getOrElse { level >= loggingLevel }
          }

          loggingSystem.setFilter( Some( filter ) )
          loggingSystem.setLevel( DEBUG )
        }
      }
    }
  }
}
