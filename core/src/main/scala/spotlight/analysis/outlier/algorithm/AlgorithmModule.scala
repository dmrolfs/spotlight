package spotlight.analysis.outlier.algorithm

import scala.annotation.tailrec
import scala.reflect._
import akka.actor.{ActorPath, Props}
import akka.cluster.sharding.ShardRegion
import akka.event.LoggingReceive

import scalaz._
import Scalaz._
import scalaz.Kleisli.{ask, kleisli}
import shapeless.{Lens, TypeCase}
import com.typesafe.config.Config
import com.typesafe.config.ConfigException.BadValue
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.ml.clustering.DoublePoint
import nl.grons.metrics.scala.{MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor
import peds.akka.publish.{EventPublisher, StackableStreamPublisher}
import peds.archetype.domain.model.core.{Entity, EntityIdentifying}
import peds.commons.{KOp, TryV}
import peds.commons.identifier.{Identifying, TaggedID}
import peds.commons.log.Trace
import demesne._
import demesne.repository.{AggregateRootRepository, EnvelopingAggregateRootRepository}
import demesne.repository.AggregateRootRepository.{ClusteredAggregateContext, LocalAggregateContext}
import spotlight.analysis.outlier._
import spotlight.model.outlier.{NoOutliers, OutlierPlan, Outliers}
import spotlight.model.timeseries._


object AlgorithmProtocol extends AggregateProtocol[AlgorithmModule.AnalysisState#ID] {
  sealed trait AlgorithmMessage
  abstract class AlgorithmCommand extends Command with AlgorithmMessage
  abstract class AlgorithmEvent extends Event with AlgorithmMessage

  import AlgorithmModule.AnalysisState

  case class UseConfiguration( override val targetId: UseConfiguration#TID, configuration: Config ) extends AlgorithmCommand
  case class ConfigurationChanged( override val sourceId: ConfigurationChanged#TID, configuration: Config ) extends AlgorithmEvent

  case class GetStateSnapshot( override val targetId: GetStateSnapshot#TID ) extends AlgorithmCommand
  case class StateSnapshot( sourceId: StateSnapshot#TID, snapshot: Option[AnalysisState] ) extends AlgorithmEvent

  case class Advanced(
    override val sourceId: Advanced#TID,
    point: DataPoint,
    isOutlier: Boolean,
    threshold: ThresholdBoundary
  ) extends AlgorithmEvent

  case class AlgorithmUsedBeforeRegistrationError(
    sourceId: AnalysisState#TID,
    algorithm: Symbol,
    path: ActorPath
  ) extends IllegalStateException(
    s"actor [${path}] not registered algorithm [${algorithm.name}] with scope [${sourceId}] before use"
  ) with OutlierAlgorithmError
}


/**
  * Created by rolfsd on 6/5/16.
  */
object AlgorithmModule {
  trait StrictSelf[T <: StrictSelf[T]] { self: T =>
    type Self >: self.type <: T
  }

  type ID = OutlierPlan.Scope

  trait AnalysisState extends Entity with Equals { self: StrictSelf[_] =>
    override type ID = AlgorithmModule.ID

    override val evID: ClassTag[ID] = classTag[AlgorithmModule.ID]
    override val evTID: ClassTag[TID] = classTag[TaggedID[AlgorithmModule.ID]]

    def scope: OutlierPlan.Scope = id
    def algorithm: Symbol
    def topic: Topic = scope.topic

    def withConfiguration( configuration: Config ): Option[Self] = None

//    def thresholds: Seq[ThresholdBoundary]
//    def addThreshold( threshold: ThresholdBoundary ): Self

    override def hashCode: Int = {
      41 * (
        41 * (
          41 * (
            41 + id.id.##
          ) + name.##
        ) + topic.##
      ) + algorithm.##
    }

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: AnalysisState => {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.id.id == that.id.id ) &&
            ( this.name == that.name) &&
            ( this.topic == that.topic) &&
            ( this.algorithm == that.algorithm )
          }
        }

        case _ => false
      }
    }
  }


  implicit val identifying: EntityIdentifying[AnalysisState] = {
    new EntityIdentifying[AnalysisState] with OutlierPlan.Scope.ScopeIdentifying[AnalysisState] {
      override lazy val idTag: Symbol = 'algorithm
      override val evEntity: ClassTag[AnalysisState] = classTag[AnalysisState]
    }
  }



  /**
    *  approximate number of points in a one day window size @ 1 pt per 10s
    */
  val ApproximateDayWindow: Int = 6 * 60 * 24


  trait ModuleConfiguration {
    def maximumNrClusterNodes: Int = 6
  }


  case class InsufficientDataSize(
    algorithm: Symbol,
    size: Long,
    required: Long
  ) extends IllegalArgumentException(
    s"${size} data points is insufficient to perform ${algorithm.name} test, which requires at least ${required} points"
  )


  case class InvalidAlgorithmConfiguration(
    algorithm: Symbol,
    path: String,
    requirement: String
  ) extends BadValue( path, s"For algorithm, ${algorithm.name}, ${path} must: ${requirement}" )
}

abstract class AlgorithmModule extends AggregateRootModule { module: AlgorithmModule.ModuleConfiguration =>
  private val trace = Trace[AlgorithmModule]


  type State <: AlgorithmModule.AnalysisState

  /**
    * Shape represents the culminated value of applying the algorithm over the time series data for this ID.
    */
  type Shape

  implicit def evState: ClassTag[State]

  val analysisStateCompanion: AnalysisStateCompanion
  trait AnalysisStateCompanion {
    def zero( id: State#TID ): State
    def shapeLens: Lens[State, Shape]
    //todo: require algo to define?    def makeShape(): Shape
    def advanceShape( shape: Shape, advanced: AlgorithmProtocol.Advanced ): Shape
  }


  def algorithm: Algorithm
  override lazy val aggregateIdTag: Symbol = algorithm.label
  override lazy val shardName: String = algorithm.label.name


  trait Algorithm {
    val label: Symbol
    def prepareContext( algorithmContext: Context ): Context = identity( algorithmContext )
    def prepareData( algorithmContext: Context ): Seq[DoublePoint]
    def step( point: PointT )( implicit state: State, algorithmContext: Context ): Option[(Boolean, ThresholdBoundary)]
  }


  type Context <: AlgorithmContext
  def makeContext( message: DetectUsing, state: Option[State] ): Context

  trait AlgorithmContext extends LazyLogging {
    def message: DetectUsing

    def plan: OutlierPlan = message.payload.plan

    def tolerance: Double

    def recent: RecentHistory

    def source: TimeSeriesBase = message.source

    def data: Seq[DoublePoint]

    def fillData( minimalSize: Int = RecentHistory.LastN ): (Seq[DoublePoint]) => Seq[DoublePoint] = { original =>
      if ( minimalSize <= original.size ) original
      else {
        val historicalSize = recent.points.size
        val needed = minimalSize + 1 - original.size
        val historical = recent.points.drop( historicalSize - needed )
        historical.toDoublePoints ++ original
      }
    }

    def tailAverage(
      tailLength: Int = AlgorithmContext.DefaultTailAverageLength
    ): (Seq[DoublePoint]) => Seq[DoublePoint] = { points =>
      val values = points map { _.value }
      val lastPos = {
        points.headOption
        .map { h => recent.points indexWhere { _.timestamp == h.timestamp } }
        .getOrElse { recent.points.size }
      }

      val last = recent.points.drop( lastPos - tailLength + 1 ) map { _.value }
      logger.debug( "tail-average: last[{}]", last.mkString(",") )

      points
      .map { _.timestamp }
      .zipWithIndex
      .map { case (ts, i) =>
        val pointsToAverage = {
          if ( i < tailLength ) {
            val all = last ++ values.take( i + 1 )
            all.drop( all.size - tailLength )
          } else {
            values.slice( i - tailLength + 1, i + 1 )
          }
        }

        ( ts, pointsToAverage )
      }
      .map { case (ts, pts) =>
        val average = pts.sum / pts.size
        logger.debug( "points to tail average ({}, [{}]) = {}", ts.toLong.toString, pts.mkString(","), average.toString )
        ( ts, average ).toDoublePoint
      }
    }
  }

  object AlgorithmContext {
    val TolerancePath = "tolerance"
    val DefaultTailAverageLength: Int = 3
  }


  class CommonContext( override val message: DetectUsing ) extends AlgorithmContext {
    override def data: Seq[DoublePoint] = message.payload.source.points
    override def recent: RecentHistory = message.recent
    override def tolerance: Double = {
      import AlgorithmContext.TolerancePath
      if ( message.properties hasPath TolerancePath ) message.properties.getDouble( TolerancePath ) else 3.0
    }
  }


  override type ID = AlgorithmModule.ID
  val IdType = TypeCase[TID]
  override def nextId: TryV[TID] = AlgorithmModule.identifying.nextIdAs[TID]
  val AdvancedType = TypeCase[AlgorithmProtocol.Advanced]


  override lazy val rootType: AggregateRootType = new RootType

//  def toCamelCase( name: String ): String = {
//    val regex = """-(\w)""".r
//    val result = regex.replaceAllIn( name, m => m.subgroups.head.toUpperCase )
//    result.head.toUpper + result.tail
//  }

  class RootType extends AggregateRootType {
    override lazy val name: String = module.shardName
    override lazy val identifying: Identifying[_] = AlgorithmModule.identifying
    override def repositoryProps( implicit model: DomainModel ): Props = Repository localProps model //todo change to clustered with multi-jvm testing of cluster
    override def maximumNrClusterNodes: Int = module.maximumNrClusterNodes
    override def aggregateIdFor: ShardRegion.ExtractEntityId = super.aggregateIdFor orElse {
      case AdvancedType( a ) => {
        logger.debug( "TEST: aggregateIdFor( {} ) = [{}]", a, a.sourceId.id.toString )
        (a.sourceId.id.toString, a )
      }
    }
    override def toString: String = "Algorithm:"+name
  }


  object Repository {
    def localProps( model: DomainModel ): Props = Props( new LocalRepository(model) )
    def clusteredProps( model: DomainModel ): Props = Props( new ClusteredRepository(model) )
  }

  class LocalRepository( model: DomainModel ) extends Repository( model ) with LocalAggregateContext
  class ClusteredRepository( model: DomainModel ) extends Repository( model ) with ClusteredAggregateContext

  class Repository( model: DomainModel )
  extends EnvelopingAggregateRootRepository( model, module.rootType ) { actor: AggregateRootRepository.AggregateContext =>
    override def aggregateProps: Props = AlgorithmActor.props( model, rootType )

    //todo    import demesne.repository.{ StartProtocol => SP }
//    override def doLoad(): Loaded = super.doLoad()
//    override def doInitialize( resources: Map[Symbol, Any] ): Valid[Done] = super.doInitialize( resources )
  }


  object AlgorithmActor {
    def props( model: DomainModel, rootType: AggregateRootType ): Props = Props( new Default( model, rootType ) )

    private class Default( model: DomainModel, rootType: AggregateRootType )
    extends AlgorithmActor(model, rootType) with StackableStreamPublisher
  }

  class AlgorithmActor( override val model: DomainModel, override val rootType: AggregateRootType )
  extends AggregateRoot[State, ID]
  with AggregateRoot.Provider
  with InstrumentedActor {
    publisher: EventPublisher =>

    override def parseId( idstr: String ): TID = {
      val identifying = AlgorithmModule.identifying
      identifying.safeParseId( idstr )( identifying.evID )
    }

    override lazy val metricBaseName: MetricName = MetricName( "spotlight.analysis.outlier.algorithm" )
    lazy val algorithmTimer: Timer = metrics timer algorithm.label.name

    override var state: State = _
    override lazy val evState: ClassTag[State] = module.evState

    var algorithmContext: Context = _

    import analysisStateCompanion.{ shapeLens, advanceShape }
//    val advanceLens: Lens[State, (analysisStateCompanion.Shape, Seq[ThresholdBoundary])] = shapeLens ~ thresholdLens
    val advanceLens: Lens[State, Shape] = shapeLens

    import AlgorithmProtocol.ConfigurationChanged

    override val acceptance: Acceptance = {
      case ( AdvancedType(event), s ) => {
        log.debug( "TEST:[{}]: accepting Advanced:[{}]", self.path, event )
        val currentState = Option(s) getOrElse {
          log.debug( "AlgorithmModule[{}]: processed first data. creating initial state", self.path )
          analysisStateCompanion zero aggregateId
        }
//        val result = advanceLens.modify( currentState ){ case (h, ts) => (updateShape( h, event ), ts :+ event.threshold ) }
        val newState = advanceLens.modify( currentState ){ case shape => advanceShape(shape, event) }
        log.debug( "TEST:[{}]: resultingState=[{}] aggregateId:[{}]", self.path, newState, aggregateId )
        newState
      }

      case ( event: ConfigurationChanged, s ) => {
        val currentState = Option(s) getOrElse { analysisStateCompanion zero aggregateId }
        currentState.withConfiguration( event.configuration ) map { _.asInstanceOf[State] } getOrElse s
      }
    }

    override def receiveCommand: Receive = LoggingReceive { around( active orElse stateReceiver ) }

    val active: Receive = {
      case msg @ DetectUsing( algo, payload: DetectOutliersInSeries, history, algorithmConfig ) => {
        val aggregator = sender()
        val toOutliers = kleisli[TryV, (Outliers, Context), Outliers] { case (o, _) => o.right }

        val start = System.currentTimeMillis()
        ( makeAlgorithmContext >=> findOutliers >=> toOutliers ).run( msg ) match {
          case \/-( r ) => {
            log.debug( "[{}] sending detect result to aggregator[{}]: [{}]", workId, aggregator.path.name, r )
            algorithmTimer.update( System.currentTimeMillis() - start, scala.concurrent.duration.MILLISECONDS )
            aggregator !+ r
          }

          case -\/( ex: AlgorithmModule.InsufficientDataSize ) => {
            log.error(
              ex,
              "[{}] skipped [{}] analysis on [{}] @ [{}] due to insufficient data - no outliers marked for interval",
              workId,
              algo.name,
              payload.plan.name + "][" + payload.topic,
              payload.source.interval
            )

            // don't let aggregator time out just due to error in algorithm
            aggregator !+ NoOutliers(
              algorithms = Set(algorithm.label),
              source = payload.source,
              plan = payload.plan,
              thresholdBoundaries = Map.empty[Symbol, Seq[ThresholdBoundary]]
            )
          }

          case -\/( ex ) => {
            log.error(
              ex,
              "[{}] failed [{}] analysis on [{}] @ [{}] - no outliers marked for interval",
              workId,
              algo.name,
              payload.plan.name + "][" + payload.topic,
              payload.source.interval
            )
            // don't let aggregator time out just due to error in algorithm
            aggregator !+ NoOutliers(
              algorithms = Set(algorithm.label),
              source = payload.source,
              plan = payload.plan,
              thresholdBoundaries = Map.empty[Symbol, Seq[ThresholdBoundary]]
            )
          }
        }
      }

      case AdvancedType( adv ) => {
        log.debug( "RECEIVE ADAANCED - TEST:[{}]: Algorithm[{}] HANDLING Advanced msg: [{}]", self.path, algorithm.label.name, adv )
        persist( adv ) { accept }
      }
    }

    import AlgorithmProtocol.{ UseConfiguration, ConfigurationChanged, GetStateSnapshot, StateSnapshot }

    val stateReceiver: Receive = {
      case UseConfiguration( _, config ) => {
        if ( Option(state).fold( true )( _.withConfiguration(config).isDefined ) ) {
          persist( ConfigurationChanged(aggregateId, config) ) { accept }
        }
      }

      case _: GetStateSnapshot => {
        val snapshot = StateSnapshot( aggregateId, Option(state) )
        log.debug( "TEST:[{}]: Algorithm[{}] returning state snapshot: [{}]", self.path, algorithm.label.name, snapshot )
        sender() ! snapshot
      }
    }


    override def unhandled( message: Any ): Unit = {
      message match {
        case m: DetectUsing if Option(state).isDefined && m.algorithm == state.algorithm && m.scope == state.id => {
          val ex = AlgorithmProtocol.AlgorithmUsedBeforeRegistrationError( aggregateId, algorithm.label, self.path )
          log.error( ex, "algorithm actor [{}] not registered for scope:[{}]", algorithm.label, aggregateId )
        }

        case m: DetectUsing => {
          log.error( "algorithm [{}] does not recognize requested payload: [{}]", algorithm, m )
          sender() !+ UnrecognizedPayload( algorithm.label, m )
        }

        case m => super.unhandled( m )
      }
    }

    // -- algorithm functional elements --
    val makeAlgorithmContext: KOp[DetectUsing, Context] = kleisli[TryV, DetectUsing, Context] { message =>
      \/ fromTryCatchNonFatal {
        algorithmContext = module.makeContext( message, Option( state ) )
        algorithmContext
      }
    }

    def findOutliers: KOp[Context, (Outliers, Context)] = {
      for {
        ctx <- ask[TryV, Context]
        data <- kleisli[TryV, Context, Seq[DoublePoint]] { c => \/ fromTryCatchNonFatal { algorithm prepareData c } }
        events <- collectOutlierPoints( data )
      _ = log.debug( "TEST: findOutliers: events-isOutliers:[{}]", events.map{ _.isOutlier }.mkString(", ") )
        o <- makeOutliers( events )
      _ = log.debug( "TEST: findOutliers: outliers:[{}]", o )
      } yield ( o, ctx )
    }

    private def collectOutlierPoints( points: Seq[DoublePoint] ): KOp[Context, Seq[AlgorithmProtocol.Advanced]] = {
      kleisli[TryV, Context, Seq[AlgorithmProtocol.Advanced]] { implicit analysisContext =>
        def tryStep( pt: PointT )( implicit s: State ): TryV[(Boolean, ThresholdBoundary)] = {
          \/ fromTryCatchNonFatal {
            algorithm.step( pt )
            .getOrElse {
              logger.debug( "skipping point[{}] per insufficient history for algorithm {}", pt, algorithm.label )
              ( false, ThresholdBoundary empty pt.timestamp.toLong )
            }
          }
        }


        @tailrec def loop(
          points: List[DoublePoint],
          accumlated: TryV[Seq[AlgorithmProtocol.Advanced]]
        )(
          implicit loopState: TryV[State]
        ): TryV[Seq[AlgorithmProtocol.Advanced]] = {
          points match {
            case Nil => accumlated

            case pt :: tail => {
              val updatedAccAndState: TryV[(Seq[AlgorithmProtocol.Advanced], State)] = {
                for {
                  acc <- accumlated
                  ls <- loopState
                  ot <- tryStep( pt )( ls )
                  (isOutlier, threshold) = ot
                } yield {
                  analysisContext.data
                  .find { _.timestamp == pt.timestamp }
                  .map { original =>
                    log.debug( "PT:[{}] ORIGINAL:[{}]", (pt._1.toLong, pt._2), (original._1.toLong, original._2) )
                    val event = AlgorithmProtocol.Advanced(
                      sourceId = aggregateId,
                      point = original.toDataPoint,
                      isOutlier = isOutlier,
                      threshold = threshold
                    )

                    log.debug(
                      "LOOP-{}[{}]: AnalysisState.Advanced:[{}]",
                      if ( isOutlier ) "HIT" else "MISS",
                      (pt._1.toLong, pt._2),
                      event
                    )

                    ( acc :+ event, acceptance(event, ls) )
                  }
                  .getOrElse {
                    log.debug( "NOT ORIGINAL PT:[{}]", (pt._1.toLong, pt._2) )
                    ( acc, ls )
                  }
                }
              }

              val newAcc = updatedAccAndState map { _._1 }
              val newState = updatedAccAndState map { _._2 }
              loop( tail, newAcc )( newState )
            }
          }
        }

        val effState = Option( state ) getOrElse { analysisStateCompanion zero aggregateId }
        val events = loop( points.toList, Seq.empty[AlgorithmProtocol.Advanced].right )( effState.right )
        events foreach { _ foreach { e => persist( e ){ accept } } }
        events
      }
    }

    private def makeOutliers( events: Seq[AlgorithmProtocol.Advanced] ): KOp[Context, Outliers] = {
      kleisli[TryV, Context, Outliers] { ctx =>
        val outliers = events collect { case e if e.isOutlier => e.point }
        val thresholds = events map { _.threshold }

        Outliers.forSeries(
          algorithms = Set( algorithm.label ),
          plan = ctx.plan,
          source = ctx.source,
          outliers = outliers,
          thresholdBoundaries = Map( algorithm.label -> thresholds )
        )
        .disjunction
        .leftMap { _.head }
      }
    }
  }
}
