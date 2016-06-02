package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.NotUsed
import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.pattern.ask
import akka.event.LoggingReceive
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import demesne.{AggregateRootType, DomainModel}
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream._
import peds.commons.util._
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.analysis.outlier.algorithm.AlgorithmActor
import spotlight.analysis.outlier.algorithm.density.{CohortDensityAnalyzer, SeriesCentroidDensityAnalyzer, SeriesDensityAnalyzer}
import spotlight.analysis.outlier.algorithm.skyline._
import spotlight.model.outlier.OutlierPlan.Scope
import spotlight.model.outlier.{OutlierPlan, Outliers}
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.model.timeseries.{TimeSeries, TimeSeriesBase, Topic}


/**
  * Created by rolfsd on 5/26/16.
  */
object AnalysisScopeProxy {
  def props(
    scope: OutlierPlan.Scope,
    plan: OutlierPlan,
    model: DomainModel,
    highWatermark: Int,
    bufferSize: Int
  ): Props = {
    val s = scope
    val p = plan
    val m = model
    val hwm = highWatermark
    val bs = bufferSize

    Props(
      new AnalysisScopeProxy with Provider {
        override val scope: Scope = s
        override val plan: OutlierPlan = p
        override val model: DomainModel = m
        override val highWatermark: Int = hwm
        override val bufferSize: Int = bs
        override def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType] = {
          algorithm match {
            case SeriesDensityAnalyzer.Algorithm => ??? // SeriesDensityAnalyzer.props( router ),
            case SeriesCentroidDensityAnalyzer.Algorithm => ??? // SeriesCentroidDensityAnalyzer.props( router ),
            case CohortDensityAnalyzer.Algorithm => ??? // CohortDensityAnalyzer.props( router ),
            case ExponentialMovingAverageAnalyzer.Algorithm => ??? // ExponentialMovingAverageAnalyzer.props( router ),
            case FirstHourAverageAnalyzer.Algorithm => ??? // FirstHourAverageAnalyzer.props( router ),
            case GrubbsAnalyzer.Algorithm => ??? // GrubbsAnalyzer.props( router ),
            case HistogramBinsAnalyzer.Algorithm => ??? // HistogramBinsAnalyzer.props( router ),
            case KolmogorovSmirnovAnalyzer.Algorithm => ??? // KolmogorovSmirnovAnalyzer.props( router ),
            case LeastSquaresAnalyzer.Algorithm => ??? // LeastSquaresAnalyzer.props( router ),
            case MeanSubtractionCumulationAnalyzer.Algorithm => ??? // MeanSubtractionCumulationAnalyzer.props( router ),
            case MedianAbsoluteDeviationAnalyzer.Algorithm => ??? // MedianAbsoluteDeviationAnalyzer.props( router ),
            case SimpleMovingAverageAnalyzer.Algorithm => ??? // SimpleMovingAverageAnalyzer.props( router ),
            case SeasonalExponentialMovingAverageAnalyzer.Algorithm => ??? // SeasonalExponentialMovingAverageAnalyzer.props( router )
            case _ => None
          }
        }
      }
    )
  }


  trait Provider {
    def scope: OutlierPlan.Scope
    def plan: OutlierPlan
    def model: DomainModel
    def highWatermark: Int
    def bufferSize: Int
    def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType]
  }
}

class AnalysisScopeProxy extends Actor with InstrumentedActor with ActorLogging { outer: AnalysisScopeProxy.Provider =>
  import OutlierPlan.Scope

  override lazy val metricBaseName: MetricName = MetricName( classOf[AnalysisScopeProxy] )
  val receiveTimer: Timer = metrics.timer( "receive" )
  val failuresMeter: Meter = metrics.meter( "failures" )

  case class Workers private( detector: ActorRef, router: ActorRef, algorithms: Set[ActorRef] )
  //todo: rework to avoid Await. Left here since exposure is limited to first sight of Plan Scope data
  var workers: Workers = scala.concurrent.Await.result( makeTopicWorkers(outer.scope.topic)(context.dispatcher), 15.seconds )

  case class PlanStream private( ingressRef: ActorRef, graph: RunnableGraph[NotUsed] )
  var streams: Map[ActorRef, PlanStream] = Map.empty[ActorRef, PlanStream]


  val streamSupervision: Supervision.Decider = {
    case ex => {
      log.error( ex, "Error caught by analysis scope proxy supervisor" )
      failuresMeter.mark()
      Supervision.Restart
    }
  }

  implicit val system: ActorSystem = context.system
  implicit val materializer: Materializer = {
    ActorMaterializer( ActorMaterializerSettings(system) withSupervisionStrategy streamSupervision )
  }

  override def receive: Receive = LoggingReceive { around( workflow ) }

  val workflow: Receive = {
    case (ts: TimeSeries, s: OutlierPlan.Scope) if s == outer.scope => streamIngressFor( sender() ) forward ts
    case (ts: TimeSeries, p: OutlierPlan) if Scope(p, ts.topic) == outer.scope => streamIngressFor( sender() ) forward ts
  }

  def streamIngressFor( subscriber: ActorRef )( implicit system: ActorSystem, materializer: Materializer ): ActorRef = {
    val ingress = {
      streams
      .get( subscriber )
      .map { _.ingressRef }
      .getOrElse {
        val ps = makePlanStream( subscriber )
        streams += ( subscriber -> ps )
        ps.ingressRef
      }
    }

    log.debug( "AnalysisScopeProxy: [{} {}] ingress = {}", plan.name, subscriber, ingress )
    ingress
  }

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy( maxNrOfRetries = -1 ) {
    case ex: ActorInitializationException => {
      log.error( ex, "{} stopping actor on caught initialization error", classOf[AnalysisScopeProxy].safeSimpleName )
      failuresMeter.mark()
      Stop
    }

    case ex: ActorKilledException => {
      log.error(
        ex,
        "{} stopping killed actor on caught actor killed exception",
        classOf[AnalysisScopeProxy].safeSimpleName
      )
      failuresMeter.mark()
      Stop
    }

    case ex: InvalidActorNameException => {
      log.error( ex, "{} restarting on caught invalid actor name", classOf[AnalysisScopeProxy].safeSimpleName )
      failuresMeter.mark()
      Restart
    }

    case ex: Exception => {
      log.error( ex,  "{} restarting on caught exception from child", classOf[AnalysisScopeProxy].safeSimpleName )
      failuresMeter.mark()
      Restart
    }

    case ex => {
      log.error( ex,  "{} restarting on unknown error from child", classOf[AnalysisScopeProxy].safeSimpleName )
      failuresMeter.mark()
      Escalate
    }
  }


  def makeTopicWorkers( t: Topic )( implicit ec: ExecutionContext ): Future[Workers] = {
    def makeRouter(): ActorRef = {
      context.actorOf(
        DetectionAlgorithmRouter.props.withDispatcher( "outlier-detection-dispatcher" ),
        s"router-${outer.plan.name}"
      )
    }

    def makeAlgorithms( routerRef: ActorRef ): Future[Set[ActorRef]] = {
      val algs = outer.plan.algorithms
      .flatMap { a =>
        outer.rootTypeFor( a )
        .map { rt =>
          implicit val timeout = akka.util.Timeout( 15.seconds )
          val aref = outer.model.aggregateOf( rt, outer.scope.id )
          val reg = ( aref ? AlgorithmActor.Register( outer.scope.id, routerRef ) ).mapTo[AlgorithmActor.Registered]
          reg map { _ => aref }
        }
      }

      Future.sequence( algs )
    }

    def makeDetector( routerRef: ActorRef ): ActorRef = {
      context.actorOf(
        OutlierDetection.props( routerRef ).withDispatcher( "outlier-detection-dispatcher" ),
        s"outlier-detector-${outer.plan.name}"
      )
    }

    val router = makeRouter()

    makeAlgorithms( router ) map { algs =>
      Workers(
        router = router,
        detector = makeDetector( router ),
        algorithms = algs
      )
    }
  }

  def makePlanStream(
    subscriber: ActorRef
  )(
    implicit tsMerging: Merging[TimeSeries],
    system: ActorSystem,
    materializer: Materializer
  ): PlanStream = {
    log.info( "OutlierPlanDetectionRouter making new flow for plan-scope:subscriber: [{} : {}]", outer.scope, subscriber )

    val (ingressRef, ingress) = {
      Source
      .actorPublisher[TimeSeries]( StreamIngress.props[TimeSeries] )
      .toMat( Sink.asPublisher(false) )( Keep.both )
      .run()
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val ingressPublisher = b.add( Source fromPublisher ingress )
      val batch = outer.plan.grouping map { g => b.add( batchSeries( g ) ) }
      val buffer = b.add( Flow[TimeSeriesBase].buffer(outer.bufferSize, OverflowStrategy.backpressure) )
      val detect = b.add( detectionFlow( outer.plan, outer.workers ) )
      val egressSubscriber = b.add( Sink.actorSubscriber[Outliers]( StreamEgress.props(subscriber, high = 50) ) )

      if ( batch.isDefined ) {
        ingressPublisher ~> batch.get ~> buffer ~> detect ~> egressSubscriber
      } else {
        ingressPublisher ~> buffer ~> detect ~> egressSubscriber
      }

      ClosedShape
    }

    val runnable = RunnableGraph.fromGraph( graph ).named( s"plan-detection-${outer.scope}-${subscriber.path}" )
    runnable.run()
    PlanStream( ingressRef, runnable )
  }

  def batchSeries(
    grouping: OutlierPlan.Grouping
  )(
    implicit tsMerging: Merging[TimeSeries]
  ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    log.debug( "batchSeries grouping = [{}]", grouping )

    Flow[TimeSeries]
    .groupedWithin( n = grouping.limit, d = grouping.window )
    .map {
      _.groupBy { _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
  }

  def detectionFlow( p: OutlierPlan, w: Workers )( implicit system: ActorSystem ): Flow[TimeSeriesBase, Outliers, NotUsed] = {
    val label = Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + p.name )

    val flow = {
      import scalaz.\/-

      Flow[TimeSeriesBase]
      .filter { p.appliesTo }
      .map { ts => OutlierDetectionMessage( ts, p ).disjunction }
      .collect { case \/-( m ) => m }
      .via {
        WatermarkProcessorAdapter.elasticProcessorFlow[OutlierDetectionMessage, DetectionResult](
          label.name,
          outer.highWatermark
        ) {
          case m => w.detector
        }
      }
      .map { _.outliers }
    }

    StreamMonitor.add( label )
    flow
  }
}



//
//
//
//
//object AnalysisScopeModule {
//  object AggregateRoot {
//    val module: EntityAggregateModule[AnalysisScopeState] = {
//      val b = EntityAggregateModule.builderFor[AnalysisScopeState].make
//      import b.P.{ Tag => BTag, Props => BProps, _ }
//
//      b
//      .builder
//      .set( BTag, AnalysisScopeState.idTag )
//      .set( BProps, AnalysisScopeActor.props(_, _) )
//      .set( IdLens, AnalysisScopeState.idLens )
//      .set( NameLens, AnalysisScopeState.nameLens )
//      .build()
//    }
//
//    object Protocol extends module.Protocol {
//      sealed trait PlanProtocol
//      //todo add plan change commands
//      //todo reify algorithm
//      //      case class AddAlgorithm( override val targetId: OutlierPlan#TID, algorithm: Symbol ) extends Command with PlanProtocol
//      case object GetSummary extends PlanProtocol
//      case class Summary( sourceId: OutlierPlan#TID, plan: OutlierPlan ) extends PlanProtocol
//
////      case class PlanChanged( override val sourceId: OutlierPlan#TID, plan: OutlierPlan ) extends Event with PlanProtocol
//    }
//
//
//    case class AnalysisScopeState private[AggregateRoot](
//      override val id: AnalysisScopeState#TID,
//      planId: OutlierPlan#TID
//    ) extends Entity {
//      override type ID = OutlierPlan.Scope
//      override def idClass: Class[_] = classOf[OutlierPlan.Scope]
//      override def name: String = id.toString
//    }
//
//    object AnalysisScopeState extends EntityCompanion[AnalysisScopeState] {
//      override def nextId: TaggedID[AnalysisScopeState#TID] = ???
//      override def idTag: Symbol = 'analysisScope
//
//      override implicit def tag( id: OutlierPlan.Scope ): TaggedID[OutlierPlan.Scope] = TaggedID( idTag, id )
//      override val idLens: Lens[AnalysisScopeState, AnalysisScopeState#TID] = lens[AnalysisScopeState] >> 'id
//      override val nameLens: Lens[AnalysisScopeState, String] = new Lens[AnalysisScopeState, String] {
//          override def get( s: AnalysisScopeState ): String = s.name
//          override def set( s: AnalysisScopeState )( n: String ): AnalysisScopeState = {
//            idLens.set( s )( OutlierPlan.Scope.nameLens.set( s.id.id )( n ) )
//          }
//        }
//    }
//
//
//    object AnalysisScopeActor {
//      def props( model: DomainModel, meta: AggregateRootType ): Props = {
//        Props(
//          new AnalysisScopeActor( model, meta )
//          with StackableStreamPublisher
//          with StackableRegisterBusPublisher
//          with AlgorithmsProvider {
//            override def maxInDetectionCpuFactor: Double = 3.0 //todo: make config driven
//            override def bufferSize: Int = 1000 //todo: make config driven
//            override def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType] = {
//              algorithm match {
//                case SeriesDensityAnalyzer.Algorithm => ??? // SeriesDensityAnalyzer.props( router ),
//                case SeriesCentroidDensityAnalyzer.Algorithm => ??? // SeriesCentroidDensityAnalyzer.props( router ),
//                case CohortDensityAnalyzer.Algorithm => ??? // CohortDensityAnalyzer.props( router ),
//                case ExponentialMovingAverageAnalyzer.Algorithm => ??? // ExponentialMovingAverageAnalyzer.props( router ),
//                case FirstHourAverageAnalyzer.Algorithm => ??? // FirstHourAverageAnalyzer.props( router ),
//                case GrubbsAnalyzer.Algorithm => ??? // GrubbsAnalyzer.props( router ),
//                case HistogramBinsAnalyzer.Algorithm => ??? // HistogramBinsAnalyzer.props( router ),
//                case KolmogorovSmirnovAnalyzer.Algorithm => ??? // KolmogorovSmirnovAnalyzer.props( router ),
//                case LeastSquaresAnalyzer.Algorithm => ??? // LeastSquaresAnalyzer.props( router ),
//                case MeanSubtractionCumulationAnalyzer.Algorithm => ??? // MeanSubtractionCumulationAnalyzer.props( router ),
//                case MedianAbsoluteDeviationAnalyzer.Algorithm => ??? // MedianAbsoluteDeviationAnalyzer.props( router ),
//                case SimpleMovingAverageAnalyzer.Algorithm => ??? // SimpleMovingAverageAnalyzer.props( router ),
//                case SeasonalExponentialMovingAverageAnalyzer.Algorithm => ??? // SeasonalExponentialMovingAverageAnalyzer.props( router )
//                case _ => None
//              }
//            }
//          }
//        )
//      }
//
//
//      trait AlgorithmsProvider {
//        def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType]
//        def maxInDetectionCpuFactor: Double
//        def bufferSize: Int
//      }
//    }
//
//    class AnalysisScopeActor( override val model: DomainModel, override val meta: AggregateRootType )
//    extends module.EntityAggregateActor { outer: EventPublisher with AnalysisScopeActor.AlgorithmsProvider =>
//      import Protocol._
//
//      override var state: AnalysisScopeState = _
//      var plan: Option[OutlierPlan] = None
////WORK HERE
////upon add and recover and receipt of PlanChanged evts, complete and rebuild worker stream
//
//      final case class Workers private[AggregateRoot]( detector: ActorRef, router: ActorRef, algorithms: Set[ActorRef] )
//      var workers: Option[Workers] = None
//
//      final case class PlanStream private[AggregateRoot]( ingressRef: ActorRef, graph: RunnableGraph[NotUsed] )
//      var stream: Option[PlanStream] = None
//
//      override def receiveRecover: Receive = {
//        case RecoveryCompleted => {
//
//        }
//      }
//
//
//      override def acceptance: Acceptance = entityAcceptance
//
//
//      def makeTopicWorkers( t: Topic ): Workers = {
//        def makeRouter(): ActorRef = {
//          context.actorOf(
//            DetectionAlgorithmRouter.props.withDispatcher( "outlier-detection-dispatcher" ),
//            s"router-${state.name}"
//          )
//        }
//
//        //              WORK HERE
//        //                analysis plan AR keyed by plan name?
//        //                break out "scope worker" into child actor keyed by scope
//        //                and that creates detector, router, algorithms and stream rt manage in Map
//        def makeAlgorithms( routerRef: ActorRef ): Set[ActorRef] = {
//          plan
//          .map { p =>
//            p.algorithms flatMap { a =>
//              outer.rootTypeFor( a ) map { rt =>
//                model.aggregateOf( rt, state.id )
//              }
//            }
//          }
//          .getOrElse { Set.empty[ActorRef] }
//        }
//
//        def makeDetector( routerRef: ActorRef ): ActorRef = {
//          context.actorOf(
//            OutlierDetection.props( routerRef ).withDispatcher( "outlier-detection-dispatcher" ),
//            s"outlier-detector-${state.name}"
//          )
//        }
//
//        val router = makeRouter()
//
//        Workers(
//          router = router,
//          detector = makeDetector( router ),
//          algorithms = makeAlgorithms( router )
//        )
//      }
//
//      def makePlanStream(
//        p: OutlierPlan,
//        w: Workers,
//        subscriber: ActorRef
//      )(
//        implicit tsMerging: Merging[TimeSeries],
//        system: ActorSystem,
//        materializer: Materializer
//      ): PlanStream = {
//        log.info( "OutlierPlanDetectionRouter making new flow for plan:subscriber: [{} : {}]", p.name, subscriber )
//
//        val (ingressRef, ingress) = {
//          Source
//          .actorPublisher[TimeSeries]( StreamIngress.props[TimeSeries] )
//          .toMat( Sink.asPublisher(false) )( Keep.both )
//          .run()
//        }
//
//        val graph = GraphDSL.create() { implicit b =>
//          import GraphDSL.Implicits._
//
//          val ingressPublisher = b.add( Source fromPublisher ingress )
//          val batch = p.grouping map { g => b.add( batchSeries( g ) ) }
//          val buffer = b.add( Flow[TimeSeriesBase].buffer(outer.bufferSize, OverflowStrategy.backpressure) )
//          val detect = b.add( detectionFlow( p, w ) )
//          val egressSubscriber = b.add( Sink.actorSubscriber[Outliers]( StreamEgress.props(subscriber, high = 50) ) )
//
//          if ( batch.isDefined ) {
//            ingressPublisher ~> batch.get ~> buffer ~> detect ~> egressSubscriber
//          } else {
//            ingressPublisher ~> buffer ~> detect ~> egressSubscriber
//          }
//
//          ClosedShape
//        }
//
//        val runnable = RunnableGraph.fromGraph( graph ).named( s"plan-detection-${p.name}-${subscriber.path}" )
//        runnable.run()
//        PlanStream( ingressRef, runnable )
//      }
//
//      def batchSeries(
//        grouping: OutlierPlan.Grouping
//      )(
//        implicit tsMerging: Merging[TimeSeries]
//      ): Flow[TimeSeries, TimeSeries, NotUsed] = {
//        log.debug( "batchSeries grouping = [{}]", grouping )
//
//        Flow[TimeSeries]
//        .groupedWithin( n = grouping.limit, d = grouping.window )
//        .map {
//          _.groupBy { _.topic }
//          .map { case (topic, tss) =>
//            tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
//          }
//        }
//        .mapConcat { identity }
//      }
//
//      def detectionFlow( p: OutlierPlan, w: Workers )( implicit system: ActorSystem ): Flow[TimeSeriesBase, Outliers, NotUsed] = {
//        val label = Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + p.name )
//
//        val flow = {
//          Flow[TimeSeriesBase]
//          .filter { p.appliesTo }
//          .map { ts => OutlierDetectionMessage( ts, p ).disjunction }
//          .collect { case \/-( m ) => m }
//          .via {
//            MaxInFlightProcessorAdapter.elasticProcessorFlow[OutlierDetectionMessage, DetectionResult](
//              label.name,
//              outer.maxInDetectionCpuFactor
//            ) {
//              case m => w.detector
//            }
//          }
//          .map { _.outliers }
//        }
//
//        StreamMonitor.add( label )
//        flow
//      }
//
//      def activate(): Unit = {
//        if ( workers.isDefined && plan.isDefined && stream.isDefined ) {
//          context.become( LoggingReceive { around( active orElse compute ) } )
//        }
//      }
//
//      override val quiescent: Receive = {
//        case Protocol.Entity.Add( plan ) => {
//          persist( Protocol.Entity.Added(plan) ) { e =>
//            acceptAndPublish( e )
//            workers = Some( makeTopicWorkers( e.info.id.id.topic ) )
//            activate()
//          }
//        }
//      }
//
//      //todo add plan modification
////      val plan: Receive = {
////        case GetSummary => sender() ! Summary( state.id, state )
////      }
//
////      def workers( detector: ActorRef, router: ActorRef, algorithms: Set[ActorRef] ): Receive = akka.actor.Actor.emptyBehavior
//      def compute: Receive = akka.actor.Actor.emptyBehavior
//
//      override val active: Receive = super.active
//
//    }
//  }
//}
