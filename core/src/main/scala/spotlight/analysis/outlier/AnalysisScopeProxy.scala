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
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.density.{CohortDensityAnalyzer, SeriesCentroidDensityAnalyzer, SeriesDensityAnalyzer}
import spotlight.analysis.outlier.algorithm.skyline._
import spotlight.model.outlier.OutlierPlan.Scope
import spotlight.model.outlier.{OutlierPlan, Outliers}
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.model.timeseries.{TimeSeries, Topic}


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
      val algs = {
        for {
          a <- outer.plan.algorithms
          rt <- outer.rootTypeFor( a )
        } yield {
          val aref = outer.model.aggregateOf( rt, outer.scope.id )
          aref ! AlgorithmModule.Protocol.Add( outer.scope.id )
          implicit val timeout = akka.util.Timeout( 15.seconds )
          ( aref ? AlgorithmModule.Protocol.Register( outer.scope.id, routerRef ) )
          .mapTo[AlgorithmModule.Protocol.Registered]
          .map { _ => aref }
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
      val buffer = b.add( Flow[TimeSeries].buffer(outer.bufferSize, OverflowStrategy.backpressure) )
      val detect = b.add( detectionFlow( outer.plan, outer.workers ) )
      val egressSubscriber = b.add( Sink.actorSubscriber[Outliers]( StreamEgress.props(subscriber, high = outer.highWatermark) ) )

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

  def detectionFlow( p: OutlierPlan, w: Workers )( implicit system: ActorSystem ): Flow[TimeSeries, Outliers, NotUsed] = {
    val label = Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + p.name )

    val flow = {
      import scalaz.\/-

      Flow[TimeSeries]
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
