package spotlight.analysis.outlier

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.NotUsed
import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.event.LoggingReceive
import akka.stream._
import akka.stream.actor.ActorSubscriberMessage
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.Timeout
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.envelope._
import peds.akka.envelope.pattern.ask
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream._
import peds.commons.util._
import demesne.{AggregateRootType, DomainModel}
import demesne.module.entity.{messages => EntityMessage}
import peds.archetype.domain.model.core.EntityIdentifying
import spotlight.analysis.outlier.OutlierDetection.DetectionResult
import spotlight.analysis.outlier.algorithm.{AlgorithmModule, AlgorithmProtocol}
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
  ): Props = Props( new Default(scope, plan, model, highWatermark, bufferSize) )

  private class Default(
    override val scope: Scope,
    override val plan: OutlierPlan,
    override val model: DomainModel,
    override val highWatermark: Int,
    override val bufferSize: Int
  ) extends AnalysisScopeProxy
  with Provider {
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
        case SimpleMovingAverageAnalyzer.Algorithm => Some( SimpleMovingAverageModule.rootType )
        case SeasonalExponentialMovingAverageAnalyzer.Algorithm => ??? // SeasonalExponentialMovingAverageAnalyzer.props( router )
        case _ => None
      }
    }
  }


  trait Provider { provider: Actor with ActorLogging =>
    def scope: OutlierPlan.Scope
    def plan: OutlierPlan
    def model: DomainModel
    def highWatermark: Int
    def bufferSize: Int
    def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType]

    implicit def timeout: Timeout = Timeout( 15.seconds )

    def makeRouter()( implicit context: ActorContext ): ActorRef = {
      val algoId = AlgorithmModule.identifying tag scope

      val algorithmRefs = for {
        name <- plan.algorithms.toSeq
        rt <- rootTypeFor( name ).toSeq
        ref = model.aggregateOf( rt, algoId )
      } yield ( name, ref )

      context.actorOf(
        DetectionAlgorithmRouter.props( Map(algorithmRefs:_*) ).withDispatcher( "outlier-detection-dispatcher" ),
        s"router-${provider.plan.name}"
      )
    }

//    def makeAlgorithms(
//      routerRef: ActorRef
//    )(
//      implicit context: ActorContext,
//      ec: ExecutionContext,
//      algorithmIdentifying: EntityIdentifying[AlgorithmModule.AnalysisState]
//    ): Future[Set[ActorRef]] = {
//      def scopeId: AlgorithmModule.AnalysisState#TID = {
//        implicitly[EntityIdentifying[AlgorithmModule.AnalysisState]] tag provider.scope
//      }
//
//      val algs = {
//        for {
//          a <- provider.plan.algorithms
//          rt <- provider.rootTypeFor( a )
//        } yield {
//          val aref = provider.model.aggregateOf( rt, provider.scope )
//          aref !+ EntityMessage.Add( scopeId )
////          implicit val to = provider.timeout
//          ( aref ?+ AlgorithmProtocol.Register( scopeId, routerRef ) )
//          .mapTo[AlgorithmProtocol.Registered]
//          .map { _ => aref }
//        }
//      }
//
//      Future sequence algs
//    }

    def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
      context.actorOf(
        OutlierDetection.props( routerRef ).withDispatcher( "outlier-detection-dispatcher" ),
        s"outlier-detector-${provider.plan.name}"
      )
    }
  }

  private[outlier] final case class Workers private( detector: ActorRef, router: ActorRef )
}

class AnalysisScopeProxy
extends Actor
with EnvelopingActor
with InstrumentedActor
with ActorLogging { outer: AnalysisScopeProxy.Provider =>
  import AnalysisScopeProxy.Workers

  override lazy val metricBaseName: MetricName = MetricName( classOf[AnalysisScopeProxy] )
  val receiveTimer: Timer = metrics.timer( "receive" )
  val failuresMeter: Meter = metrics.meter( "failures" )

  lazy val workers: Workers = makeTopicWorkers( outer.scope.topic )

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
    case (ts: TimeSeries, s: OutlierPlan.Scope) if s == outer.scope => streamIngressFor( sender() ) forwardEnvelope ts
    case (ts: TimeSeries, p: OutlierPlan) if Scope(p, ts.topic) == outer.scope => streamIngressFor( sender() ) forwardEnvelope ts
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


  def makeTopicWorkers( t: Topic ): Workers = {
    val router = outer.makeRouter()
    Workers( detector = outer.makeDetector( router ), router = router )
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
      val detect = b.add( detectionFlow( outer.plan, subscriber, outer.workers ) )

      if ( batch.isDefined ) {
        ingressPublisher ~> batch.get ~> buffer ~> detect
      } else {
        ingressPublisher ~> buffer ~> detect
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

  def detectionFlow(
    p: OutlierPlan,
    subscriber: ActorRef,
    w: Workers
  )(
    implicit system: ActorSystem
  ): Sink[TimeSeries, NotUsed] = {
    val label = Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + p.name )

    Flow[TimeSeries]
    .filter { p.appliesTo }
    .map { ts => OutlierDetectionMessage( ts, p, subscriber ).disjunction }
    .collect { case scalaz.\/-( m ) => m }
    .toMat {
      Sink
      .actorRef[OutlierDetectionMessage]( w.detector, ActorSubscriberMessage.OnComplete )
      .named( s"${p.name}-outlier-detection-inlet" )
    }( Keep.right )
  }


  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy( maxNrOfRetries = -1 ) {
    case ex: ActorInitializationException => {
      log.error( ex, "AnalysisScopeProxy[{}] stopping actor on caught initialization error", self.path )
      failuresMeter.mark()
      Stop
    }

    case ex: ActorKilledException => {
      log.error( ex, "AnalysisScopeProxy[{}] stopping killed actor on caught actor killed exception", self.path )
      failuresMeter.mark()
      Stop
    }

    case ex: InvalidActorNameException => {
      log.error( ex, "AnalysisScopeProxy[{}] restarting on caught invalid actor name", self.path )
      failuresMeter.mark()
      Restart
    }

    case ex: Exception => {
      log.error( ex, "AnalysisScopeProxy[{}] restarting on caught exception from child", self.path )
      failuresMeter.mark()
      Restart
    }

    case ex => {
      log.error( ex, "AnalysisScopeProxy[{}] restarting on unknown error from child", self.path )
      failuresMeter.mark()
      Escalate
    }
  }
}
