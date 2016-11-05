package spotlight.analysis.outlier

import scala.concurrent.duration._
import akka.NotUsed
import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.event.LoggingReceive
import akka.stream._
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.InstrumentedActor
import peds.akka.stream._
import peds.commons.log.Trace
import demesne.{AggregateRootType, DomainModel}
import spotlight.analysis.outlier.algorithm.{AlgorithmModule, AlgorithmProtocol}
import spotlight.model.outlier.OutlierPlan.Scope
import spotlight.model.outlier.{CorrelatedData, CorrelatedSeries, OutlierPlan}
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.model.timeseries.{TimeSeries, Topic}


/**
  * Created by rolfsd on 5/26/16.
  */
object AnalysisScopeProxy extends LazyLogging {
  private val trace = Trace[AnalysisScopeProxy.type]

  def props(
    scope: OutlierPlan.Scope,
    plan: OutlierPlan,
    model: DomainModel,
    highWatermark: Int,
    bufferSize: Int
  ): Props = Props( new Default(scope, plan, model, highWatermark, bufferSize) )

  private class Default(
    override val scope: Scope,
    override var plan: OutlierPlan,
    override val model: DomainModel,
    override val highWatermark: Int,
    override val bufferSize: Int
  ) extends AnalysisScopeProxy
  with Provider {
    private val trace = Trace[Default]

    override def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType] = trace.block( s"rootTypeFor(${algorithm})" ) {
      DetectionAlgorithmRouter unsafeRootTypeFor algorithm
    }
  }

  def name( scope: OutlierPlan.Scope ): String = "Proxy-" + scope.toString

  trait Provider { provider: Actor with ActorLogging =>
    def scope: OutlierPlan.Scope
    def plan: OutlierPlan
    def plan_=( p: OutlierPlan ): Unit
    def model: DomainModel
    def highWatermark: Int
    def bufferSize: Int
    def rootTypeFor( algorithm: Symbol ): Option[AggregateRootType]

    implicit def timeout: Timeout = Timeout( 15.seconds )

    def makeRouter()( implicit context: ActorContext ): ActorRef = trace.block( "makeRouter" ){
//      val algoId = AlgorithmModule.identifying tag scope
//      log.debug( "TEST: scope:[{}] algoId:[{}]", scope, algoId )

      val algorithmRefs = for {
        name <- plan.algorithms.toSeq
      _ = log.debug( "TEST: plan algorithm name:[{}]", name )
        rt <- rootTypeFor( name ).toSeq
        algoId <- rt.identifying.castIntoTID( scope )
      _ = log.debug( "TEST: algoId [{}]", algoId )
        ref = model( rt, algoId )
      _ = log.debug( "TEST: aggregate for [{}]:[{}] = [{}]", name, rt.name, ref)
      } yield {
        ref !+ AlgorithmProtocol.UseConfiguration( AlgorithmModule.identifying.tag(scope), plan.algorithmConfig )
        ( name, ref )
      }

      log.debug( "TEST: for Router - algorithmRefs=[{}]", algorithmRefs.mkString(", ") )

      context.actorOf(
        DetectionAlgorithmRouter.props( Map(algorithmRefs:_*) ).withDispatcher( "outlier-detection-dispatcher" ),
        DetectionAlgorithmRouter.name( provider.scope.toString )
      )
    }

    def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
      context.actorOf(
        OutlierDetection.props( routerRef, provider.scope.toString ).withDispatcher( "outlier-detection-dispatcher" ),
        OutlierDetection.name( provider.scope.toString )
      )
    }
  }


  private[outlier] final case class Workers private( detector: ActorRef, router: ActorRef )


}

class AnalysisScopeProxy
extends ActorSubscriber
with EnvelopingActor
with InstrumentedActor
with ActorLogging { outer: AnalysisScopeProxy.Provider =>
  import AnalysisScopeProxy.Workers

  private val trace = Trace[AnalysisScopeProxy]

  override lazy val metricBaseName: MetricName = MetricName( classOf[AnalysisScopeProxy] )
  val receiveTimer: Timer = metrics.timer( "receive" )
  val failuresMeter: Meter = metrics.meter( "failures" )

  lazy val workers: Workers = makeTopicWorkers( outer.scope.topic )

  case class PlanStream private( ingressRef: ActorRef, graph: RunnableGraph[NotUsed] )
  var streams: Map[ActorRef, PlanStream] = Map.empty[ActorRef, PlanStream] //todo good idea to key off sender(analysis plan) ref or path or name?


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


  override protected def requestStrategy: RequestStrategy = new WatermarkRequestStrategy( outer.highWatermark )

  override def receive: Receive = LoggingReceive { around( workflow ) }

  import spotlight.analysis.outlier.{ AnalysisPlanProtocol => P }

  val workflow: Receive = {
    case m @ CorrelatedData( ts: TimeSeries, correlationIds, Some(s) ) if s == outer.scope => {
      log.debug(
        "AnalysisScopeProxy:WORKFLOW[{}] [{}] forwarding time series [{}] into detection stream",
        self.path.name, correlationIds, ts.topic
      )
      streamIngressFor( sender() ) forwardEnvelope m
    }

//    case (ts: TimeSeries, s: OutlierPlan.Scope) if s == outer.scope => {
//      log.debug( "AnalysisScopeProxy[{}] [{}] forwarding time series [{}] into detection stream", self.path.name, workId, ts.topic )
//      streamIngressFor( sender() ) forwardEnvelope P.AcceptTimeSeries(
//        targetId = AnalysisPlanModule.identifying.tag( s ),
//        correlationIds = Set(workId),
//        data = ts,
//        scope = Some(s)
//      )
//    }

//    case (ts: TimeSeries, p: OutlierPlan) if workflow isDefinedAt (ts, Scope(p, ts.topic)) => workflow( (ts, Scope(p, ts.topic)) )

    case AnalysisPlanProtocol.AlgorithmsChanged(_, algorithms, config) => {
      if ( algorithms != outer.plan.algorithms ) updatePlan( algorithms, config )
      if ( config != outer.plan.algorithmConfig ) updateAlgorithmConfigurations( config )
    }
    case bad => log.error( "AnalysisScopeProxy[{}]: UNKNOWN BAD MESSAGE: [{}]", outer.scope, bad )
  }

  def updatePlan( algorithms: Set[Symbol], configuration: Config ): Unit = trace.block( "updatePlan" ) {
    //todo need to update plans
    log.warning( "#TODO #DMR: NEED TO UPDATE PLAN ON ALGORITHMS_CHANGED -- MORE TO DO HERE WRT DOWNSTREAM")
    log.info( "updating plan with algorithms for {} scope", outer.scope )
    outer.plan = OutlierPlan.algorithmsLens.set( outer.plan )( algorithms )
  }

  def updateAlgorithmConfigurations( configuration: Config ): Unit = trace.block( "updateAlgorithmConfigurations" ) {
    outer.workers.router !+ AlgorithmProtocol.UseConfiguration( AlgorithmModule.identifying.tag(outer.scope), configuration )
  }

  def streamIngressFor( subscriber: ActorRef )( implicit system: ActorSystem, materializer: Materializer ): ActorRef = {
    log.debug( "looking for plan stream for subscriber:[{}]", subscriber )
    val ingress = {
      streams
      .get( subscriber )
      .map { _.ingressRef }
      .getOrElse {
        log.debug( "making plan stream for subscriber:[{}]", subscriber )
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
    log.info( "making new flow for plan-scope:subscriber: [{} : {}]", outer.scope, subscriber )

    val (ingressRef, ingress) = {
      Source
      .actorPublisher[CorrelatedSeries]( StreamIngress.props[CorrelatedSeries] )
      .toMat( Sink.asPublisher(false) )( Keep.both )
      .run()
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      def flowLog[T]( label: String ): FlowShape[T, T] = {
        b.add( Flow[T] map { m => log.debug( "AnalysisScopeProxy:FLOW-STREAM: {}: [{}]", label, m.toString); m } )
      }

      val ingressPublisher = b.add( Source.fromPublisher( ingress ) )
      val batch = outer.plan.grouping map { g => b.add( batchSeries( g ) ) }
      val buffer = b.add( Flow[CorrelatedSeries].buffer(outer.bufferSize, OverflowStrategy.backpressure) )
      val detect = b.add( detectionFlow( outer.plan, subscriber, outer.workers ) )

      if ( batch.isDefined ) {
        ingressPublisher ~> flowLog[CorrelatedSeries]("entry") ~> batch.get ~> flowLog[CorrelatedSeries]( "batched") ~> buffer ~> flowLog[CorrelatedSeries]( "to-detect" ) ~> detect
      } else {
        ingressPublisher ~> flowLog[CorrelatedSeries]("entry") ~> buffer ~> flowLog[CorrelatedSeries]( "to-detect" ) ~> detect
      }

      ClosedShape
    }

    val runnable = RunnableGraph.fromGraph( graph ).named( s"PlanDetection-${outer.scope}-${subscriber.path}" )
    runnable.run()
    PlanStream( ingressRef, runnable )
  }

  def batchSeries(
    grouping: OutlierPlan.Grouping
  )(
    implicit tsMerging: Merging[TimeSeries]
  ): Flow[CorrelatedSeries, CorrelatedSeries, NotUsed] = {
    log.debug( "batchSeries grouping = [{}]", grouping )
    val correlatedDataLens = CorrelatedData.dataLens[TimeSeries] ~ CorrelatedData.correlationIdsLens[TimeSeries]

    Flow[CorrelatedSeries]
    .groupedWithin( n = grouping.limit, d = grouping.window )
    .map {
      _.groupBy { _.data.topic }
      .map { case (_, msgs) =>
        msgs.tail.foldLeft( msgs.head ){ case (acc, m) =>
          val newData = tsMerging.merge( acc.data, m.data ) valueOr { exs => throw exs.head }
          val newCorrelationIds = acc.correlationIds ++ m.correlationIds
          correlatedDataLens.set( acc )( newData, newCorrelationIds )
        }
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
  ): Sink[CorrelatedData[TimeSeries], NotUsed] = {
    val label = Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + p.name )

    Flow[CorrelatedData[TimeSeries]]
    .map { m => log.debug( "AnalysisScopeProxy:FLOW-DETECT: before-filter: [{}]", m.toString); m }
    .filter { m =>
      val result = p appliesTo m
      log.debug( "AnalysisScopeProxy:FLOW-DETECT: filtering:[{}] msg:[{}]", result, m.toString )
      result
    }
    .map { m => OutlierDetectionMessage(m, p, subscriber).disjunction }
    .collect { case scalaz.\/-( m ) => m }
    .map { m => log.debug( "AnalysisScopeProxy:FLOW-DETECT: on-to-detection-grid: [{}]", m.toString); m }
    .toMat {
      Sink
      .actorRef[OutlierDetectionMessage]( w.detector, ActorSubscriberMessage.OnComplete )
      .named( s"${p.name}OutlierDetectionInlet" )
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
