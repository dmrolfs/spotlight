//package spotlight.analysis.outlier
//
//import scala.concurrent.ExecutionContext
//import scala.concurrent.duration._
//import akka.NotUsed
//import akka.actor._
//import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
//import akka.agent.Agent
//import akka.event.LoggingReceive
//import akka.stream._
//import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy, WatermarkRequestStrategy}
//import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
//import akka.util.Timeout
//import com.typesafe.config.Config
//import com.typesafe.scalalogging.LazyLogging
//import nl.grons.metrics.scala.{Meter, MetricName, Timer}
//import peds.akka.envelope._
//import peds.akka.metrics.{Instrumented, InstrumentedActor}
//import peds.akka.stream._
//import peds.commons.log.Trace
//import demesne.{AggregateRootType, DomainModel}
//import spotlight.analysis.outlier.algorithm.{AlgorithmModule, AlgorithmProtocol}
//import spotlight.model.outlier.OutlierPlan.Scope
//import spotlight.model.outlier.{CorrelatedData, CorrelatedSeries, OutlierPlan}
//import spotlight.model.timeseries.TimeSeriesBase.Merging
//import spotlight.model.timeseries.{TimeSeries, Topic}
//
//
///**
//  * Created by rolfsd on 5/26/16.
//  */
//object AnalysisScopeProxy extends Instrumented with LazyLogging {
//  private val trace = Trace[AnalysisScopeProxy.type]
//
//  def props(
//    scope: OutlierPlan.Scope,
//    plan: OutlierPlan,
//    model: DomainModel,
//    highWatermark: Int,
//    bufferSize: Int
//  ): Props = Props( new Default(scope, plan, model, highWatermark, bufferSize) )
//
//  private class Default(
//    override val scope: Scope,
//    override var plan: OutlierPlan,
//    override val model: DomainModel,
//    override val highWatermark: Int,
//    override val bufferSize: Int
//  ) extends AnalysisScopeProxy
//  with Provider {
//    private val trace = Trace[Default]
//
//    override def rootTypeFor( algorithm: Symbol )( implicit ec: ExecutionContext ): Option[AggregateRootType] = trace.block( s"rootTypeFor(${algorithm})" ) {
//      DetectionAlgorithmRouter.unsafeRootTypeFor( algorithm ) orElse {
//        scala.concurrent.Await.result( DetectionAlgorithmRouter.futureRootTypeFor(algorithm), 30.seconds )
//      }
//    }
//  }
//
//  def name( scope: OutlierPlan.Scope ): String = "Proxy-" + scope.toString
//
//  trait Provider { provider: Actor with ActorLogging =>
//    def scope: OutlierPlan.Scope
//    def plan: OutlierPlan
//    def plan_=( p: OutlierPlan ): Unit
//    def model: DomainModel
//    def highWatermark: Int
//    def bufferSize: Int
//    def rootTypeFor( algorithm: Symbol )( implicit ec: ExecutionContext ): Option[AggregateRootType]
//
//    implicit def timeout: Timeout = Timeout( 15.seconds )
//
//    def makeRouter()( implicit context: ActorContext ): ActorRef = trace.block( "makeRouter" ){
//      val algorithmRefs = for {
//        name <- plan.algorithms.toSeq
//        rt <- rootTypeFor( name )( context.dispatcher ).toSeq
//        algoId <- rt.identifying.castIntoTID( scope )
//        ref = model( rt, algoId )
//      } yield {
//        ref !+ AlgorithmProtocol.UseConfiguration( AlgorithmModule.identifying.tag(scope), plan.algorithmConfig )
//        ( name, DetectionAlgorithmRouter.DirectResolver(ref) )
//      }
//
//      context.actorOf(
//        DetectionAlgorithmRouter.props( Map(algorithmRefs:_*) ).withDispatcher( DetectionAlgorithmRouter.DispatcherPath ),
//        DetectionAlgorithmRouter.name( provider.scope.toString )
//      )
//    }
//
//    def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
//      context.actorOf(
//        OutlierDetection.props( routerRef ).withDispatcher( OutlierDetection.DispatcherPath ),
//        OutlierDetection.name( provider.scope.toString )
//      )
//    }
//  }
//
//
//  private[outlier] final case class Workers private( detector: ActorRef, router: ActorRef )
//
//  override lazy val metricBaseName: MetricName = MetricName( classOf[AnalysisScopeProxy] )
//  val totalStreams: Agent[Map[ActorPath, Int]] = Agent( Map.empty[ActorPath, Int] )( scala.concurrent.ExecutionContext.global )
//  metrics.gauge( "proxies" ){ totalStreams.get.foldLeft( 0 ){ case (acc, (_, count)) => acc + count } }
//
//  def incrementStreamsForProxy( path: ActorPath ): Unit = {
//    totalStreams send { streams =>
//      val current = streams.getOrElse( path, 1 )
//      streams + ( path -> (current + 1) )
//    }
//  }
//  def decrementStreamsForProxy( path: ActorPath ): Unit = {
//    totalStreams send { streams =>
//      streams
//      .get( path )
//      .map { count => streams + (path -> (count - 1) ) }
//      .getOrElse { streams }
//    }
//  }
//}
//
//class AnalysisScopeProxy
//extends ActorSubscriber
//with EnvelopingActor
//with InstrumentedActor
//with ActorLogging { outer: AnalysisScopeProxy.Provider =>
//  import AnalysisScopeProxy.Workers
//
//  private val trace = Trace[AnalysisScopeProxy]
//
//  override lazy val metricBaseName: MetricName = MetricName( classOf[AnalysisScopeProxy] )
//  val receiveTimer: Timer = metrics.timer( "receive" )
//  val failuresMeter: Meter = metrics.meter( "failures" )
//
//  lazy val workers: Workers = makeTopicWorkers( outer.scope.topic )
//
//  case class PlanStream private( ingressRef: ActorRef, graph: RunnableGraph[NotUsed] )
//  var streams: Map[ActorRef, PlanStream] = Map.empty[ActorRef, PlanStream] //todo good idea to key off sender(analysis plan) ref or path or name?
//
//  val streamSupervision: Supervision.Decider = {
//    case ex => {
//      log.error( ex, "Error caught by analysis scope proxy supervisor" )
//      failuresMeter.mark()
//      Supervision.Restart
//    }
//  }
//
//  implicit val system: ActorSystem = context.system
//  implicit val materializer: Materializer = {
//    ActorMaterializer( ActorMaterializerSettings(system) withSupervisionStrategy streamSupervision )
//  }
//
//  //todo decrement streams count on restart
//
//  override protected def requestStrategy: RequestStrategy = new WatermarkRequestStrategy( outer.highWatermark )
//
//  override def receive: Receive = LoggingReceive { around( workflow ) }
//
//  val workflow: Receive = {
//    case m @ CorrelatedData( ts: TimeSeries, correlationIds, Some(s) ) if s == outer.scope => {
//      log.debug(
//        "AnalysisScopeProxy:WORKFLOW[{}] [{}] forwarding time series [{}] into detection stream",
//        self.path.name, correlationIds, ts.topic
//      )
//      streamIngressFor( sender() ) forwardEnvelope m
//    }
//
//    case AnalysisPlanProtocol.AlgorithmsChanged(_, algorithms, config) => {
//      if ( algorithms != outer.plan.algorithms ) updatePlan( algorithms, config )
//      if ( config != outer.plan.algorithmConfig ) updateAlgorithmConfigurations( config )
//    }
//
//    case Terminated( deadActor ) => {
//      log.info( "[{}] notified of dead actor at: [{}]", self.path.name, deadActor.path )
//      AnalysisScopeProxy decrementStreamsForProxy self.path
//    }
//  }
//
//  def updatePlan( algorithms: Set[Symbol], configuration: Config ): Unit = trace.block( "updatePlan" ) {
//    //todo need to update plans
//    log.warning( "#TODO #DMR: NEED TO UPDATE PLAN ON ALGORITHMS_CHANGED -- MORE TO DO HERE WRT DOWNSTREAM")
//    log.info( "updating plan with algorithms for {} scope", outer.scope )
//    outer.plan = OutlierPlan.algorithmsLens.set( outer.plan )( algorithms )
//  }
//
//  def updateAlgorithmConfigurations( configuration: Config ): Unit = trace.block( "updateAlgorithmConfigurations" ) {
//    outer.workers.router !+ AlgorithmProtocol.UseConfiguration( AlgorithmModule.identifying.tag(outer.scope), configuration )
//  }
//
//  def streamIngressFor( subscriber: ActorRef )( implicit system: ActorSystem, materializer: Materializer ): ActorRef = {
//    log.debug( "looking for plan stream for subscriber:[{}]", subscriber )
//    val ingress = {
//      streams
//      .get( subscriber )
//      .map { _.ingressRef }
//      .getOrElse {
//        log.debug( "making plan stream for subscriber:[{}]", subscriber )
//        val ps = makePlanStream( subscriber )
//        streams += ( subscriber -> ps )
//        AnalysisScopeProxy incrementStreamsForProxy self.path
//        ps.ingressRef
//      }
//    }
//
//    log.debug( "AnalysisScopeProxy: [{} {}] ingress = {}", plan.name, subscriber, ingress )
//    ingress
//  }
//
//
//  def makeTopicWorkers( t: Topic ): Workers = {
//    val router = outer.makeRouter()
//    Workers( detector = outer.makeDetector( router ), router = router )
//  }
//
//  def makePlanStream(
//    subscriber: ActorRef
//  )(
//    implicit tsMerging: Merging[TimeSeries],
//    system: ActorSystem,
//    materializer: Materializer
//  ): PlanStream = {
//    log.info( "making new flow for plan-scope:subscriber: [{}] : [{}]", outer.scope, subscriber.path.name )
//
//    val (ingressRef, ingress) = {
//      Source
//      .actorPublisher[CorrelatedSeries]( StreamIngress.props[CorrelatedSeries] )
//      .toMat( Sink.asPublisher(false) )( Keep.both )
//      .run()
//    }
//
//    val graph = GraphDSL.create() { implicit b =>
//      import GraphDSL.Implicits._
//
//      def flowLog[T]( label: String ): FlowShape[T, T] = {
//        b.add( Flow[T] map { m => log.debug( "AnalysisScopeProxy:FLOW-STREAM: {}: [{}]", label, m.toString); m } )
//      }
//
//      val ingressPublisher = b.add( Source.fromPublisher( ingress ) )
//      val batch = outer.plan.grouping map { g => b.add( batchSeries( g ) ) }
//      val buffer = b.add( Flow[CorrelatedSeries].buffer(outer.bufferSize, OverflowStrategy.backpressure) )
//      val detect = b.add( detectionFlow( outer.plan, subscriber, outer.workers ) )
//
//      if ( batch.isDefined ) {
//        ingressPublisher ~> flowLog[CorrelatedSeries]("entry") ~> batch.get ~> flowLog[CorrelatedSeries]( "batched") ~> buffer ~> flowLog[CorrelatedSeries]( "to-detect" ) ~> detect
//      } else {
//        ingressPublisher ~> flowLog[CorrelatedSeries]("entry") ~> buffer ~> flowLog[CorrelatedSeries]( "to-detect" ) ~> detect
//      }
//
//      ClosedShape
//    }
//
//    val runnable = RunnableGraph.fromGraph( graph ).named( s"PlanDetection-${outer.scope}-${subscriber.path}" )
//    runnable.run()
//    context watch ingressRef
//    PlanStream( ingressRef, runnable )
//  }
//
//  def batchSeries(
//    grouping: OutlierPlan.Grouping
//  )(
//    implicit tsMerging: Merging[TimeSeries]
//  ): Flow[CorrelatedSeries, CorrelatedSeries, NotUsed] = {
//    log.debug( "batchSeries grouping = [{}]", grouping )
//    val correlatedDataLens = CorrelatedData.dataLens[TimeSeries] ~ CorrelatedData.correlationIdsLens[TimeSeries]
//
//    Flow[CorrelatedSeries]
//    .groupedWithin( n = grouping.limit, d = grouping.window )
//    .map {
//      _.groupBy { _.data.topic }
//      .map { case (_, msgs) =>
//        msgs.tail.foldLeft( msgs.head ){ case (acc, m) =>
//          val newData = tsMerging.merge( acc.data, m.data ) valueOr { exs => throw exs.head }
//          val newCorrelationIds = acc.correlationIds ++ m.correlationIds
//          correlatedDataLens.set( acc )( newData, newCorrelationIds )
//        }
//      }
//    }
//    .mapConcat { identity }
//  }
//
//  def detectionFlow(
//    p: OutlierPlan,
//    subscriber: ActorRef,
//    w: Workers
//  )(
//    implicit system: ActorSystem
//  ): Sink[CorrelatedData[TimeSeries], NotUsed] = {
//    val label = Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + p.name )
//
//    Flow[CorrelatedData[TimeSeries]]
//    .map { m => log.debug( "AnalysisScopeProxy:FLOW-DETECT: before-filter: [{}]", m.toString); m }
//    .filter { m =>
//      val result = p appliesTo m
//      log.debug( "AnalysisScopeProxy:FLOW-DETECT: filtering:[{}] msg:[{}]", result, m.toString )
//      result
//    }
//    .map { m => OutlierDetectionMessage(m, p, subscriber).disjunction }
//    .collect { case scalaz.\/-( m ) => m }
//    .map { m => log.debug( "AnalysisScopeProxy:FLOW-DETECT: on-to-detection-grid: [{}]", m.toString); m }
//    .toMat {
//      Sink
//      .actorRef[OutlierDetectionMessage]( w.detector, ActorSubscriberMessage.OnComplete )
//      .named( s"${p.name}OutlierDetectionInlet" )
//    }( Keep.right )
//  }
//
//
//  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy( maxNrOfRetries = -1 ) {
//    case ex: ActorInitializationException => {
//      log.error( ex, "AnalysisScopeProxy[{}] stopping actor on caught initialization error", self.path )
//      failuresMeter.mark()
//      Stop
//    }
//
//    case ex: ActorKilledException => {
//      log.error( ex, "AnalysisScopeProxy[{}] stopping killed actor on caught actor killed exception", self.path )
//      failuresMeter.mark()
//      Stop
//    }
//
//    case ex: InvalidActorNameException => {
//      log.error( ex, "AnalysisScopeProxy[{}] restarting on caught invalid actor name", self.path )
//      failuresMeter.mark()
//      Restart
//    }
//
//    case ex: Exception => {
//      log.error( ex, "AnalysisScopeProxy[{}] restarting on caught exception from child", self.path )
//      failuresMeter.mark()
//      Restart
//    }
//
//    case ex => {
//      log.error( ex, "AnalysisScopeProxy[{}] restarting on unknown error from child", self.path )
//      failuresMeter.mark()
//      Escalate
//    }
//  }
//}
