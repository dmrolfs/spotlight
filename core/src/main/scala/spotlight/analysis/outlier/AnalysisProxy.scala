package spotlight.analysis.outlier

import akka.NotUsed
import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.stream._
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{Meter, MetricName, Timer}
import peds.akka.envelope._
import peds.akka.metrics.{Instrumented, InstrumentedActor}
import peds.commons.log.Trace
import demesne.DomainModel
import peds.akka.stream.StreamIngress
import spotlight.model.outlier.{CorrelatedData, CorrelatedSeries, OutlierPlan, Outliers}
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 12/8/16.
  */
object AnalysisProxy extends Instrumented with LazyLogging {
  private val trace = Trace[AnalysisProxy.type]

  def props(
    model: DomainModel,
    plan: OutlierPlan,
    highWatermark: Int,
    bufferSize: Int
  ): Props = Props( new Default(model, plan, highWatermark = highWatermark, bufferSize = bufferSize) )

  def name( plan: OutlierPlan ): String = s"AnalysisProxy-${plan.name}"


  private class Default(
    override val model: DomainModel,
    override var plan: OutlierPlan,
    override val highWatermark: Int,
    override val bufferSize: Int
  ) extends AnalysisProxy with Provider

  trait Provider { provider: Actor with ActorLogging =>
    def model: DomainModel
    def plan: OutlierPlan
    def plan_=( p: OutlierPlan ): Unit
    def highWatermark: Int
    def bufferSize: Int

    def makeRouter()( implicit context: ActorContext ): ActorRef = {
      val algorithmRefs = for {
        name <- plan.algorithms.toSeq
        rt <- DetectionAlgorithmRouter.rootTypeFor( name )( context.dispatcher ).toSeq
      } yield ( name, DetectionAlgorithmRouter.RootTypeResolver(rt, model) )

      context.actorOf(
        DetectionAlgorithmRouter.props( Map(algorithmRefs:_*) ).withDispatcher( DetectionAlgorithmRouter.DispatcherPath ),
        DetectionAlgorithmRouter.name( provider.plan.name )
      )
    }

    def makeDetector( routerRef: ActorRef )( implicit context: ActorContext ): ActorRef = {
      context.actorOf(
        OutlierDetection.props( routerRef ).withDispatcher( OutlierDetection.DispatcherPath ),
        OutlierDetection.name( plan.name )
      )
    }
  }


  private[outlier] final case class Workers private( detector: ActorRef, router: ActorRef )


  override lazy val metricBaseName: MetricName = MetricName( classOf[AnalysisProxy] )
  private val proxyGraphs: Agent[Set[ActorPath]] = Agent( Set.empty[ActorPath] )( scala.concurrent.ExecutionContext.global )
  metrics.gauge( "proxy-graphs" ){ proxyGraphs.map{ _.size }.get() }

  def registerProxyGraph( ingressPath: ActorPath ): Unit = proxyGraphs send { _ + ingressPath }
  def deregisterProxyGraph( ingressPath: ActorPath ): Unit = proxyGraphs send { _ - ingressPath }
}

class AnalysisProxy extends ActorSubscriber with EnvelopingActor with InstrumentedActor with ActorLogging {
  outer: AnalysisProxy.Provider =>

  import AnalysisProxy.Workers

  private val trace = Trace[AnalysisProxy]

  override lazy val metricBaseName: MetricName = MetricName( classOf[AnalysisProxy] )
  val failuresMeter: Meter = metrics.meter( "failures" )


  override protected def requestStrategy: RequestStrategy = new WatermarkRequestStrategy( outer.highWatermark )

  lazy val workers: Workers = {
    val router = outer.makeRouter()
    Workers( detector = outer.makeDetector(router), router = router )
  }


  case class PlanGraph private( ingressRef: ActorRef, graph: RunnableGraph[NotUsed] )

  var graphs: Map[ActorRef, PlanGraph] = Map.empty[ActorRef, PlanGraph]

  val system: ActorSystem = context.system
  val materializer: Materializer = {
    ActorMaterializer( ActorMaterializerSettings(system) withSupervisionStrategy streamDecider )
  }


  override def receive: Receive = LoggingReceive { around( workflow ) }

  val workflow: Receive = {
    case m @ CorrelatedData( ts: TimeSeries, correlationIds, Some(s) ) if s.plan == outer.plan.name => {
      log.debug(
        "AnalysisProxy:WORKFLOW[{}] [{}] forwarding time series [{}] into detection stream",
        self.path.name, correlationIds, ts.topic
      )

      val subscriber = sender()
      graphIngressFor( subscriber )( system, materializer ) forwardEnvelope m
    }

    case AnalysisPlanProtocol.AlgorithmsChanged( _, algorithms, config ) => {
      if ( algorithms != outer.plan.algorithms ) updatePlan( algorithms, config )
    }

    case Terminated( deadActor ) => {
      log.info( "[{}] notified of dead graph ingress at: [{}]", self.path.name, deadActor.path )
      AnalysisProxy deregisterProxyGraph deadActor.path
    }
  }


  def updatePlan( algorithms: Set[Symbol], configuration: Config ): Unit = trace.block( "updatePlan" ) {
    //todo need to update plans
    log.warning( "#TODO #DMR: NEED TO UPDATE PLAN ON ALGORITHMS_CHANGED -- MORE TO DO HERE WRT DOWNSTREAM")
    log.info( "updating plan with algorithms for {} plan", outer.plan.name )
    outer.plan = OutlierPlan.algorithmsLens.set( outer.plan )( algorithms )
  }


  def graphIngressFor( subscriber: ActorRef )( implicit system: ActorSystem, mat: Materializer ): ActorRef = {
    log.debug( "looking for graph for subscriber:[{}]", subscriber.path.name )
    val ingress = {
      graphs
      .get( subscriber )
      .map { _.ingressRef }
      .getOrElse {
        log.debug( "making graph for subscriber:[{}]", subscriber.path.name )
//        val (ingressRef, graph) = makeGraph( subscriber )
        val (ingressRef, graph) = makeTestGraph( subscriber )
        graphs += ( subscriber -> PlanGraph(ingressRef, graph) )
        AnalysisProxy registerProxyGraph ingressRef.path
        ingressRef
      }
    }

    log.debug( "AnalysisProxy: [{} {}] ingress = {}", plan.name, subscriber.path.name, ingress.path.name )
    ingress
  }

  def makeTestGraph(
    subscriber: ActorRef
  )(
    implicit m: Merging[TimeSeries],
    system: ActorSystem,
    materializer: Materializer
  ): (ActorRef, RunnableGraph[NotUsed]) = {
    val (ingressRef, ingress) = {
      Source
      .actorPublisher[CorrelatedSeries]( StreamIngress.props[CorrelatedSeries] )
      .toMat( Sink.asPublisher(false) )( Keep.both )
      .run()
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val ingressPublisher = b.add( Source.fromPublisher( ingress ) )
      val random = new scala.util.Random( System.nanoTime() )
      val delay = b.add(
        Flow[CorrelatedSeries]
        .map { cs =>
          val base = 15L
          val offset = ( 5 * random.nextGaussian() ).toLong
          Thread.sleep( base + offset )
          cs
        }
      )

      val result = b.add(
        Sink foreach[CorrelatedSeries] { cs =>
          val outlierPoints = if ( random.nextDouble() < 0.10 ) cs.data.points.take(1) else Seq.empty[DataPoint]

          val outliers = Outliers.forSeries(
            algorithms = outer.plan.algorithms,
            plan = outer.plan,
            source = cs.data,
            outliers = outlierPoints,
            thresholdBoundaries = Map.empty[Symbol, Seq[ThresholdBoundary]]
          ).toOption.get

          subscriber !+ spotlight.analysis.outlier.OutlierDetection.DetectionResult( outliers, cs.correlationIds )
        }
      )
      ingressPublisher ~> delay ~> result

      ClosedShape
    }

    val runnable = RunnableGraph.fromGraph( graph ).named( s"PlanDetection-${outer.plan.name}-${subscriber.path}" )
    runnable.run()
    context watch ingressRef
    ( ingressRef, runnable )
  }

  def makeGraph(
    subscriber: ActorRef
  )(
    implicit m: Merging[TimeSeries],
    system: ActorSystem,
    mat: Materializer
  ): (ActorRef, RunnableGraph[NotUsed]) = {
    log.info( "making new flow for plan:subscriber: [{}] : [{}]", outer.plan.name, subscriber.path.name )

    val (ingressRef, ingress) = {
      Source
      .actorPublisher[CorrelatedSeries]( StreamIngress.props[CorrelatedSeries] )
      .toMat( Sink.asPublisher(false) )( Keep.both )
      .run()
    }

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      def flowLog[T]( label: String ): FlowShape[T, T] = {
        b.add( Flow[T] map { m => log.debug( "AnalysisProxy:FLOW-STREAM: {}: [{}]", label, m.toString); m } )
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

    val runnable = RunnableGraph.fromGraph( graph ).named( s"PlanDetection-${outer.plan.name}-${subscriber.path}" )
    runnable.run()
    context watch ingressRef
    ( ingressRef, runnable )
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
      _
      .groupBy { _.data.topic }
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

//todo  WORK HERE TO VERIFY HOW RESULT RETURNS -- can use mapAsync???
  def detectionFlow(
    p: OutlierPlan,
    subscriber: ActorRef,
    w: Workers
  )(
    implicit system: ActorSystem
  ): Sink[CorrelatedSeries, NotUsed] = {
    val label = Symbol( OutlierDetection.WatchPoints.DetectionFlow.name + "." + p.name )

    Flow[CorrelatedData[TimeSeries]]
    .map { m => log.debug( "AnalysisProxy:FLOW-DETECT: before-filter: [{}]", m.toString); m }
    .filter { m =>
      val result = p appliesTo m
      log.debug( "AnalysisProxy:FLOW-DETECT: filtering:[{}] msg:[{}]", result, m.toString )
      result
    }
    .map { m => OutlierDetectionMessage(m, p, subscriber).disjunction }
    .collect { case scalaz.\/-( m ) => m }
    .map { m => log.debug( "AnalysisProxy:FLOW-DETECT: on-to-detection-grid: [{}]", m.toString); m }
    .toMat {
      Sink
      .actorRef[OutlierDetectionMessage]( w.detector, ActorSubscriberMessage.OnComplete )
      .named( s"${p.name}OutlierDetectionInlet" )
    }( Keep.right )
  }


  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy( maxNrOfRetries = -1 ) {
    case ex: ActorInitializationException => {
      log.error( ex, "AnalysisProxy[{}] stopping actor on caught initialization error", self.path )
      failuresMeter.mark()
      Stop
    }

    case ex: ActorKilledException => {
      log.error( ex, "AnalysisProxy[{}] stopping killed actor on caught actor killed exception", self.path )
      failuresMeter.mark()
      Stop
    }

    case ex: InvalidActorNameException => {
      log.error( ex, "AnalysisProxy[{}] restarting on caught invalid actor name", self.path )
      failuresMeter.mark()
      Restart
    }

    case ex: Exception => {
      log.error( ex, "AnalysisProxy[{}] restarting on caught exception from child", self.path )
      failuresMeter.mark()
      Restart
    }

    case ex => {
      log.error( ex, "AnalysisProxy[{}] restarting on unknown error from child", self.path )
      failuresMeter.mark()
      Escalate
    }
  }

  val streamDecider: Supervision.Decider = {
    case ex => {
      log.error( ex, "Error caught by analysis proxy supervisor" )
      failuresMeter.mark()
      Supervision.Restart
    }
  }

}
