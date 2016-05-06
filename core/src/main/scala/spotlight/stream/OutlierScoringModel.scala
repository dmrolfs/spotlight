package spotlight.stream

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.NotUsed
import akka.stream.FanOutShape.{Init, Name}
import akka.actor._
import akka.stream.scaladsl._
import akka.stream._
import akka.stream.stage._
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.slf4j.LoggerFactory
import peds.akka.metrics.Instrumented
import peds.akka.stream.StreamMonitor
import peds.commons.{V, Valid}
import peds.commons.collection.BloomFilter
import spotlight.analysis.outlier.OutlierPlanDetectionRouter
import spotlight.model.outlier._
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 10/21/15.
  */
object OutlierScoringModel extends Instrumented with StrictLogging {
  val OutlierMetricPrefix = "spotlight.outlier."

  object ScoringShape {
    def apply[In, Out, Off]( init: Init[In] ): ScoringShape[In, Out, Off] = new ScoringShape( init )
  }

  class ScoringShape[In, Out, Off]( _init: Init[In] = Name("ScoringStage") ) extends FanOutShape2[In, Out, Off]( _init ) {
    protected override def construct( init: Init[In] ): FanOutShape[In] = new ScoringShape( init )
  }

  object WatchPoints {
    val ScoringPlanned = Symbol("scoring.planned")
    val ScoringUnrecognized = Symbol("scoring.unrecognized")
    val ScoringBatch = Symbol("scoring.batch")
    val ScoringAnalysisBuffer = Symbol("scoring.analysisBuffer")
    val ScoringGrouping = Symbol("scoring.grouping")
    val ScoringDetect = Symbol("scoring.detect")
  }

  def scoringGraph(
    planRouterRef: ActorRef,
    config: Configuration
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Graph[ScoringShape[TimeSeries, Outliers, TimeSeries], NotUsed] = {
    import akka.stream.scaladsl.GraphDSL.Implicits._
    import StreamMonitor._

    GraphDSL.create() { implicit b =>
      val logMetrics = b.add( logMetric( Logger(LoggerFactory getLogger "Metrics"), config.plans ) )
      val blockPriors = b.add( Flow[TimeSeries].filter{ ts => !isOutlierReport(ts) } )
      val broadcast = b.add( Broadcast[TimeSeries](outputPorts = 2, eagerCancel = false) )

      val watchPlanned = Flow[TimeSeries].map{ identity }.watchConsumed( WatchPoints.ScoringPlanned )
      val passPlanned = b.add( Flow[TimeSeries].filter{ isPlanned( _, config.plans ) }.via( watchPlanned ) )

      val watchUnrecognized = Flow[TimeSeries].map{ identity }.watchConsumed( WatchPoints.ScoringUnrecognized )
      val passUnrecognized = b.add(
        Flow[TimeSeries].filter{ !isPlanned( _, config.plans ) }.via( watchUnrecognized )
      )

      val planConcat = b.add(
        Flow[TimeSeries]
        .map { ts => config.plans collect { case p if p appliesTo ts => (ts, p) } }
        .mapConcat { identity }
      )

      val planBuffer = b.add(
        Flow[(TimeSeries, OutlierPlan)]
        .buffer( 1000, OverflowStrategy.backpressure).watchFlow( Symbol("plan.buffer") )
        .via( batchSeriesByPlan(100000) )
      )

      val plansDetectOutliers = b.add(
        OutlierPlanDetectionRouter.elasticPlanDetectionRouterFlow(
          planDetectorRouterRef = planRouterRef,
          maxInDetectionCpuFactor = config.maxInDetectionCpuFactor
        )
      )

      logMetrics ~> blockPriors ~> broadcast ~> passPlanned ~> planConcat ~> planBuffer ~> plansDetectOutliers
      broadcast ~> passUnrecognized

      ScoringShape(
        FanOutShape.Ports(
          inlet = logMetrics.in,
          outlets = scala.collection.immutable.Seq( plansDetectOutliers.out, passUnrecognized.out )
        )
      )
    }
  }


  def logMetric(
    destination: Logger,
    plans: Seq[OutlierPlan]
  ): GraphStage[FlowShape[TimeSeries, TimeSeries]] = {
    new GraphStage[FlowShape[TimeSeries, TimeSeries]] {
      val count = new AtomicInteger( 0 )
      var bloom = BloomFilter[Topic]( maxFalsePosProbability = 0.001, 500000 )

      val in = Inlet[TimeSeries]( "logMetric.in" )
      val out = Outlet[TimeSeries]( "logMetric.out" )

      override def shape: FlowShape[TimeSeries, TimeSeries] = FlowShape.of( in, out )

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
        new GraphStageLogic( shape ) {
          setHandler(
            in,
            new InHandler {
              override def onPush(): Unit = {
                val e = grab( in )

                if ( !bloom.has_?( e.topic ) ) {
                  bloom += e.topic
                  if ( !e.topic.name.startsWith( OutlierMetricPrefix ) ) {
                    destination.debug(
                      "[{}] Plan for {}: {}",
                      count.incrementAndGet( ).toString,
                      e.topic,
                      plans.find {_ appliesTo e}.getOrElse( "NONE" )
                    )
                  }
                }

                push( out, e )
              }
            }
          )

          setHandler(
            out,
            new OutHandler {
              override def onPull(): Unit = pull( in )
            }
          )
        }
      }
    }
  }

  def filterPlanned( plans: Seq[OutlierPlan] )( implicit system: ActorSystem ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    Flow[TimeSeries]
    .filter { ts => !isOutlierReport(ts) && isPlanned( ts, plans ) }
  }

  def filterUnrecognized( plans: Seq[OutlierPlan] )( implicit system: ActorSystem ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    Flow[TimeSeries].filter{ ts => !isOutlierReport(ts) }
  }

  def isOutlierReport( ts: TimeSeriesBase ): Boolean = ts.topic.name.startsWith( OutlierMetricPrefix )
  def isPlanned( ts: TimeSeriesBase, plans: Seq[OutlierPlan] ): Boolean = plans.exists{ _ appliesTo ts }

  val debugLogger = Logger( LoggerFactory getLogger "Debug" )

  def batchSeriesByPlan(
    max: Long
  )(
    implicit system: ActorSystem,
    materializer: Materializer,
    tsMerging: Merging[TimeSeries]
  ): Flow[(TimeSeries, OutlierPlan), (TimeSeries, OutlierPlan), NotUsed] = {
    type PlanSeriesAccumulator = Map[(Topic, OutlierPlan), TimeSeries]
    val seed: ((TimeSeries, OutlierPlan)) => (PlanSeriesAccumulator, Int) = (tsp: (TimeSeries, OutlierPlan)) => {
      val (ts, p) = tsp
      val key = (ts.topic, p)
      ( Map( key -> ts ), 0 )
    }

    Flow[(TimeSeries, OutlierPlan)]
    .batch( max, seed ){ case ((acc, count), (ts, p)) =>
      import scalaz._, Scalaz._

      val key: (Topic, OutlierPlan) = (ts.topic, p)
      val existing: Option[TimeSeries]  = acc get key

      val newAcc: V[PlanSeriesAccumulator] = for {
        updated <- existing.map{ e => tsMerging.merge(e, ts).disjunction } getOrElse ts.right
      } yield {
        acc + ( key -> updated )
      }

      val resultingAcc = newAcc match {
        case \/-( a ) => {
//          val points: Int = a.values.foldLeft( 0 ){ _ + _.points.size }
//          debugLogger.info(
//            "batchSeriesByPlan batching count:[{}] topic-plans & points = [{}] [{}] avg pts/topic-plan combo=[{}]",
//            count.toString,
//            a.keys.size.toString,
//            points.toString,
//            ( points.toDouble / a.keys.size.toDouble ).toString
//          )
          a
        }
        case -\/( exs ) => {
          exs foreach { ex => logger.error( "batching series by plan failed: {}", ex ) }
          acc
        }
      }

      ( resultingAcc, count + 1 )
    }
    .map { ec: (PlanSeriesAccumulator, Int) =>
//      val (elems, count) = ec
//      val recs: Seq[((Topic, OutlierPlan), TimeSeries)] = elems.toSeq
//      debugLogger.info(
//        "batchSeriesByPlan pushing combos downstream topic:plans combos:[{}] total points:[{}]",
//        recs.size.toString,
//        recs.foldLeft( 0 ){ _ + _._2.points.size }.toString
//      )
      ec
    }
    .mapConcat { case (ps, _) => ps.toSeq.to[scala.collection.immutable.Seq].map { case ((topic, plan), ts) => ( ts, plan ) } }
    .map { tsp: (TimeSeries, OutlierPlan) =>
//      val (ts, p) = tsp
//      debugLogger.info( "batchSeriesByPlan pushing downstream topic:plan=[{}:{}]\t# points:[{}]", ts.topic, p.name, ts.points.size.toString )
      tsp
    }
  }


  def batchSeriesByWindow(
    windowSize: FiniteDuration = 1.minute,
    parallelism: Int = 4
  )(
    implicit system: ActorSystem,
    tsMerging: Merging[TimeSeries],
    materializer: Materializer
  ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    val numTopics = 1

    val n = if ( numTopics * windowSize.toMicros.toInt < 0 ) { numTopics * windowSize.toMicros.toInt } else { Int.MaxValue }
    logger.debug( "n = [{}] for windowSize=[{}]", n.toString, windowSize.toCoarsest )
    Flow[TimeSeries]
    .groupedWithin( n, d = windowSize ) // max elems = 1 per micro; duration = windowSize
    .map {
      _.groupBy{ _.topic }
      .map { case (topic, tss) =>
        tss.tail.foldLeft( tss.head ){ (acc, e) => tsMerging.merge( acc, e ) valueOr { exs => throw exs.head } }
      }
    }
    .mapConcat { identity }
  }
}
