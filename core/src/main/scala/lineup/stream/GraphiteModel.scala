package lineup.stream

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import akka.actor._
import akka.stream.scaladsl._
import akka.stream._
import akka.stream.stage.{ SyncDirective, Context, PushStage }

import com.typesafe.scalalogging.{ StrictLogging, Logger }
import org.slf4j.LoggerFactory
import peds.akka.metrics.Instrumented
import peds.akka.stream.StreamMonitor
import peds.commons.collection.BloomFilter
import lineup.analysis.outlier.OutlierDetection
import lineup.model.outlier._
import lineup.model.timeseries.TimeSeriesBase.Merging
import lineup.model.timeseries._
import lineup.stream.OutlierDetectionWorkflow._


/**
 * Created by rolfsd on 10/21/15.
 */
object GraphiteModel extends Instrumented with StrictLogging {
  object WatchPoints {
    val ScoringPlanned = Symbol("scoring.planned")
    val ScoringBatch = Symbol("scoring.batch")
    val ScoringAnalysisBuffer = Symbol("scoring.analysisBuffer")
    val ScoringGrouping = Symbol("scoring.grouping")
    val ScoringDetect = Symbol("scoring.detect")
  }
  def scoringGraph(
    detector: ActorRef,
    config: Configuration
  )(
    implicit system: ActorSystem,
    materializer: Materializer
  ): Graph[FlowShape[TimeSeries,Outliers], Unit] = {
    import akka.stream.scaladsl.GraphDSL.Implicits._
    import StreamMonitor._

    GraphDSL.create() { implicit b =>
      val planned = b.add( filterPlanned( config.plans ).watchFlow( WatchPoints.ScoringPlanned ) )

      val batch = b.add( batchSeries( config.windowDuration ).watchConsumed( WatchPoints.ScoringBatch ) )

      val scoringBuffer = b.add(
        Flow[TimeSeries]
        .via(
          Flow[TimeSeries]
          .buffer( config.workflowBufferSize, OverflowStrategy.backpressure )
          .watchFlow( WatchPoints.ScoringAnalysisBuffer )
        )
        .watchFlow( WatchPoints.ScoringGrouping )
      )

      val detectOutlier = b.add(
        OutlierDetection.detectOutlier(
          detector = detector,
          maxAllowedWait = config.detectionBudget,
          parallelism = Runtime.getRuntime.availableProcessors() * 8
        )
        .watchFlow( WatchPoints.ScoringDetect )
      )

      planned ~> batch ~> scoringBuffer ~> detectOutlier

      FlowShape( planned.in, detectOutlier.out )
    }
  }


  def filterPlanned( plans: Seq[OutlierPlan] )( implicit system: ActorSystem ): Flow[TimeSeries, TimeSeries, Unit] = {
    def logMetric: PushStage[TimeSeries, TimeSeries] = new PushStage[TimeSeries, TimeSeries] {
      val count = new AtomicInteger( 0 )
      var bloom = BloomFilter[Topic]( maxFalsePosProbability = 0.001, 500000 )
      val metricLogger = Logger( LoggerFactory getLogger "Metrics" )

      override def onPush( elem: TimeSeries, ctx: Context[TimeSeries] ): SyncDirective = {
        if ( bloom has_? elem.topic ) ctx push elem
        else {
          bloom += elem.topic
          if ( !elem.topic.name.startsWith(OutlierMetricPrefix) ) {
            metricLogger.debug(
              s"""[${count.incrementAndGet}] Plan for ${elem.topic}: ${plans.find{ _ appliesTo elem }.getOrElse{"NONE"}}"""
            )
          }

          ctx push elem
        }
      }
    }

    Flow[TimeSeries]
    .transform( () => logMetric )
    .filter { ts => !ts.topic.name.startsWith( OutlierMetricPrefix ) && plans.exists{ _ appliesTo ts } }
  }

  def batchSeries(
    windowSize: FiniteDuration = 1.minute,
    parallelism: Int = 4
  )(
    implicit system: ActorSystem,
    tsMerging: Merging[TimeSeries],
    materializer: Materializer
  ): Flow[TimeSeries, TimeSeries, Unit] = {
    val numTopics = 1

    val n = if ( numTopics * windowSize.toMicros.toInt < 0 ) { numTopics * windowSize.toMicros.toInt } else { Int.MaxValue }
    logger info s"n = [${n}] for windowSize=[${windowSize.toCoarsest}]"
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
