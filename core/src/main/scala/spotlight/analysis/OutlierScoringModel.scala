package spotlight.analysis

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import akka.NotUsed
import akka.actor._
import akka.stream.FanOutShape.{ Init, Name }
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import cats.syntax.either._
import cats.syntax.validated._
import com.typesafe.scalalogging.{ Logger, StrictLogging }
import org.slf4j.LoggerFactory
//import bloomfilter.mutable.BloomFilter
import omnibus.akka.metrics.Instrumented
import omnibus.akka.stream.StreamMonitor
import spotlight.Settings
import spotlight.model.outlier._
import spotlight.model.timeseries.TimeSeriesBase.Merging
import spotlight.model.timeseries._

/** Created by rolfsd on 10/21/15.
  */
object OutlierScoringModel extends Instrumented with StrictLogging {
  val OutlierMetricPrefix = "spotlight.outlier."

  object ScoringShape {
    def apply[In, Out, Off]( init: Init[In] ): ScoringShape[In, Out, Off] = new ScoringShape( init )
  }

  class ScoringShape[In, Out, Off]( _init: Init[In] = Name( "ScoringStage" ) ) extends FanOutShape2[In, Out, Off]( _init ) {
    protected override def construct( init: Init[In] ): FanOutShape[In] = new ScoringShape( init )
  }

  object WatchPoints {
    val PlanBuffer = Symbol( "plan.buffer" )
    val ScoringPlanned = Symbol( "scoring.planned" )
    val ScoringUnrecognized = Symbol( "scoring.unrecognized" )
    val Catalog = 'catalog
  }

  def scoringGraph(
    catalogFlow: DetectFlow,
    settings: Settings
  )(
    implicit
    system: ActorSystem,
    materializer: Materializer
  ): Graph[ScoringShape[TimeSeries, Outliers, TimeSeries], NotUsed] = {
    import GraphDSL.Implicits._
    import StreamMonitor._

    val parallelism = settings.parallelismFactor

    GraphDSL.create() { implicit b ⇒
      val start = b.add( Flow[TimeSeries].map { identity } )
      val logMetrics = b.add( logMetric( Logger( LoggerFactory getLogger "Metrics" ), settings.plans ) )
      val blockPriors = b.add( Flow[TimeSeries] filter { notReportedBySpotlight } )
      val zipWithInPlan = b.add(
        Flow[TimeSeries]
          .map { ts ⇒ ( ts, isPlanned( ts, settings.plans ) ) }
          .buffer( 10, OverflowStrategy.backpressure ) //.watchFlow( 'preBroadcast )
      )

      val broadcast = b.add( Broadcast[( TimeSeries, Boolean )]( outputPorts = 2, eagerCancel = false ) )

      val passPlanned = b.add( Flow[( TimeSeries, Boolean )].collect { case ( ts, inPlan ) if inPlan ⇒ ts } )
      val passUnrecognized = b.add( Flow[( TimeSeries, Boolean )].collect { case ( ts, inPlan ) if !inPlan ⇒ ts } )

      val regulator = b.add(
        Flow[TimeSeries]
          .buffer( 10, OverflowStrategy.backpressure ).watchFlow( 'regulator )
          .via( regulateByTopic( 10000 ) )
      )

      //      val buffer = b.add( Flow[TimeSeries].buffer( 10, OverflowStrategy.backpressure ).watchFlow( WatchPoints.PlanBuffer ) )
      val detect = b.add( catalogFlow.watchFlow( WatchPoints.Catalog ) )

      start ~> logMetrics ~> blockPriors ~> zipWithInPlan ~> broadcast ~> passPlanned ~> regulator /*~> buffer */ ~> detect
      broadcast ~> passUnrecognized

      ScoringShape(
        FanOutShape.Ports(
          inlet = start.in,
          outlets = scala.collection.immutable.Seq( detect.out, passUnrecognized.out )
        )
      )
    }
  }

  def logMetric(
    destination: Logger,
    plans: Set[AnalysisPlan]
  ): GraphStage[FlowShape[TimeSeries, TimeSeries]] = {
    new GraphStage[FlowShape[TimeSeries, TimeSeries]] {
      val count = new AtomicInteger( 0 )
      //      var bloom = BloomFilter[Topic]( maxFalsePosProbability = 0.1, 10000000 )
      //      val bloom = BloomFilter[Topic]( numberOfItems = 10000000, falsePositiveRate = 0.1 )

      val in = Inlet[TimeSeries]( "logMetric.in" )
      val out = Outlet[TimeSeries]( "logMetric.out" )

      override def shape: FlowShape[TimeSeries, TimeSeries] = FlowShape.of( in, out )

      override def createLogic( inheritedAttributes: Attributes ): GraphStageLogic = {
        new GraphStageLogic( shape ) {
          setHandler(
            in,
            new InHandler {
              override def onPush(): Unit = {
                val e = grab( in )
                //
                //                //                if ( !bloom.has_?( e.topic ) ) {
                //                if ( !bloom.mightContain( e.topic ) ) {
                //                  bloom add e.topic
                //                  //                  bloom += e.topic
                //                  if ( !e.topic.name.startsWith( OutlierMetricPrefix ) ) {
                //                    destination.debug(
                //                      "[{}] Plan for {}: {}",
                //                      count.incrementAndGet().toString,
                //                      e.topic,
                //                      plans find { _ appliesTo e } getOrElse "NONE"
                //                    )
                //                  }
                //                }
                //
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

  def notReportedBySpotlight( ts: TimeSeriesBase ): Boolean = ts.topic.name.startsWith( OutlierMetricPrefix ) == false
  def isPlanned( ts: TimeSeriesBase, plans: Set[AnalysisPlan] ): Boolean = plans exists { _ appliesTo ts }

  val debugLogger = Logger( LoggerFactory getLogger "Debug" )

  def regulateByTopic(
    max: Long
  )(
    implicit
    system: ActorSystem,
    materializer: Materializer,
    tsMerging: Merging[TimeSeries]
  ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    type TopicAccumulator = Map[Topic, TimeSeries]
    val seedFn: ( TimeSeries ) ⇒ ( TopicAccumulator, Long ) = ( ts: TimeSeries ) ⇒ ( Map( ts.topic → ts ), 0 )

    Flow[TimeSeries]
      .batch( max, seedFn ) {
        case ( ( acc, count ), ts ) ⇒
          val existing = acc get ts.topic
          val updatedAcc = {
            for {
              updated ← existing map { e ⇒ tsMerging.merge( e, ts ).toEither } getOrElse ts.asRight
            } yield {
              acc + ( ts.topic → updated )
            }
          }

          val newAcc = updatedAcc valueOr { exs ⇒
            exs map { ex ⇒ logger.error( "flow regulation for topic:[{}] failed on series merge", ts.topic, ex ) }
            acc
          }

          ( newAcc, count + 1 )
      }
      .map { case ( acc, count ) ⇒ acc }
      .mapConcat { _.values.to[scala.collection.immutable.Iterable] }
  }

  def batchSeriesByWindow(
    windowSize: FiniteDuration = 1.minute,
    parallelism: Int = 4
  )(
    implicit
    system: ActorSystem,
    tsMerging: Merging[TimeSeries],
    materializer: Materializer
  ): Flow[TimeSeries, TimeSeries, NotUsed] = {
    val numTopics = 1

    val n = if ( numTopics * windowSize.toMicros.toInt < 0 ) { numTopics * windowSize.toMicros.toInt } else { Int.MaxValue }
    logger.debug( "n = [{}] for windowSize=[{}]", n.toString, windowSize.toCoarsest )
    Flow[TimeSeries]
      .groupedWithin( n, d = windowSize ) // max elems = 1 per micro; duration = windowSize
      .map {
        _.groupBy { _.topic }
          .map {
            case ( topic, tss ) ⇒
              tss.tail.foldLeft( tss.head ) { ( acc, e ) ⇒ tsMerging.merge( acc, e ) valueOr { exs ⇒ throw exs.head } }
          }
      }
      .mapConcat { identity }
  }
}

//  def batchSeriesByPlan(
//    max: Long
//  )(
//    implicit system: ActorSystem,
//    materializer: Materializer,
//    tsMerging: Merging[TimeSeries]
//  ): Flow[(TimeSeries, AnalysisPlan.Scope), (TimeSeries, AnalysisPlan.Scope), NotUsed] = {
//    type PlanSeriesAccumulator = Map[AnalysisPlan.Scope, TimeSeries]
//    val seed: ((TimeSeries, AnalysisPlan.Scope)) => (PlanSeriesAccumulator, Int) = (tsp: (TimeSeries, AnalysisPlan.Scope)) => {
//      val (ts, p) = tsp
//      val key = p
//      ( Map( key -> ts ), 0 )
//    }
//
//    Flow[(TimeSeries, AnalysisPlan.Scope)]
//    .batch( max, seed ){ case ((acc, count), (ts, p)) =>
//      import scalaz._, Scalaz._
//
//      val key: AnalysisPlan.Scope = p
//      val existing: Option[TimeSeries]  = acc get key
//
//      val newAcc: V[PlanSeriesAccumulator] = for {
//        updated <- existing.map{ e => tsMerging.merge(e, ts).disjunction } getOrElse ts.right
//      } yield {
//        acc + ( key -> updated )
//      }
//
//      val resultingAcc = newAcc match {
//        case \/-( a ) => {
////          val points: Int = a.values.foldLeft( 0 ){ _ + _.points.size }
////          debugLogger.info(
////            "batchSeriesByPlan batching count:[{}] topic-plans & points = [{}] [{}] avg pts/topic-plan combo=[{}]",
////            count.toString,
////            a.keys.size.toString,
////            points.toString,
////            ( points.toDouble / a.keys.size.toDouble ).toString
////          )
//          a
//        }
//        case -\/( exs ) => {
//          exs foreach { ex => logger.error( "batching series by plan failed: {}", ex ) }
//          acc
//        }
//      }
//
//      ( resultingAcc, count + 1 )
//    }
//    .map { ec: (PlanSeriesAccumulator, Int) =>
////      val (elems, count) = ec
////      val recs: Seq[((Topic, AnalysisPlan), TimeSeries)] = elems.toSeq
////      debugLogger.info(
////        "batchSeriesByPlan pushing combos downstream topic:plans combos:[{}] total points:[{}]",
////        recs.size.toString,
////        recs.foldLeft( 0 ){ _ + _._2.points.size }.toString
////      )
//      ec
//    }
//    .mapConcat { case (ps, _) => ps.toSeq.to[scala.collection.immutable.Seq].map { case (scope, ts) => ( ts, scope ) } }
//    .map { tsp: (TimeSeries, AnalysisPlan.Scope) =>
////      val (ts, p) = tsp
////      debugLogger.info( "batchSeriesByPlan pushing downstream topic:plan=[{}:{}]\t# points:[{}]", ts.topic, p.name, ts.points.size.toString )
//      tsp
//    }
//  }

//def scoringGraph(
//catalogProxyProps: Props,
//settings: Settings
//)(
//implicit system: ActorSystem,
//materializer: Materializer
//): Graph[ScoringShape[TimeSeries, Outliers, TimeSeries], NotUsed] = {
//  import akka.stream.scaladsl.GraphDSL.Implicits._
//  import StreamMonitor._
//
//  GraphDSL.create() { implicit b =>
//  val logMetrics = b.add( logMetric( Logger(LoggerFactory getLogger "Metrics"), settings.plans ) )
//  val blockPriors = b.add( Flow[TimeSeries] filter { notReportedBySpotlight } )
//  val broadcast = b.add( Broadcast[TimeSeries](outputPorts = 2, eagerCancel = false) )
//
//  val watchPlanned = Flow[TimeSeries].map{ identity }.watchFlow( WatchPoints.ScoringPlanned )
//  val passPlanned = b.add( Flow[TimeSeries].filter{ isPlanned( _, settings.plans ) }.via( watchPlanned ) )
//
//  val watchUnrecognized = Flow[TimeSeries].map{ identity }.watchFlow( WatchPoints.ScoringUnrecognized )
//  val passUnrecognized = b.add(
//  Flow[TimeSeries].filter{ !isPlanned( _, settings.plans ) }.via( watchUnrecognized )
//  )
//
//  val regulator = b.add( Flow[TimeSeries].via( regulateByTopic(100000) ) )
//
//  val buffer = b.add( Flow[TimeSeries].buffer( 1000, OverflowStrategy.backpressure ).watchFlow( WatchPoints.PlanBuffer ) )
//
//  val detect = b.add( PlanCatalog.flow( catalogProxyProps ).watchFlow(WatchPoints.Catalog) )
//
//  logMetrics ~> blockPriors ~> broadcast ~> passPlanned ~> regulator ~> buffer ~> detect
//  broadcast ~> passUnrecognized
//
//  ScoringShape(
//  FanOutShape.Ports(
//  inlet = logMetrics.in,
//  outlets = scala.collection.immutable.Seq( detect.out, passUnrecognized.out )
//  )
//  )
//}
//}
