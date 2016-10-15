package spotlight.analysis.outlier.algorithm.skyline

import scala.reflect.ClassTag
import scalaz.{-\/, Validation, \/, \/-}
import scalaz.syntax.either._
import scalaz.syntax.validation._
import shapeless.{Lens, lens}
import com.typesafe.config.Config
import org.apache.commons.math3.ml.clustering.DoublePoint
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.math3.distribution.TDistribution
import peds.commons.log.Trace
import peds.commons.{TryV, Valid}
import spotlight.analysis.outlier.{DetectUsing, RecentHistory}
import spotlight.analysis.outlier.algorithm.AlgorithmModule
import spotlight.analysis.outlier.algorithm.AlgorithmProtocol.Advanced
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 10/5/16.
  */
object GrubbsAlgorithm extends AlgorithmModule with AlgorithmModule.ModuleConfiguration { outer =>
  private val trace = Trace[GrubbsAlgorithm.type]

  override lazy val algorithm: Algorithm = new Algorithm {
    override val label: Symbol = 'Grubbs

    override def prepareData( algorithmContext: Context ): TryV[Seq[DoublePoint]] = {
      algorithmContext.tailAverage()( algorithmContext.data ).right
    }

    override def step( point: PointT )( implicit state: State, algorithmContext: Context ): (Boolean, ThresholdBoundary) = trace.block( s"step($point)" ) {
      // original version set expected at context stats mean and stddev
      val result = {
        logger.debug( "TEST: state=[{}]", state )
        Option( state )
        .map { s =>
          val r1 = s
          .grubbsScore

          logger.debug( "TEST: step r1=[{}]", r1)

            r1
          .map { grubbs =>
            val threshold = ThresholdBoundary.fromExpectedAndDistance(
              timestamp = point.timestamp.toLong,
              expected = state.movingStatistics.getMean,
              distance = algorithmContext.tolerance * grubbs * state.movingStatistics.getStandardDeviation
            )

            ( threshold isOutlier point.value, threshold )
          }
        }
      }

      result match {
        case Some( \/-(r) ) => r

        case None => {
          import spotlight.model.timeseries._
          logger.debug( s"skipping point[{}] until history is established", point)
          ( false, ThresholdBoundary empty point.timestamp.toLong )
        }

        case Some( -\/( ex: AlgorithmModule.InsufficientDataSize ) ) => {
          import spotlight.model.timeseries._
          logger.debug( "skipping point[{}]: {}", point, ex.getMessage )
          ( false, ThresholdBoundary empty point.timestamp.toLong )
        }

        case Some( -\/(ex) ) => {
          logger.warn( "issue in step calculation", ex )
          throw ex
        }
      }
    }
  }


  case class Context( override val message: DetectUsing, alpha: Double ) extends CommonContext( message )

  object GrubbsContext {
    val AlphaPath = "alpha"
    def getAlpha( conf: Config ): TryV[Double] = trace.block( "getAlpha" ) {
      logger.debug( "configuration:[{}]", conf )
      logger.debug( "configuration[{}] getDouble = [{}]", AlphaPath, conf.getDouble(AlphaPath).toString )
      val alpha = if ( conf hasPath AlphaPath ) conf getDouble AlphaPath else 0.05
      alpha.right
    }
  }

  override def makeContext( message: DetectUsing, state: Option[State] ): TryV[Context] = {
    GrubbsContext.getAlpha( message.properties ) map { Context( message, _ ) }
  }


  case class State(
    override val id: TID,
    override val name: String,
    movingStatistics: DescriptiveStatistics,
    override val thresholds: Seq[ThresholdBoundary] = Seq.empty[ThresholdBoundary]
  ) extends AlgorithmModule.AnalysisState with AlgorithmModule.StrictSelf[State] {
    override type Self = State

    override def algorithm: Symbol = outer.algorithm.label
    override def addThreshold( threshold: ThresholdBoundary ): State = this.copy( thresholds = this.thresholds :+ threshold )

    def grubbsScore( implicit context: Context ): TryV[Double] = trace.block( "grubbsScore" ) {
      for {
        size <- checkSize( movingStatistics.getN )
        critical <- criticalValue( context.alpha )
      } yield {
        val mean = movingStatistics.getMean
        val sd = movingStatistics.getStandardDeviation
        logger.debug( "Grubbs movingStatistics: N:[{}] mean:[{}] stddev:[{}]", size.toString, mean.toString, sd.toString )

        val thresholdSquared = math.pow( critical, 2 )
        logger.debug( "Grubbs threshold^2:[{}]", thresholdSquared )
        ((size - 1) / math.sqrt(size)) * math.sqrt( thresholdSquared / (size - 2 + thresholdSquared) )
      }
    }

    def criticalValue( alpha: Double ): TryV[Double] = trace.block( s"criticalValue($alpha)" ){
      def calculateCritical( size: Long ): TryV[Double] = trace.briefBlock( s"calculateCritical($size)" ){
        \/ fromTryCatchNonFatal {
          val degreesOfFreedom = math.max( size - 2, 1 ) //todo: not a great idea but for now avoiding error if size <= 2
          new TDistribution( degreesOfFreedom ).inverseCumulativeProbability( alpha / (2.0 * size) )
        }
      }

      for {
        size <- checkSize( movingStatistics.getN )
        critical <- calculateCritical( size )
      } yield critical
    }


    private def checkSize( size: Long ): TryV[Long] = trace.briefBlock( s"checkSize($size)" ){
      size match {
        case s if s < 3 => AlgorithmModule.InsufficientDataSize( algorithm, s, 3 ).left
        case s => s.right
      }
    }

    override def canEqual( that: Any ): Boolean = that.isInstanceOf[State]
    override def toString: String = {
      s"${ClassUtils.getAbbreviatedName(getClass, 15)}( " +
        s"id:[${id}]; "+
        s"movingStatistics:[N:${movingStatistics.getN} mean:${movingStatistics.getMean} stddev:${movingStatistics.getStandardDeviation}]; " +
        s"""thresholds:[${thresholds.mkString(",")}]""" +
        " )"
    }
  }

  override val analysisStateCompanion: AnalysisStateCompanion = State
  override implicit def evState: ClassTag[State] = ClassTag( classOf[State] )

  object State extends AnalysisStateCompanion {
    private val trace = Trace[State.type]

    override type Shape = DescriptiveStatistics

    override def zero( id: State#TID ): State = State( id, name = "", movingStatistics = makeShape() )

    def makeShape(): Shape = new DescriptiveStatistics( RecentHistory.LastN )

    override def updateShape( statistics: Shape, event: Advanced ): Shape = trace.block( "State.updateShape" ) {
      logger.debug( "TEST: statistics shape = [{}]", statistics )
      logger.debug( "TEST: advanced event  = [{}]", event )
      val newStats = statistics.copy()
      newStats addValue event.point.value
      newStats
    }

    override def shapeLens: Lens[State, Shape] = lens[State] >> 'movingStatistics
    override def thresholdLens: Lens[State, Seq[ThresholdBoundary]] = lens[State] >> 'thresholds
  }
}
