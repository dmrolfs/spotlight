package spotlight.model.outlier

import scala.concurrent.duration._
import scala.reflect._
import scala.util.matching.Regex
import scalaz.{ Ordering ⇒ _, _ }
import Scalaz._
import com.typesafe.config.{ Config, ConfigFactory }
import peds.archetype.domain.model.core._
import peds.commons._
import peds.commons.identifier._
import peds.commons.util._
import spotlight.model.timeseries.{ TimeSeriesBase, Topic }

/** Created by rolfsd on 10/4/15.
  */
sealed trait AnalysisPlan extends Entity with Equals {
  override type ID = ShortUUID
  override val evID: ClassTag[ID] = classTag[ShortUUID]
  override val evTID: ClassTag[TID] = classTag[TaggedID[ShortUUID]]

  override def slug: String = name
  def appliesTo: AnalysisPlan.AppliesTo
  def algorithms: Set[String]
  def grouping: Option[AnalysisPlan.Grouping]
  def timeout: Duration
  def isQuorum: IsQuorum
  def reduce: ReduceOutliers
  def algorithmConfig: Config
  def summary: String
  def isActive: Boolean
  def toSummary: AnalysisPlan.Summary = AnalysisPlan summarize this

  override def hashCode: Int = 41 + id.##

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: AnalysisPlan ⇒ {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.id == that.id )
        }
      }

      case _ ⇒ false
    }
  }

  private[outlier] def typeOrder: Int
  private[outlier] def originLineNumber: Int
}

object AnalysisPlan extends EntityLensProvider[AnalysisPlan] {
  implicit def summarize( p: AnalysisPlan ): Summary = Summary( p )

  case class Summary(
      id: AnalysisPlan#TID,
      name: String,
      slug: String,
      isActive: Boolean = true,
      appliesTo: Option[AnalysisPlan.AppliesTo] = None
  ) extends Equals {
    override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[Summary]

    override def toString: String = {
      s"AnalysisPlan(id:${id.id} name:${name} slug:${slug} isActive:${isActive} appliesTo:${appliesTo})"
    }

    override def hashCode(): Int = 41 + id.##

    override def equals( rhs: Any ): Boolean = {
      rhs match {
        case that: Summary ⇒ {
          if ( this eq that ) true
          else {
            ( that.## == this.## ) &&
              ( that canEqual this ) &&
              ( this.id == that.id )
          }
        }

        case _ ⇒ false
      }
    }
  }

  object Summary {
    def apply( info: AnalysisPlan ): Summary = {
      Summary( id = info.id, name = info.name, slug = info.slug, isActive = info.isActive, appliesTo = Option( info.appliesTo ) )
    }
  }

  case class Scope( plan: String, topic: Topic ) {
    def name: String = toString
    override val toString: String = plan + ":" + topic
  }

  object Scope {
    def apply( plan: AnalysisPlan, topic: Topic ): Scope = Scope( plan = plan.name, topic = topic /*, planId = plan.id*/ )
    def apply( plan: AnalysisPlan, ts: TimeSeriesBase ): Scope = apply( plan, ts.topic )

    trait ScopeIdentifying[T] { self: Identifying[T] ⇒
      override type ID = Scope
      override val evID: ClassTag[ID] = classTag[Scope]
      override val evTID: ClassTag[TID] = classTag[TaggedID[Scope]]
      override def nextId: TryV[TID] = new IllegalStateException( "scopes are fixed to plan:topic pairs so not generated" ).left
      override def fromString( idstr: String ): ID = {
        val Array( p, t ) = idstr split ':'
        Scope( plan = p, topic = t )
      }
    }
  }

  implicit val analysisPlanIdentifying: EntityIdentifying[AnalysisPlan] = {
    new EntityIdentifying[AnalysisPlan] with ShortUUID.ShortUuidIdentifying[AnalysisPlan] {
      override val evEntity: ClassTag[AnalysisPlan] = classTag[AnalysisPlan]
    }
  }

  import shapeless._

  override val idLens: Lens[AnalysisPlan, AnalysisPlan#TID] = new Lens[AnalysisPlan, AnalysisPlan#TID] {
    override def get( p: AnalysisPlan ): AnalysisPlan#TID = p.id
    override def set( p: AnalysisPlan )( id: AnalysisPlan#TID ): AnalysisPlan = {
      SimpleAnalysisPlan(
        id = id,
        name = p.name,
        appliesTo = p.appliesTo,
        algorithms = p.algorithms,
        grouping = p.grouping,
        timeout = p.timeout,
        isQuorum = p.isQuorum,
        reduce = p.reduce,
        algorithmConfig = p.algorithmConfig,
        typeOrder = p.typeOrder,
        originLineNumber = p.originLineNumber,
        isActive = p.isActive
      )
    }
  }

  override val nameLens: Lens[AnalysisPlan, String] = new Lens[AnalysisPlan, String] {
    override def get( p: AnalysisPlan ): String = p.name
    override def set( p: AnalysisPlan )( name: String ): AnalysisPlan = {
      SimpleAnalysisPlan(
        id = p.id,
        name = name,
        appliesTo = p.appliesTo,
        algorithms = p.algorithms,
        grouping = p.grouping,
        timeout = p.timeout,
        isQuorum = p.isQuorum,
        reduce = p.reduce,
        algorithmConfig = p.algorithmConfig,
        typeOrder = p.typeOrder,
        originLineNumber = p.originLineNumber,
        isActive = p.isActive
      )
    }
  }

  override val slugLens: Lens[AnalysisPlan, String] = new Lens[AnalysisPlan, String] {
    override def get( p: AnalysisPlan ): String = p.slug
    override def set( p: AnalysisPlan )( slug: String ): AnalysisPlan = {
      SimpleAnalysisPlan(
        id = p.id,
        name = slug, // for outlier plan slug == name
        appliesTo = p.appliesTo,
        algorithms = p.algorithms,
        grouping = p.grouping,
        timeout = p.timeout,
        isQuorum = p.isQuorum,
        reduce = p.reduce,
        algorithmConfig = p.algorithmConfig,
        typeOrder = p.typeOrder,
        originLineNumber = p.originLineNumber,
        isActive = p.isActive
      )
    }
  }

  val isActiveLens: Lens[AnalysisPlan, Boolean] = new Lens[AnalysisPlan, Boolean] {
    override def get( p: AnalysisPlan ): Boolean = p.isActive
    override def set( p: AnalysisPlan )( a: Boolean ): AnalysisPlan = {
      SimpleAnalysisPlan(
        id = p.id,
        name = p.name,
        appliesTo = p.appliesTo,
        algorithms = p.algorithms,
        grouping = p.grouping,
        timeout = p.timeout,
        isQuorum = p.isQuorum,
        reduce = p.reduce,
        algorithmConfig = p.algorithmConfig,
        typeOrder = p.typeOrder,
        originLineNumber = p.originLineNumber,
        isActive = a
      )
    }
  }

  val algorithmsLens: Lens[AnalysisPlan, Set[String]] = new Lens[AnalysisPlan, Set[String]] {
    override def get( p: AnalysisPlan ): Set[String] = p.algorithms
    override def set( p: AnalysisPlan )( algos: Set[String] ): AnalysisPlan = {
      SimpleAnalysisPlan(
        id = p.id,
        name = p.name,
        appliesTo = p.appliesTo,
        algorithms = algos,
        grouping = p.grouping,
        timeout = p.timeout,
        isQuorum = p.isQuorum,
        reduce = p.reduce,
        algorithmConfig = p.algorithmConfig,
        typeOrder = p.typeOrder,
        originLineNumber = p.originLineNumber,
        isActive = p.isActive
      )
    }
  }

  val AlgorithmConfig: String = "algorithm-config"

  type ExtractTopic = PartialFunction[Any, Option[Topic]]

  type Creator = () ⇒ V[Set[AnalysisPlan]]

  final case class Grouping( limit: Int, window: FiniteDuration )

  def apply(
    name: String,
    timeout: Duration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[String],
    grouping: Option[AnalysisPlan.Grouping],
    planSpecification: Config
  )(
    appliesTo: ( Any ) ⇒ Boolean
  ): AnalysisPlan = {
    SimpleAnalysisPlan(
      id = analysisPlanIdentifying.safeNextId,
      name = name,
      appliesTo = AppliesTo.function( appliesTo ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      typeOrder = 3,
      originLineNumber = planSpecification.origin.lineNumber
    )
  }

  def apply(
    name: String,
    timeout: Duration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[String],
    grouping: Option[AnalysisPlan.Grouping],
    planSpecification: Config
  )(
    appliesTo: PartialFunction[Any, Boolean]
  ): AnalysisPlan = {
    SimpleAnalysisPlan(
      id = analysisPlanIdentifying.safeNextId,
      name = name,
      appliesTo = AppliesTo.partialFunction( appliesTo ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      typeOrder = 3,
      originLineNumber = planSpecification.origin.lineNumber
    )
  }

  def forTopics(
    name: String,
    timeout: Duration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[String],
    grouping: Option[AnalysisPlan.Grouping],
    planSpecification: Config,
    extractTopic: ExtractTopic,
    topics: Set[Topic]
  ): AnalysisPlan = {
    SimpleAnalysisPlan(
      id = analysisPlanIdentifying.safeNextId,
      name = name,
      appliesTo = AppliesTo.topics( topics, extractTopic ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      typeOrder = 1,
      originLineNumber = planSpecification.origin.lineNumber
    )
  }

  def forTopics(
    name: String,
    timeout: Duration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[String],
    grouping: Option[AnalysisPlan.Grouping],
    planSpecification: Config,
    extractTopic: ExtractTopic,
    topics: String*
  ): AnalysisPlan = {
    SimpleAnalysisPlan(
      id = analysisPlanIdentifying.safeNextId,
      name = name,
      appliesTo = AppliesTo.topics( topics.map { Topic( _ ) }.toSet, extractTopic ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      typeOrder = 1,
      originLineNumber = planSpecification.origin.lineNumber
    )
  }

  def forRegex(
    name: String,
    timeout: Duration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[String],
    grouping: Option[AnalysisPlan.Grouping],
    planSpecification: Config,
    extractTopic: ExtractTopic,
    regex: Regex
  ): AnalysisPlan = {
    SimpleAnalysisPlan(
      id = analysisPlanIdentifying.safeNextId,
      name = name,
      appliesTo = AppliesTo.regex( regex, extractTopic ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      typeOrder = 2,
      originLineNumber = planSpecification.origin.lineNumber
    )
  }

  def default(
    name: String,
    timeout: Duration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[String],
    grouping: Option[AnalysisPlan.Grouping],
    planSpecification: Config = ConfigFactory.empty
  ): AnalysisPlan = {
    SimpleAnalysisPlan(
      id = analysisPlanIdentifying.safeNextId,
      name = name,
      appliesTo = AppliesTo.all,
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      typeOrder = Int.MaxValue,
      originLineNumber = planSpecification.origin.lineNumber
    )
  }

  private def getAlgorithms( spec: Config ): Set[String] = {
    import scala.collection.JavaConverters._
    if ( spec hasPath AlgorithmConfig ) spec.getConfig( AlgorithmConfig ).root.keySet.asScala.toSet
    else Set.empty[String]
  }

  private def getAlgorithmConfig( spec: Config ): Config = {
    if ( spec hasPath AlgorithmConfig ) spec getConfig AlgorithmConfig
    else ConfigFactory.empty( s"no algorithm-config at spec[${spec.origin}]" )
  }

  final case class SimpleAnalysisPlan private[outlier] (
      override val id: AnalysisPlan#TID,
      override val name: String,
      override val appliesTo: AnalysisPlan.AppliesTo,
      override val algorithms: Set[String],
      override val grouping: Option[AnalysisPlan.Grouping],
      override val timeout: Duration,
      override val isQuorum: IsQuorum,
      override val reduce: ReduceOutliers,
      override val algorithmConfig: Config,
      override private[outlier] val typeOrder: Int,
      override private[outlier] val originLineNumber: Int,
      override val isActive: Boolean = true
  ) extends AnalysisPlan {
    override val summary: String = getClass.safeSimpleName + s"""(${name} ${appliesTo.toString})"""

    override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[SimpleAnalysisPlan]

    override val toString: String = {
      getClass.safeSimpleName + s"[${id}](" +
        s"""name:[$name], ${appliesTo.toString}, quorum:[${isQuorum}], reduce:[${reduce}] """ +
        s"""algorithms:[${algorithms.mkString( "," )}], timeout:[${timeout.toCoarsest}], """ +
        s"""grouping:[${grouping}], algorithm-config:[${algorithmConfig.root}]""" +
        ")"
    }
  }

  sealed trait AppliesTo extends ( ( Any ) ⇒ Boolean ) with Serializable

  object AppliesTo {
    def function( f: ( Any ) ⇒ Boolean ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Boolean = f( message )
      override val toString: String = "AppliesTo.function"
    }

    def partialFunction( pf: PartialFunction[Any, Boolean] ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Boolean = if ( pf isDefinedAt message ) pf( message ) else false
      override val toString: String = "AppliesTo.partialFunction"
    }

    def topics( topics: Set[Topic], extractTopic: ExtractTopic ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Boolean = {
        if ( extractTopic isDefinedAt message ) extractTopic( message ) map { topics contains _ } getOrElse { false }
        else false
      }

      override val toString: String = s"""AppliesTo.topics[${topics.mkString( "," )}]"""
    }

    def regex( regex: Regex, extractTopic: ExtractTopic ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Boolean = {
        if ( !extractTopic.isDefinedAt( message ) ) false
        else {
          val result = extractTopic( message ) flatMap { t ⇒ regex findFirstMatchIn t.toString }
          result.isDefined
        }
      }

      override val toString: String = s"AppliesTo.regex[$regex]"
    }

    val all: AppliesTo = new AppliesTo {
      override def apply( message: Any ): Boolean = true
      override val toString: String = "AppliesTo.all"
    }
  }

  implicit val analysisPlanOrdering = new Ordering[AnalysisPlan] {
    override def compare( lhs: AnalysisPlan, rhs: AnalysisPlan ): Int = {
      val typeOrdering = Ordering[Int].compare( lhs.typeOrder, rhs.typeOrder )
      if ( typeOrdering != 0 ) typeOrdering
      else Ordering[Int].compare( lhs.originLineNumber, rhs.originLineNumber )
    }
  }
}
