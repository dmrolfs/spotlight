package spotlight.model.outlier

import scala.concurrent.duration._
import scala.reflect._
import scala.util.matching.Regex
import scalaz.{Ordering => _, _}
import Scalaz._
import com.typesafe.config.{Config, ConfigFactory}
import peds.archetype.domain.model.core._
import peds.commons._
import peds.commons.identifier._
import peds.commons.util._
import spotlight.model.timeseries.{TimeSeriesBase, Topic}


/**
 * Created by rolfsd on 10/4/15.
 */
sealed trait OutlierPlan extends Entity with Equals {
  override type ID = ShortUUID
  override val evID: ClassTag[ID] = classTag[ShortUUID]
  override val evTID: ClassTag[TID] = classTag[TaggedID[ShortUUID]]

  override def slug: String = name
  def appliesTo: OutlierPlan.AppliesTo
  def algorithms: Set[Symbol]
  def grouping: Option[OutlierPlan.Grouping]
  def timeout: FiniteDuration
  def isQuorum: IsQuorum
  def reduce: ReduceOutliers
  def algorithmConfig: Config
  def summary: String
  def isActive: Boolean
  def toSummary: OutlierPlan.Summary = OutlierPlan summarize this

  override def hashCode: Int = 41 + id.##

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: OutlierPlan => {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
          ( that canEqual this ) &&
          ( this.id == that.id )
        }
      }

      case _ => false
    }
  }

  private[outlier] def typeOrder: Int
  private[outlier] def originLineNumber: Int
}

object OutlierPlan extends EntityLensProvider[OutlierPlan] {
  implicit def summarize( p: OutlierPlan ): Summary = Summary( p.id, p.name, p.appliesTo )

  case class Summary( id: OutlierPlan#TID, name: String, appliesTo: OutlierPlan.AppliesTo )
  object Summary {
    def apply( info: OutlierPlan ): Summary = Summary( id = info.id, name = info.name, appliesTo = info.appliesTo )
  }

  case class Scope( plan: String, topic: Topic ) {
    def name: String = toString
    override val toString: String = plan +":"+ topic
  }

  object Scope {
    def apply( plan: OutlierPlan, topic: Topic ): Scope = Scope( plan = plan.name, topic = topic /*, planId = plan.id*/ )
    def apply( plan: OutlierPlan, ts: TimeSeriesBase ): Scope = apply( plan, ts.topic )

    trait ScopeIdentifying[T] { self: Identifying[T] =>
      override type ID = Scope
      override val evID: ClassTag[ID] = classTag[Scope]
      override val evTID: ClassTag[TID] = classTag[TaggedID[Scope]]
      override def nextId: TryV[TID] = new IllegalStateException( "scopes are fixed to plan:topic pairs so not generated" ).left
      override def fromString( idstr: String ): ID = {
        val Array(p, t) = idstr split ':'
        Scope( plan = p, topic = t )
      }
    }
  }


  implicit val outlierPlanIdentifying: EntityIdentifying[OutlierPlan] = {
    new EntityIdentifying[OutlierPlan] with ShortUUID.ShortUuidIdentifying[OutlierPlan] {
      override val evEntity: ClassTag[OutlierPlan] = classTag[OutlierPlan]
    }
  }


  import shapeless._

  override val idLens: Lens[OutlierPlan, OutlierPlan#TID] = new Lens[OutlierPlan, OutlierPlan#TID] {
    override def get( p: OutlierPlan ): OutlierPlan#TID = p.id
    override def set( p: OutlierPlan )( id: OutlierPlan#TID ): OutlierPlan = {
      SimpleOutlierPlan(
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

  override val nameLens: Lens[OutlierPlan, String] = new Lens[OutlierPlan, String] {
    override def get( p: OutlierPlan ): String = p.name
    override def set( p: OutlierPlan )( name: String ): OutlierPlan = {
      SimpleOutlierPlan(
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

  override val slugLens: Lens[OutlierPlan, String] = new Lens[OutlierPlan, String] {
    override def get( p: OutlierPlan ): String = p.slug
    override def set( p: OutlierPlan )( slug: String ): OutlierPlan = {
      SimpleOutlierPlan(
        id = p.id,
        name = slug,  // for outlier plan slug == name
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

  val isActiveLens: Lens[OutlierPlan, Boolean] = new Lens[OutlierPlan, Boolean] {
    override def get( p: OutlierPlan ): Boolean = p.isActive
    override def set( p: OutlierPlan )( a: Boolean ): OutlierPlan = {
      SimpleOutlierPlan(
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


  val AlgorithmConfig: String = "algorithm-config"

  type ExtractTopic = PartialFunction[Any, Option[Topic]]

  type Creator = () => V[Set[OutlierPlan]]

  final case class Grouping( limit: Int, window: FiniteDuration )

  def apply(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    grouping: Option[OutlierPlan.Grouping],
    planSpecification: Config
  )(
    appliesTo: (Any) => Boolean
  ): OutlierPlan = {
    SimpleOutlierPlan(
      id = outlierPlanIdentifying.safeNextId,
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
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    grouping: Option[OutlierPlan.Grouping],
    planSpecification: Config
  )(
    appliesTo: PartialFunction[Any, Boolean]
  ): OutlierPlan = {
    SimpleOutlierPlan(
      id = outlierPlanIdentifying.safeNextId,
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
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    grouping: Option[OutlierPlan.Grouping],
    planSpecification: Config,
    extractTopic: ExtractTopic,
    topics: Set[Topic]
  ): OutlierPlan = {
    SimpleOutlierPlan(
      id = outlierPlanIdentifying.safeNextId,
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
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    grouping: Option[OutlierPlan.Grouping],
    planSpecification: Config,
    extractTopic: ExtractTopic,
    topics: String*
  ): OutlierPlan = {
    SimpleOutlierPlan(
      id = outlierPlanIdentifying.safeNextId,
      name = name,
      appliesTo = AppliesTo.topics( topics.map{ Topic(_) }.toSet, extractTopic ),
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
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    grouping: Option[OutlierPlan.Grouping],
    planSpecification: Config,
    extractTopic: ExtractTopic,
    regex: Regex
  ): OutlierPlan = {
    SimpleOutlierPlan(
      id = outlierPlanIdentifying.safeNextId,
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
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    grouping: Option[OutlierPlan.Grouping],
    planSpecification: Config = ConfigFactory.empty
  ): OutlierPlan = {
    SimpleOutlierPlan(
      id = outlierPlanIdentifying.safeNextId,
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

  private def getAlgorithms( spec: Config ): Set[Symbol] = {
    import scala.collection.JavaConverters._
    if ( spec hasPath AlgorithmConfig ) spec.getConfig( AlgorithmConfig ).root.keySet.asScala.toSet.map{ a: String => Symbol(a) }
    else Set.empty[Symbol]
  }

  private def getAlgorithmConfig( spec: Config ): Config = {
    if ( spec hasPath AlgorithmConfig ) spec getConfig AlgorithmConfig
    else ConfigFactory.empty( s"no algorithm-config at spec[${spec.origin}]" )
  }


  final case class SimpleOutlierPlan private[outlier] (
    override val id: OutlierPlan#TID,
    override val name: String,
    override val appliesTo: OutlierPlan.AppliesTo,
    override val algorithms: Set[Symbol],
    override val grouping: Option[OutlierPlan.Grouping],
    override val timeout: FiniteDuration,
    override val isQuorum: IsQuorum,
    override val reduce: ReduceOutliers,
    override val algorithmConfig: Config,
    override private[outlier] val typeOrder: Int,
    override private[outlier] val originLineNumber: Int,
    override val isActive: Boolean = true
  ) extends OutlierPlan {
    override val summary: String = getClass.safeSimpleName + s"""(${name} ${appliesTo.toString})"""

    override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[SimpleOutlierPlan]

    override val toString: String = {
      getClass.safeSimpleName + s"[${id}](" +
        s"""name:[$name], ${appliesTo.toString}, quorum:[${isQuorum}], reduce:[${reduce}] """ +
        s"""algorithms:[${algorithms.mkString(",")}], timeout:[${timeout.toCoarsest}], """ +
        s"""grouping:[${grouping}], algorithm-config:[${algorithmConfig.root}]""" +
        ")"
    }
  }


  sealed trait AppliesTo extends ((Any) => Boolean) with Serializable

  object AppliesTo {
    def function( f: (Any) => Boolean ): AppliesTo = new AppliesTo {
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

      override val toString: String = s"""AppliesTo.topics[${topics.mkString(",")}]"""
    }

    def regex( regex: Regex, extractTopic: ExtractTopic ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Boolean = {
        if ( !extractTopic.isDefinedAt(message) ) false
        else {
          val result = extractTopic( message ) flatMap { t => regex findFirstMatchIn t.toString }
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


  implicit val outlierPlanOrdering = new Ordering[OutlierPlan] {
    override def compare( lhs: OutlierPlan, rhs: OutlierPlan ): Int = {
      val typeOrdering = Ordering[Int].compare( lhs.typeOrder, rhs.typeOrder )
      if ( typeOrdering != 0 ) typeOrdering
      else Ordering[Int].compare( lhs.originLineNumber, rhs.originLineNumber )
    }
  }
}
