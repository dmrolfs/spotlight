package spotlight.model.outlier

import scala.concurrent.duration._
import scala.reflect._
import scala.util.matching.Regex
import com.typesafe.config.{Config, ConfigFactory, ConfigOrigin}
import peds.archetype.domain.model.core.{Entity, EntityIdentifying, EntityLensProvider}
import peds.commons._
import peds.commons.identifier.{ShortUUID, TaggedID}
import peds.commons.util._
import spotlight.model.timeseries.Topic


/**
 * Created by rolfsd on 10/4/15.
 */
sealed trait OutlierPlan extends Entity with Equals {
  override type ID = ShortUUID

  def appliesTo: OutlierPlan.AppliesTo
  def algorithms: Set[Symbol]
  def grouping: Option[OutlierPlan.Grouping]
  def timeout: FiniteDuration
  def isQuorum: IsQuorum
  def reduce: ReduceOutliers
  def algorithmConfig: Config
  def summary: String

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

  private[outlier] def origin: ConfigOrigin
  private[outlier] def typeOrder: Int
}

object OutlierPlan extends EntityLensProvider[OutlierPlan] {
  import shapeless._

  val outlierPlanIdentifying: EntityIdentifying[OutlierPlan] = {
    new EntityIdentifying[OutlierPlan] with ShortUUID.ShortUuidIdentifying[OutlierPlan] {
      override val evEntity: ClassTag[OutlierPlan] = classTag[OutlierPlan]
    }
  }

  def nextId(): OutlierPlan#TID = outlierPlanIdentifying.safeNextId
//  override val idTag: Symbol = 'outlierPlan
//  override implicit def tag( id: OutlierPlan#ID ): OutlierPlan#TID = TaggedID( idTag, id )

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
        origin = p.origin,
        typeOrder = p.typeOrder
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
        origin = p.origin,
        typeOrder = p.typeOrder
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
        origin = p.origin,
        typeOrder = p.typeOrder
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
      id = nextId,
      name = name,
      appliesTo = AppliesTo.function( appliesTo ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      origin = planSpecification.origin,
      typeOrder = 3
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
      id = nextId,
      name = name,
      appliesTo = AppliesTo.partialFunction( appliesTo ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      origin = planSpecification.origin,
      typeOrder = 3
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
      id = nextId,
      name = name,
      appliesTo = AppliesTo.topics( topics, extractTopic ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      origin = planSpecification.origin,
      typeOrder = 1
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
      id = nextId,
      name = name,
      appliesTo = AppliesTo.topics( topics.map{ Topic(_) }.toSet, extractTopic ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      origin = planSpecification.origin,
      typeOrder = 1
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
      id = nextId,
      name = name,
      appliesTo = AppliesTo.regex( regex, extractTopic ),
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      origin = planSpecification.origin,
      typeOrder = 2
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
      id = nextId,
      name = name,
      appliesTo = AppliesTo.all,
      algorithms = algorithms ++ getAlgorithms( planSpecification ),
      grouping = grouping,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( planSpecification ),
      origin = planSpecification.origin,
      typeOrder = Int.MaxValue
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
    override private[outlier] val origin: ConfigOrigin,
    override private[outlier] val typeOrder: Int
  ) extends OutlierPlan {
    override val evID: ClassTag[ID] = classTag[ShortUUID]
    override val evTID: ClassTag[TID] = classTag[TaggedID[ShortUUID]]

    override def slug: String = name

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


  sealed trait AppliesTo extends ((Any) => Boolean)

  private object AppliesTo {
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
      else Ordering[Int].compare( lhs.origin.lineNumber, rhs.origin.lineNumber )
    }
  }
}



//case class OutlierPlan(
//  name: String,
//  algorithms: Set[Symbol],
//  timeout: FiniteDuration,
//  isQuorum: IsQuorum,
//  reduce: ReduceOutliers,
//  algorithmProperties: Map[String, Any] = Map()
//) {
//  override def toString: String = s"${getClass.safeSimpleName}($name)"
//}
//
//object OutlierPlan {
//  val nameLens: Lens[OutlierPlan, String] = lens[OutlierPlan] >> 'name
//  val algorithmsLens: Lens[OutlierPlan, Set[Symbol]] = lens[OutlierPlan] >> 'algorithms
//  val timeoutLens: Lens[OutlierPlan, FiniteDuration] = lens[OutlierPlan] >> 'timeout
//  val isQuorumLens: Lens[OutlierPlan, IsQuorum] = lens[OutlierPlan] >> 'isQuorum
//  val reduceLens: Lens[OutlierPlan, ReduceOutliers] = lens[OutlierPlan] >> 'reduce
//  val algorithmPropertiesLens: Lens[OutlierPlan, Map[String, Any]] = lens[OutlierPlan] >> 'algorithmProperties
//}


//trait OutlierPlan extends Entity {
//  override type ID = ShortUUID
//  override def idClass: Class[_] = classOf[ShortUUID]
//
//  def algorithms: Set[String]
//  def algorithmProperties: Map[String, Any]
//  def timeout: FiniteDuration
//  def isQuorum: IsQuorum
//  def reduce: ReduceOutliers
//}
//
//object OutlierPlan extends EntityCompanion[OutlierPlan] {
//  override def idTag: Symbol = 'outlierPlan
//  override def nextId: OutlierPlan#TID = ShortUUID()
//  override implicit def tag( id: OutlierPlan#ID ): OutlierPlan#TID = TaggedID( idTag, id )
//
//  def apply(
//    id: OutlierPlan#TID,
//    name: String,
//    slug: String,
//    algorithms: Set[String],
//    timeout: FiniteDuration,
//    isQuorum: IsQuorum,
//    reduce: ReduceOutliers,
//    algorithmProperties: Map[String, Any] = Map()
//  ): V[OutlierPlan] = {
//    ( checkId(id) |@| checkAlgorithms(algorithms) ) { (i, a) =>
//      SimpleOutlierPlan(
//        id = i,
//        name = name,
//        slug = slug,
//        algorithms = a,
//        timeout = timeout,
//        isQuorum = isQuorum,
//        reduce = reduce,
//        algorithmProperties = algorithmProperties
//      )
//    }
//  }
//
//  private def checkId( id: OutlierPlan#TID ): V[OutlierPlan#TID] = {
//    if ( id.tag != idTag ) InvalidIdError( id ).failureNel
//    else id.successNel
//  }
//
//  private def checkAlgorithms( algorithms: Set[String] ): V[Set[String]] = {
//    if ( algorithms.isEmpty ) InvalidAlgorithmError( algorithms ).failureNel
//    else algorithms.successNel
//  }
//
//
//  override def idLens: Lens[OutlierPlan, OutlierPlan#TID] = new Lens[OutlierPlan, OutlierPlan#TID] {
//    override def get( p: OutlierPlan ): OutlierPlan#TID = p.id
//    override def set( p: OutlierPlan )( i: OutlierPlan#TID ): OutlierPlan = {
//      OutlierPlan(
//        id = i,
//        name = p.name,
//        slug = p.slug,
//        algorithms = p.algorithms,
//        timeout = p.timeout,
//        isQuorum = p.isQuorum,
//        reduce = p.reduce,
//        algorithmProperties = p.algorithmProperties
//      ) valueOr { exs => throw exs.head }
//    }
//  }
//
//  override def nameLens: Lens[OutlierPlan, String] = new Lens[OutlierPlan, String] {
//    override def get( p: OutlierPlan ): String = p.name
//    override def set( p: OutlierPlan)( n: String ): OutlierPlan = {
//      OutlierPlan(
//        id = p.id,
//        name = n,
//        slug = p.slug,
//        algorithms = p.algorithms,
//        timeout = p.timeout,
//        isQuorum = p.isQuorum,
//        reduce = p.reduce,
//        algorithmProperties = p.algorithmProperties
//      ) valueOr { exs => throw exs.head }
//    }
//  }
//
//  def slugLens: Lens[OutlierPlan, String] = new Lens[OutlierPlan, String] {
//    override def get( p: OutlierPlan ): String = p.slug
//    override def set( p: OutlierPlan)( s: String ): OutlierPlan = {
//      OutlierPlan(
//        id = p.id,
//        name = p.name,
//        slug = s,
//        algorithms = p.algorithms,
//        timeout = p.timeout,
//        isQuorum = p.isQuorum,
//        reduce = p.reduce,
//        algorithmProperties = p.algorithmProperties
//      ) valueOr { exs => throw exs.head }
//    }
//  }
//
//
//  final case class SimpleOutlierPlan private[outlier](
//    override val id: OutlierPlan#TID,
//    override val name: String,
//    override val slug: String,
//    override val algorithms: Set[String],
//    override val timeout: FiniteDuration,
//    override val isQuorum: IsQuorum,
//    override val reduce: ReduceOutliers,
//    override val algorithmProperties: Map[String, Any] = Map()
//  ) extends OutlierPlan with Equals {
//    override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[SimpleOutlierPlan]
//
//    override def equals( rhs: Any ): Boolean = rhs match {
//      case that: SimpleOutlierPlan => {
//        if ( this eq that ) true
//        else {
//          ( that.## == this.## ) &&
//          ( that canEqual this ) &&
//          ( this.id == that.id )
//        }
//      }
//
//      case _ => false
//    }
//
//    override def hashCode: Int = {
//      41 * (
//        41 + id.##
//      )
//    }
//
//    override def toString: String = s"${getClass.safeSimpleName}($name)"
//  }
//}
