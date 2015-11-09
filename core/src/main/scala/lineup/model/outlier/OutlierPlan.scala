package lineup.model.outlier

import com.typesafe.config.{ConfigFactory, Config}
import lineup.model.timeseries.Topic
import peds.commons.log.Trace

import scala.concurrent.duration._
import shapeless._
import peds.commons.util._

import scala.util.matching.Regex


/**
 * Created by rolfsd on 10/4/15.
 */
final case class OutlierPlan private[outlier] (
  name: String,
  appliesTo: OutlierPlan.AppliesTo,
  algorithms: Set[Symbol],
  timeout: FiniteDuration,
  isQuorum: IsQuorum,
  reduce: ReduceOutliers,
  algorithmConfig: Config
) {
  override def toString: String = {
    getClass.safeSimpleName + "(" +
      s"""name:[$name], timeout:[${timeout.toCoarsest}], """ +
      s"""algorithms:[${algorithms.mkString(",")}], algorithmConfig:[${algorithmConfig.root}]""" +
    ")"
  }
}

object OutlierPlan {
  val trace = Trace[OutlierPlan.type]
  type AppliesTo = (Any) => Boolean
  type ExtractTopic = PartialFunction[Any, Option[Topic]]

  def apply(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    algorithmConfig: Config
  )(
    appliesTo: AppliesTo
  ): OutlierPlan = {
    OutlierPlan(
      name = name,
      appliesTo = appliesTo,
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = algorithmConfig
    )
  }

  def apply(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    algorithmConfig: Config
  )(
    appliesTo: PartialFunction[Any, Boolean]
  ): OutlierPlan = {
    OutlierPlan(
      name = name,
      appliesTo = appliesTo,
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = algorithmConfig
    )
  }

  def forTopics(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    algorithmConfig: Config,
    extractTopic: ExtractTopic,
    topics: Set[Topic]
  ): OutlierPlan = {
    OutlierPlan(
      name = name,
      appliesTo = appliesToTopics( topics, extractTopic ),
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = algorithmConfig
    )
  }

//  def forTopics(
//    name: String,
//    timeout: FiniteDuration,
//    isQuorum: IsQuorum,
//    reduce: ReduceOutliers,
//    algorithms: Set[Symbol],
//    algorithmConfig: Config,
//    extractTopic: ExtractTopic,
//    topics: String*
//  ): OutlierPlan = {
//    OutlierPlan(
//      name = name,
//      appliesTo = appliesToTopics( topics.map{ Topic(_) }.toSet, extractTopic ),
//      algorithms = algorithms,
//      timeout = timeout,
//      isQuorum = isQuorum,
//      reduce = reduce,
//      algorithmConfig = algorithmConfig
//    )
//  }

  def forRegex(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    algorithmConfig: Config,
    extractTopic: ExtractTopic,
    regex: Regex
  ): OutlierPlan = {
    OutlierPlan(
      name = name,
      appliesTo = appliesToRegex( regex, extractTopic ),
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = algorithmConfig
    )
  }

  def default(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    algorithmConfig: Config = ConfigFactory.empty
  ): OutlierPlan = {
    OutlierPlan(
      name = name,
      appliesTo = defaultAppliesTo,
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = algorithmConfig
    )
  }

  private def appliesToPF( pf: PartialFunction[Any, Boolean] ): AppliesTo = (message: Any) => {
    trace.briefBlock( s"appliesToPF($message)" ) {
      if ( pf isDefinedAt message ) pf( message ) else false
    }
  }

  private def appliesToTopics( topics: Set[Topic], extractTopic: ExtractTopic ): AppliesTo = {
    (message: Any) => {
      trace.block( s"appliesToTopics(${message.getClass.safeSimpleName}(${extractTopic(message)}))" ){
        trace( s"""topics:[${topics.mkString(",")}]""" )
        if ( extractTopic isDefinedAt message ) extractTopic( message ) map { topics contains _ } getOrElse { false }
        else false
      }
    }
  }

  private def appliesToRegex( regex: Regex, extractTopic: ExtractTopic ): AppliesTo = {
    (message: Any) => {
      trace.block( s"appliesToRegex(${message.getClass.safeSimpleName}(${extractTopic(message)}))" ){
        trace( s"""regex:[${regex}]""" )

        if ( !extractTopic.isDefinedAt(message) ) false
        else {
          val result = extractTopic( message ) flatMap { t => regex findFirstMatchIn t.toString }
          result.isDefined
        }
      }
    }
  }

  private val defaultAppliesTo: AppliesTo = (message: Any) => { trace.briefBlock(s"defaultAppliesTo($message)"){ true } }
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
