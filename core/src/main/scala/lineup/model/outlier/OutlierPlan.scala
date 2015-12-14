package lineup.model.outlier

import scala.concurrent.{ Future, ExecutionContext }
import scala.concurrent.duration._
import scala.util.matching.Regex
import com.typesafe.config.{ ConfigOrigin, ConfigFactory, Config }
import lineup.model.timeseries.{TimeSeriesBase, Topic}
import peds.commons.log.Trace
import peds.commons.util._


/**
 * Created by rolfsd on 10/4/15.
 */
sealed trait OutlierPlan {
  def name: String
  def appliesTo: OutlierPlan.AppliesTo
  def algorithms: Set[Symbol]
  def timeout: FiniteDuration
  def isQuorum: IsQuorum
  def reduce: ReduceOutliers
  def algorithmConfig: Config

  private[outlier] def origin: ConfigOrigin
  private[outlier] def typeOrder: Int
}

object OutlierPlan {
  val trace = Trace[OutlierPlan.type]

  val AlgorithmConfig = "algorithm-config"


  type ExtractTopic = PartialFunction[Any, Option[Topic]]

//  def apply(
//    name: String,
//    timeout: FiniteDuration,
//    isQuorum: IsQuorum,
//    reduce: ReduceOutliers,
//    algorithms: Set[Symbol],
//    specification: Config
//  )(
//    appliesTo: (Any) => Boolean
//  ): OutlierPlan = {
//    SimpleOutlierPlan(
//      name = name,
//      appliesTo = AppliesTo.predicate( appliesTo ),
//      algorithms = algorithms,
//      timeout = timeout,
//      isQuorum = isQuorum,
//      reduce = reduce,
//      algorithmConfig = getAlgorithmConfig( specification ),
//      origin = specification.origin,
//      typeOrder = 3
//    )
//  }
//
//  def apply(
//    name: String,
//    timeout: FiniteDuration,
//    isQuorum: IsQuorum,
//    reduce: ReduceOutliers,
//    algorithms: Set[Symbol],
//    specification: Config
//  )(
//    appliesTo: PartialFunction[Any, Boolean]
//  ): OutlierPlan = {
//    SimpleOutlierPlan(
//      name = name,
//      appliesTo = AppliesTo.partialPredicate( appliesTo ),
//      algorithms = algorithms,
//      timeout = timeout,
//      isQuorum = isQuorum,
//      reduce = reduce,
//      algorithmConfig = getAlgorithmConfig( specification ),
//      origin = specification.origin,
//      typeOrder = 3
//    )
//  }

  def apply(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    specification: Config
  )(
    appliesTo: (Any) => Applicability
  ): OutlierPlan = {
    SimpleOutlierPlan(
      name = name,
      appliesTo = AppliesTo.function( appliesTo ),
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( specification ),
      origin = specification.origin,
      typeOrder = 3
    )
  }

  def apply(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    specification: Config
  )(
    appliesTo: PartialFunction[Any, Applicability]
  ): OutlierPlan = {
    SimpleOutlierPlan(
      name = name,
      appliesTo = AppliesTo.partialFunction( appliesTo ),
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( specification ),
      origin = specification.origin,
      typeOrder = 3
    )
  }

  def forTopics(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    specification: Config,
    extractTopic: ExtractTopic,
    topics: Set[Topic]
  ): OutlierPlan = {
    SimpleOutlierPlan(
      name = name,
      appliesTo = AppliesTo.topics( topics, extractTopic ),
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( specification ),
      origin = specification.origin,
      typeOrder = 1
    )
  }

  def forTopics(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    specification: Config,
    extractTopic: ExtractTopic,
    topics: String*
  ): OutlierPlan = {
    SimpleOutlierPlan(
      name = name,
      appliesTo = AppliesTo.topics( topics.map{ Topic(_) }.toSet, extractTopic ),
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( specification ),
      origin = specification.origin,
      typeOrder = 1
    )
  }

  //todo: change to blacklist and whitelist and dont forget about precendence between the two (whitelist before blacklist)
  def forRegex(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    specification: Config,
    extractTopic: ExtractTopic,
    regex: Regex
  ): OutlierPlan = {
    SimpleOutlierPlan(
      name = name,
      appliesTo = AppliesTo.regex( regex, extractTopic ),
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( specification ),
      origin = specification.origin,
      typeOrder = 2
    )
  }

  def forBlock( name: String, specification: Config, extractTopic: ExtractTopic, regex: Regex ): OutlierPlan = {
    BlockingPlan(
      name = name,
      appliesTo = AppliesTo.block( regex, extractTopic ),
      algorithmConfig = getAlgorithmConfig( specification ),
      origin = specification.origin,
      typeOrder = Int.MaxValue - 1
    )
  }

  def default(
    name: String,
    timeout: FiniteDuration,
    isQuorum: IsQuorum,
    reduce: ReduceOutliers,
    algorithms: Set[Symbol],
    specification: Config = ConfigFactory.empty
  ): OutlierPlan = {
    SimpleOutlierPlan(
      name = name,
      appliesTo = AppliesTo.all,
      algorithms = algorithms,
      timeout = timeout,
      isQuorum = isQuorum,
      reduce = reduce,
      algorithmConfig = getAlgorithmConfig( specification ),
      origin = specification.origin,
      typeOrder = Int.MaxValue
    )
  }

  private def getAlgorithmConfig( spec: Config ): Config = {
    if ( spec hasPath AlgorithmConfig ) spec getConfig AlgorithmConfig
    else ConfigFactory.empty( s"no algorithm-config at spec[${spec.origin}]" )
  }


  final case class SimpleOutlierPlan private[outlier] (
    override val name: String,
    override val appliesTo: OutlierPlan.AppliesTo,
    override val algorithms: Set[Symbol],
    override val timeout: FiniteDuration,
    override val isQuorum: IsQuorum,
    override val reduce: ReduceOutliers,
    override val algorithmConfig: Config,
    override private[outlier] val origin: ConfigOrigin,
    override private[outlier] val typeOrder: Int
  ) extends OutlierPlan {
    override val toString: String = {
      getClass.safeSimpleName + "(" +
        s"""name:[$name], ${appliesTo.toString} timeout:[${timeout.toCoarsest}], """ +
        s"""algorithms:[${algorithms.mkString(",")}], algorithm-config:[${algorithmConfig.root}]""" +
        ")"
    }
  }

  final case class BlockingPlan private[outlier](
    override val name: String,
    override val appliesTo: OutlierPlan.AppliesTo,
    override val algorithmConfig: Config,
    override private[outlier] val origin: ConfigOrigin,
    override private[outlier] val typeOrder: Int
  ) extends OutlierPlan {
    override def algorithms: Set[Symbol] = Set.empty[Symbol]

    override def reduce: ReduceOutliers = new ReduceOutliers {
      override def apply(
        results: SeriesOutlierResults,
        source: TimeSeriesBase
      )(
        implicit ec: ExecutionContext
      ): Future[Outliers] = Future { NoOutliers(algorithms, source) }
    }

    override def isQuorum: IsQuorum = new IsQuorum {
      override def totalIssued: Int = 0
      override def apply( results: SeriesOutlierResults ): Boolean = false
    }

    override def timeout: FiniteDuration = 1.second
  }


  sealed trait Applicability
  case object Applies extends Applicability
  case object NotApplicable extends Applicability
  case object NoFurtherConsideration extends Applicability
  implicit def booleanToApplicability( isApplicable: Boolean ): Applicability = if ( isApplicable ) Applies else NotApplicable
  implicit def applicabilityToBoolean( a: Applicability): Boolean = { a == Applies }

  sealed trait AppliesTo extends ((Any) => Applicability)

  private object AppliesTo {
    def predicate( p: (Any) => Boolean ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Applicability = p( message )
      override val toString: String = "AppliesTo.predicate"
    }

    def partialPredicate( pp: PartialFunction[Any, Boolean] ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Applicability = if ( pp.isDefinedAt(message) ) pp(message) else NotApplicable
      override val toString: String = "AppliesTo.partialPredicate"
    }

    def function( f: (Any) => Applicability ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Applicability = f( message )
      override val toString: String = "AppliesTo.function"
    }

    def partialFunction( pf: PartialFunction[Any, Applicability] ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Applicability = if ( pf isDefinedAt message ) pf( message ) else NotApplicable
      override val toString: String = "AppliesTo.partialFunction"
    }

    def topics( topics: Set[Topic], extractTopic: ExtractTopic ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Applicability = {
        if ( extractTopic isDefinedAt message ) {
          extractTopic( message ) map { t => booleanToApplicability( topics.contains(t) ) } getOrElse NotApplicable
        } else {
          NotApplicable
        }
      }

      override val toString: String = s"""AppliesTo.topics[${topics.mkString(",")}]"""
    }

    def regex( regex: Regex, extractTopic: ExtractTopic ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Applicability = {
        if ( !extractTopic.isDefinedAt(message) ) false
        else {
          val result = extractTopic( message ) flatMap { t => regex findFirstMatchIn t.toString }
          result.isDefined
        }
      }

      override val toString: String = s"AppliesTo.regex[$regex]"
    }

    def block( regex: Regex, extractTopic: ExtractTopic ): AppliesTo = new AppliesTo {
      override def apply( message: Any ): Applicability = {
        if ( !extractTopic.isDefinedAt(message) ) NotApplicable
        else {
          val result = extractTopic( message ) flatMap { t => regex findFirstMatchIn t.toString }
          if ( result.isDefined ) NoFurtherConsideration else NotApplicable
        }
      }

      override val toString: String = s"AppliesTo.regex[$regex]"
    }

    val all: AppliesTo = new AppliesTo {
      override def apply( message: Any ): Applicability = Applies
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
