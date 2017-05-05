package spotlight.analysis.algorithm

import scalaz._
import Scalaz._
import com.persist.logging._
import omnibus.commons.Valid
import AlgorithmIdentifier.SpanType
import bloomfilter.CanGenerateHashFrom
import demesne.AggregateRootType
import omnibus.commons.identifier.{ Identifying, ShortUUID }
import spotlight.model.outlier.AnalysisPlan
import spotlight.model.timeseries.Topic

/** Created by rolfsd on 2/1/17.
  */
case class AlgorithmIdentifier( planName: String, planId: String, spanType: SpanType, span: String ) extends Equals {
  override def hashCode(): Int = {
    41 * (
      41 * (
        41 * (
          41 + planId.##
        ) + planName.##
      ) + spanType.##
    ) + span.##
  }

  override def canEqual( rhs: Any ): Boolean = rhs.isInstanceOf[AlgorithmIdentifier]

  override def equals( rhs: Any ): Boolean = {
    rhs match {
      case that: AlgorithmIdentifier ⇒ {
        if ( this eq that ) true
        else {
          ( that.## == this.## ) &&
            ( that canEqual this ) &&
            ( this.planId == that.planId ) &&
            ( this.planName == that.planName ) &&
            ( this.spanType == that.spanType ) &&
            ( this.span == that.span )
        }
      }

      case _ ⇒ false
    }
  }

  override def toString: String = AlgorithmIdentifier toAggregateId this
}

object AlgorithmIdentifier extends ClassLogging {
  sealed trait SpanType {
    def label: String

    /** Depending on SpanType, produces the next identifer.
      * @param topic
      * @return
      */
    def nextIdFor( topic: Topic ): String
  }

  object SpanType {
    def from( spanRep: String ): Valid[SpanType] = {
      options
        .collectFirst { case st if st.label == spanRep ⇒ st.successNel[Throwable] }
        .getOrElse { Validation.failureNel( new IllegalStateException( s"unknown algorithm id span type:${spanRep}" ) ) }
    }

    lazy val options: Seq[SpanType] = Seq( TopicSpan, GroupSpan )
  }

  case object TopicSpan extends SpanType {
    override val label: String = "topic"

    /** Span identifier is the topic hint.
      * @param topic
      * @return
      */
    override def nextIdFor( topic: Topic ): String = topic.toString
  }

  case object GroupSpan extends SpanType {
    override val label: String = "group"
    override def nextIdFor( topic: Topic ): String = ShortUUID().toString
  }

  // <plan-name>@<plan-id>:<topic|group>:<span-identifier>
  // span-type is topic => span is topic
  // span-type is group => span is short uuid
  private lazy val IdFormat = s"""(.*)@(.*):(${AlgorithmIdentifier.SpanType.options.map { _.label }.mkString( "|" )}):(.*)""".r

  def toAggregateId( id: AlgorithmIdentifier ): String = id.planName + "@" + id.planId + ":" + id.spanType.label + ":" + id.span

  def fromAggregateId( aggregateId: String ): Valid[AlgorithmIdentifier] = {
    aggregateId match {
      case IdFormat( planName, planId, stype, span ) ⇒ {
        SpanType.from( stype )
          .map { spanType ⇒ AlgorithmIdentifier( planName = planName, planId = planId, spanType = spanType, span = span ) }
          .leftMap { exs ⇒
            log.error(
              Map(
                "@msg" → "failed to parse span type from algorithm aggregateId",
                "aggregateId" → aggregateId,
                "parsed" → Map(
                  "plan-name" → planName,
                  "plan-id" → planId,
                  "span-type" → stype,
                  "span" → span
                )
              )
            )
            exs
          }
      }

      case _ ⇒ {
        Validation.failureNel(
          new IllegalStateException(
            s"failed to parse algorithm aggregateId[${aggregateId}], which does not match expected format."
          )
        )
      }
    }
  }

  def nextId( planName: String, planId: String, spanType: SpanType, topic: Topic ): AlgorithmIdentifier = {
    AlgorithmIdentifier( planName = planName, planId = planId, spanType = spanType, span = spanType.nextIdFor( topic ) )
  }

  implicit val canGenerateHash: CanGenerateHashFrom[AlgorithmIdentifier] = new CanGenerateHashFrom[AlgorithmIdentifier] {
    import bloomfilter.CanGenerateHashFrom._

    override def generateHash( from: AlgorithmIdentifier ): Long = {
      41L * (
        41L * (
          41L * (
            41L + CanGenerateHashFromString.generateHash( from.planId )
          ) + CanGenerateHashFromString.generateHash( from.planName )
        ) + CanGenerateHashFromString.generateHash( from.spanType.label )
      ) + CanGenerateHashFromString.generateHash( from.span )
    }
  }
}

case class AlgorithmIdGenerator( planName: String, planId: AnalysisPlan#TID, algorithmRootType: AggregateRootType ) {
  import scala.language.existentials
  @transient private val identifying: Identifying.Aux[_, Algorithm.ID] = {
    algorithmRootType.identifying.asInstanceOf[Identifying.Aux[_, Algorithm.ID]]
  }

  def next(): Algorithm.TID = {
    identifying.tag(
      AlgorithmIdentifier(
        planName = planName,
        planId = planId.id.toString(),
        spanType = AlgorithmIdentifier.GroupSpan,
        span = ShortUUID().toString()
      )
    )
  }
}
