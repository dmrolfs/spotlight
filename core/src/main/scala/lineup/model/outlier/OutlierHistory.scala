package lineup.model.outlier

import lineup.model.timeseries.Topic
import shapeless._
import scalaz.{ Lens => _, _ }, Scalaz._
import peds.commons.V
import peds.archetype.domain.model.core.{ EntityCompanion, Entity }
import peds.commons.identifier.TaggedID


trait OutlierHistory extends Entity {
  override type ID = Topic
  override def idClass: Class[_] = classOf[Topic]
  def outlierAnnotations: Seq[OutlierAnnotation]
}

object OutlierHistory {

  val idTag: Symbol = 'outlierHistory
  implicit def tag( id: OutlierHistory#ID ): OutlierHistory#TID = TaggedID( idTag, id )

  def apply( topic: Topic, outlierAnnotations: Seq[OutlierAnnotation] ): V[OutlierHistory] = {
    SimpleOutlierHistory( id = topic, outlierAnnotations = outlierAnnotations ).successNel
  }

  def apply( series: Outliers ): V[OutlierHistory] = {
    val annotations = OutlierAnnotation annotationsFromSeries series
    annotations map { a => SimpleOutlierHistory( id = series.topic, outlierAnnotations = a ) }
  }

  val idLens: Lens[OutlierHistory, OutlierHistory#TID] = new Lens[OutlierHistory, OutlierHistory#TID] {
    override def get( h: OutlierHistory ): OutlierHistory#TID = h.id
    override def set( h: OutlierHistory )( id: OutlierHistory#TID ): OutlierHistory = {
      SimpleOutlierHistory( id, h.outlierAnnotations )
    }
  }

  val topicLens: Lens[OutlierHistory, Topic] = new Lens[OutlierHistory, Topic] {
    override def get( h: OutlierHistory ): Topic = h.id
    override def set( h: OutlierHistory )( t: Topic ): OutlierHistory = SimpleOutlierHistory( t, h.outlierAnnotations )
  }

  val nameLens: Lens[OutlierHistory, String] = new Lens[OutlierHistory, String] {
    override def get( h: OutlierHistory ): String = topicLens.get( h ).name
    override def set( h: OutlierHistory)( n: String ): OutlierHistory = topicLens.set( h )( n )
  }

  val outlierAnnotationsLens: Lens[OutlierHistory, Seq[OutlierAnnotation]] = new Lens[OutlierHistory, Seq[OutlierAnnotation]] {
    override def get( h: OutlierHistory ): Seq[OutlierAnnotation] = h.outlierAnnotations
    override def set( h: OutlierHistory )( os: Seq[OutlierAnnotation] ): OutlierHistory = SimpleOutlierHistory( h.id, os )
  }


  case class SimpleOutlierHistory(
    override val id: OutlierHistory#TID,
    override val outlierAnnotations: Seq[OutlierAnnotation]
  ) extends OutlierHistory {
    override val name: String = id.get.name
  }
}
