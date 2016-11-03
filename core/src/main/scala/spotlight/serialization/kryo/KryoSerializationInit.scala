package spotlight.serialization.kryo

import com.esotericsoftware.kryo.Kryo
import com.typesafe.scalalogging.LazyLogging
import org.joda.{time => joda}
import de.javakaffee.kryoserializers.jodatime.{JodaDateTimeSerializer, JodaLocalDateSerializer, JodaLocalDateTimeSerializer}


/**
  * Created by rolfsd on 11/1/16.
  */
class KryoSerializationInit extends LazyLogging {
  def customize( kryo: Kryo ): Unit = {
    logger.warn( "Customizing Kryo..." )

    kryo.addDefaultSerializer( classOf[joda.DateTime], classOf[JodaDateTimeSerializer] )
    kryo.addDefaultSerializer( classOf[joda.LocalDate], classOf[JodaLocalDateSerializer] )
    kryo.addDefaultSerializer( classOf[joda.LocalDateTime], classOf[JodaLocalDateTimeSerializer] )

    kryo.addDefaultSerializer( classOf[com.typesafe.config.Config], classOf[KryoConfigSerializer] )
    kryo.addDefaultSerializer( Class.forName("com.typesafe.config.impl.SimpleConfig"), classOf[KryoConfigSerializer] )
    logger.warn( "Kryo: graph-context:[{}]", kryo.getGraphContext )
    logger.warn( "Kryo: depth:[{}] context:[{}]", kryo.getDepth.toString, kryo.getContext )
    logger.warn( "...Kryo Customized" )
  }
}
