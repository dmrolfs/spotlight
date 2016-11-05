package spotlight.serialization.kryo

import com.esotericsoftware.kryo.Kryo
import org.joda.{time => joda}
import de.javakaffee.kryoserializers.jodatime.{JodaDateTimeSerializer, JodaLocalDateSerializer, JodaLocalDateTimeSerializer}


/**
  * Created by rolfsd on 11/1/16.
  */
class KryoSerializationInit {
  def customize( kryo: Kryo ): Unit = {
    kryo.addDefaultSerializer( classOf[joda.DateTime], classOf[JodaDateTimeSerializer] )
    kryo.addDefaultSerializer( classOf[joda.LocalDate], classOf[JodaLocalDateSerializer] )
    kryo.addDefaultSerializer( classOf[joda.LocalDateTime], classOf[JodaLocalDateTimeSerializer] )

    kryo.addDefaultSerializer( classOf[com.typesafe.config.Config], classOf[KryoConfigSerializer] )
    kryo.addDefaultSerializer( Class.forName("com.typesafe.config.impl.SimpleConfig"), classOf[KryoConfigSerializer] )
  }
}
