package spotlight.serialization.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging


/**
  * Created by rolfsd on 11/1/16.
  */
class KryoConfigSerializer() extends Serializer[Config] with StrictLogging {
  override val isImmutable: Boolean = true

  override def read( kryo: Kryo, input: Input, clazz: Class[Config] ): Config = {
    val hocon = kryo.readObject( input, classOf[String] )
    logger.warn( "KryoConfigSerializer reading config: [{}]", hocon )
    ConfigFactory.parseString( hocon )
  }

  override def write( kryo: Kryo, output: Output, config: Config ): Unit = {
//    throw new IllegalStateException( "IN KRYO CONFIG SERIALIZER write" )
    val hocon = config.root().render( ConfigRenderOptions.concise )
    logger.warn( "KryoConfigSerializer writing config: [{}]", hocon )
    kryo.writeObjectOrNull( output, hocon, classOf[String] )
  }
}
