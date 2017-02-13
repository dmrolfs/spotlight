package spotlight.serialization.kryo

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.typesafe.config.{ Config, ConfigFactory, ConfigRenderOptions }

/** Created by rolfsd on 11/1/16.
  */
class KryoConfigSerializer() extends Serializer[Config] {
  override val isImmutable: Boolean = true

  override def read( kryo: Kryo, input: Input, clazz: Class[Config] ): Config = {
    val hocon = kryo.readObject( input, classOf[String] )
    ConfigFactory.parseString( hocon )
  }

  override def write( kryo: Kryo, output: Output, config: Config ): Unit = {
    val hocon = config.root().render( ConfigRenderOptions.concise )
    kryo.writeObjectOrNull( output, hocon, classOf[String] )
  }
}
