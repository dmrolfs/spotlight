package spotlight.app

import scala.annotation.tailrec
import org.apache.avro.Schema
import org.apache.avro.file.{ SeekableFileInput, DataFileReader }
import org.apache.avro.generic.{ GenericDatumReader, GenericRecord }
import com.typesafe.scalalogging.StrictLogging
import better.files._


/**
  * Created by rolfsd on 1/24/16.
  */
object ReadAvro extends StrictLogging{
  val dr = new GenericDatumReader[GenericRecord]

  def main( args: Array[String] ): Unit = {
    @tailrec def loop( r: DataFileReader[GenericRecord], acc: Int = 0 ): Int = {
      if ( !r.hasNext ) acc
      else {
        val h = r.next()
        println( h.toString )
        loop( r, acc + 1 )
      }
    }

    logger info s"${args.mkString}"
    val avsc = getClass.getClassLoader.getResourceAsStream( "avro/timeseries.avsc" )
    val schema = new Schema.Parser().parse( avsc )
    val target = file"${args(0)}"
    val input = new SeekableFileInput( target.toJava )
    val reader = new DataFileReader[GenericRecord]( input, dr )
    val items = loop( reader  )
    logger info s"Read [${items}] items"
  }
}
