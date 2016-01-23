package lineup.train

import java.io.File
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import org.apache.avro.Schema
import org.apache.avro.file.{ CodecFactory, DataFileWriter }
import org.apache.avro.generic.{ GenericRecordBuilder, GenericDatumWriter, GenericRecord }
import org.apache.hadoop.conf.{ Configuration => HConfiguration }
import peds.commons.log.Trace
import lineup.model.timeseries.{ TimeSeriesCohort, DataPoint, Row }


/**
  * Created by rolfsd on 1/19/16.
  */
class TrainingRepositoryAvroInterpreter()
extends TrainingRepository.Interpreter
with TrainingRepositoryAvroInterpreter.AvroWriter { outer: TrainingRepositoryAvroInterpreter.WritersContextProvider =>

  import TrainingRepository._
  import TrainingRepositoryAvroInterpreter._

  override def step[Next]( action: TrainingProtocolF[TrainingProtocol[Next]] ): Task[TrainingProtocol[Next]] = {
    action match {
      case PutTimeSeries(series, next) => write.run( TimeSeriesCohort(series) ) map { _ => next }
      case PutTimeSeriesCohort(cohort, next) => write.run( cohort ) map { _ => next }
    }
  }

  override def genericRecords: Op[WritersContext, List[GenericRecord]] = {
    Kleisli[Task, WritersContext, List[GenericRecord]] { ctx =>
      import scala.collection.JavaConverters._

      Task {
        val dpSchema = outer.datapointSubSchema( ctx.schema )

        ctx.cohort.data.toList map { ts =>
          val record = new GenericRecordBuilder( ctx.schema )
          record.set( "topic", ts.topic.toString ) // record topic per individual series rt generalized cohort

          val dps = ts.points map { case DataPoint(t, v) =>
            val dp = new GenericRecordBuilder( dpSchema )
            dp.set( "timestamp", t.getMillis )
            dp.set( "value", v )
            dp.build()
          }

          record.set( "points", dps.asJava )
          record.build()
        }
      }
    }
  }


  override def makeWritersContext: Op[TimeSeriesCohort, WritersContext] = {
    Kleisli[Task, TimeSeriesCohort, WritersContext] { cohort => Task { outer.context( cohort ) } }
  }

  override def makeWriter: Op[WritersContext, Writer] = Kleisli[Task, WritersContext, Writer] {ctx  =>
    Task fromDisjunction {
      \/ fromTryCatchNonFatal {
        if ( ctx.destination.exists ) _timeseriesDataWriterFactory.appendTo( ctx.destination )
        else _timeseriesDataWriterFactory.create( ctx.schema, ctx.destination )
      }
    }
  }

  override def writeRecords: Op[RecordsContext, Writer] = {
    Kleisli[Task, RecordsContext, Writer] { case RecordsContext(records, writer) =>
      Task {
        records foreach { r => writer append r }
        writer
      }
    }
  }

  override def closeWriter: Op[Writer, Unit] = Kleisli[Task, Writer, Unit] { writer => Task { writer.close() } }

  private val _timeseriesDatumWriter: GenericDatumWriter[GenericRecord] = {
    new GenericDatumWriter[GenericRecord]( outer.timeseriesSchema )
  }

  private val _timeseriesDataWriterFactory: DataFileWriter[GenericRecord] = {
    new DataFileWriter[GenericRecord]( _timeseriesDatumWriter ).setCodec( CodecFactory.snappyCodec )
  }
}

object TrainingRepositoryAvroInterpreter {
  val trace = Trace[TrainingRepositoryAvroInterpreter.type]

  case class WritersContext( cohort: TimeSeriesCohort, schema: Schema, destination: File )

  trait WritersContextProvider {
    def hadoopConfiguration: HConfiguration
    def timeseriesSchema: Schema
    def datapointSubSchema( tsSchema: Schema ): Schema
    def destination: File
    def context( cohort: TimeSeriesCohort ): WritersContext = WritersContext( cohort, schema = timeseriesSchema, destination )
  }

  trait SimpleWritersContextProvider extends WritersContextProvider {
    override def hadoopConfiguration: HConfiguration = new HConfiguration

    override lazy val timeseriesSchema: Schema = {
      val avsc = getClass.getClassLoader.getResourceAsStream( "timeseries.avsc" )
      new Schema.Parser().parse( avsc )
    }

    override def datapointSubSchema( tsSchema: Schema ): Schema = tsSchema.getField( "points" ).schema.getElementType

    override def destination: File = {
      import better.files._
      import org.joda.{ time => joda }
      val formatter = joda.format.DateTimeFormat forPattern "yyyyMMddHH"
      val suffix = formatter print joda.DateTime.now
      file"timeseries-${suffix}.avro".toJava
    }
  }


  type Writer = DataFileWriter[GenericRecord]

  case class RecordsContext( records: List[GenericRecord], writer: Writer )


  trait AvroWriter {
    type Op[I, O] = Kleisli[Task, I, O]

    def write: Op[TimeSeriesCohort, Unit] = makeWritersContext >==> makeRecordsContext >==> writeRecords >==> closeWriter

    def makeWritersContext: Op[TimeSeriesCohort, WritersContext]

    def makeRecordsContext: Op[WritersContext, RecordsContext] = {
      for {
        w <- makeWriter
        records <- genericRecords
      } yield RecordsContext( records, w )
    }

    def genericRecords: Op[WritersContext, List[GenericRecord]]

    def makeWriter: Op[WritersContext, Writer]

    def writeRecords: Op[RecordsContext, Writer]

    def closeWriter: Op[Writer, Unit]
  }
}
