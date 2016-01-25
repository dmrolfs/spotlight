package lineup.train

import java.io.File
import java.util.concurrent.ExecutorService
import scala.concurrent.ExecutionContext
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import org.apache.avro.Schema
import org.apache.avro.file.{ CodecFactory, DataFileWriter }
import org.apache.avro.generic.{ GenericRecordBuilder, GenericDatumWriter, GenericRecord }
import org.apache.hadoop.conf.{ Configuration => HConfiguration }
import com.typesafe.config.ConfigFactory
import peds.commons.log.Trace
import lineup.model.timeseries.{ TimeSeriesCohort, DataPoint }


/**
  * Created by rolfsd on 1/19/16.
  */
class AvroFileTrainingRepositoryInterpreter()(implicit ec: ExecutionContext )
extends TrainingRepository.Interpreter
with AvroFileTrainingRepositoryInterpreter.AvroWriter {
  outer: AvroFileTrainingRepositoryInterpreter.WritersContextProvider =>

  import TrainingRepository._
  import AvroFileTrainingRepositoryInterpreter._

  implicit val pool: ExecutorService = executionContextToService( ec )

  override def step[Next]( action: TrainingProtocolF[TrainingProtocol[Next]] ): Task[TrainingProtocol[Next]] = {
    action match {
      case PutTimeSeries(series, next) => write.run( TimeSeriesCohort(series) ) map { _ => next }
      case PutTimeSeriesCohort(cohort, next) => write.run( cohort ) map { _ => next }
    }
  }

  override val genericRecords: Op[WritersContext, List[GenericRecord]] = {
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


  override val makeWritersContext: Op[TimeSeriesCohort, WritersContext] = {
    Kleisli[Task, TimeSeriesCohort, WritersContext] { cohort => Task { outer.context( cohort ) } }
  }

  override val makeWriter: Op[WritersContext, Writer] = Kleisli[Task, WritersContext, Writer] {ctx  =>
    Task fromDisjunction {
      \/ fromTryCatchNonFatal {
        if ( ctx.destination.exists ) makeDataWriter.appendTo( ctx.destination )
        else makeDataWriter.create( ctx.schema, ctx.destination )
      }
    }
  }

  override val writeRecords: Op[RecordsContext, Writer] = {
    Kleisli[Task, RecordsContext, Writer] { case RecordsContext(records, writer) =>
      Task {
        records foreach { r => writer append r }
        writer
      }
    }
  }

  override val closeWriter: Op[Writer, Unit] = Kleisli[Task, Writer, Unit] { writer => Task { writer.close() } }

  private val _timeseriesDatumWriter: GenericDatumWriter[GenericRecord] = {
    new GenericDatumWriter[GenericRecord]( outer.timeseriesSchema )
  }

  private def makeDataWriter: DataFileWriter[GenericRecord] = {
    new DataFileWriter[GenericRecord]( _timeseriesDatumWriter ).setCodec( CodecFactory.snappyCodec )
  }
}

object AvroFileTrainingRepositoryInterpreter {
  val trace = Trace[AvroFileTrainingRepositoryInterpreter.type]

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
      val avsc = getClass.getClassLoader.getResourceAsStream( "avro/timeseries.avsc" )
      new Schema.Parser().parse( avsc )
    }

    override def datapointSubSchema( tsSchema: Schema ): Schema = tsSchema.getField( "points" ).schema.getElementType

    val trainingHome: String = {
      val Home = "lineup.training.home"
      val config = ConfigFactory.load().withFallback( ConfigFactory parseString s"${Home}: ." )
      config getString Home
    }

    import better.files.{ File => BFile }
    BFile( trainingHome ).createIfNotExists( asDirectory = true )

    override def destination: File = {
      import org.joda.{ time => joda }
      import better.files._
      val formatter = joda.format.DateTimeFormat forPattern "yyyyMMddTHH"
      val suffix = formatter print joda.DateTime.now
      file"${trainingHome}/timeseries-${suffix}.avro".toJava
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
        writer <- makeWriter
        records <- genericRecords
      } yield RecordsContext( records, writer )
    }

    def genericRecords: Op[WritersContext, List[GenericRecord]]

    def makeWriter: Op[WritersContext, Writer]

    def writeRecords: Op[RecordsContext, Writer]

    def closeWriter: Op[Writer, Unit]
  }
}
