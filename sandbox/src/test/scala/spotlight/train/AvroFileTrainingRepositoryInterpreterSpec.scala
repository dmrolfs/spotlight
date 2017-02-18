package spotlight.train

import com.typesafe.config.{ ConfigFactory, Config }

import scalaz.concurrent.Task
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import better.files.{ File => BFile, _ }
import scala.annotation.tailrec
import org.apache.avro.generic.GenericRecord
import org.mockito.invocation.InvocationOnMock
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.stubbing.Answer
import omnibus.commons.log.Trace
import org.apache.hadoop.conf.{ Configuration => HConfiguration }
import org.apache.avro.Schema
import org.apache.avro.file._
import org.apache.avro.generic._
import org.apache.avro.mapred.FsInput
import org.apache.avro.util.Utf8
import org.apache.hadoop.fs._
import org.joda.{ time => joda }
import spotlight.model.timeseries._


/**
  * Created by rolfsd on 1/21/16.
  */

object AvroFileTrainingRepositoryInterpreterSpec {
  val index = new AtomicInteger( 0 )

  val TimeSeriesSchema =
    """
      |{
      |  "namespace": "spotlight.train",
      |  "name": "TimeSeries",
      |  "type": "record",
      |  "fields": [
      |    { "name": "topic", "type": "string" },
      |    {
      |      "name": "points",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "name": "DataPoint",
      |          "type": "record",
      |          "fields": [
      |            { "name": "timestamp", "type": "long" },
      |            { "name": "value", "type": "double" }
      |          ]
      |        }
      |      }
      |    }
      |  ]
      |}
    """.stripMargin
}

class AvroFileTrainingRepositoryInterpreterSpec
extends fixture.WordSpec
with ParallelTestExecution
with MustMatchers
with MockitoSugar {
  import AvroFileTrainingRepositoryInterpreterSpec._

  val trace = Trace[AvroFileTrainingRepositoryInterpreterSpec]

  type Fixture = TestFixture
  override type FixtureParam = Fixture

  class TestFixture { outer =>
    implicit val schema = new Schema.Parser().parse( TimeSeriesSchema )

    val writer = mock[DataFileWriter[GenericRecord]]

    val hadoopConfiguration: HConfiguration = {
      val conf = new HConfiguration
      conf.setBoolean( "dfs.support.append", true )
//      conf.set( "fs.defaultFS", "hdfs://localhost" )
//      conf.setInt( "dfs.replication", 1 )
      conf
    }

    val fileSystem: FileSystem = FileSystem get hadoopConfiguration

    val dateFormatter = joda.format.ISODateTimeFormat.basicOrdinalDateTimeNoMillis

    val destination = file"./log/test-training-${dateFormatter.print(joda.Instant.now)}-${index.incrementAndGet()}.avro"
//    val destination: File = {
//      BFile( s"./log/test-training-${dateFormatter.print(joda.Instant.now)}-${index.incrementAndGet()}.avro" ).toJava
//    }

    def input( path: Path, conf: HConfiguration ): SeekableInput = new FsInput( path, conf )

    trait TestWritersContextProvider extends AvroFileTrainingRepositoryInterpreter.WritersContextProvider {
      override def hadoopConfiguration: HConfiguration = outer.hadoopConfiguration
      override lazy val timeseriesSchema: Schema = outer.schema
      override def datapointSubSchema(tsSchema: Schema): Schema = tsSchema.getField( "points" ).schema.getElementType
//      override def fileSystem: FileSystem = outer.fileSystem
//      override def outPath: Path = outer.destination
      //    def fileSystem: FileSystem = FileSystem get hadoopConfiguration
      override def outFile: File = outer.destination.toJava
    }

    implicit val ec = scala.concurrent.ExecutionContext.global

    val interpreter = new AvroFileTrainingRepositoryInterpreter( ) with TestWritersContextProvider {
      override def config: Config = {
        val fallback = """
         |spotlight.training {
         |  archival: on
         |  home: .
         |}
        """.stripMargin

        ConfigFactory.load().withFallback( ConfigFactory parseString fallback )
      }
    }


    def recordToSeries( r: GenericRecord ): TimeSeries = {
      import scala.collection.JavaConverters._

      val topic = r.get( "topic" ).asInstanceOf[Utf8].toString
      val points = r.get( "points" ).asInstanceOf[java.util.List[GenericRecord]].asScala map { p =>
        val t = p.get( "timestamp" ).asInstanceOf[Long]
        val v = p.get( "value" ).asInstanceOf[Double]
        DataPoint( timestamp = new joda.DateTime(t), value = v )
      }

      TimeSeries( topic, points.toIndexedSeq )
    }

    def seriesToRecord( ts: TimeSeries )( implicit s: Schema ): GenericRecord = {
      import scala.collection.JavaConverters._

      val result = new GenericRecordBuilder( s )
      result.set( "topic", new Utf8(ts.topic.toString) )

      val dpSchema = s.getField( "points" ).schema.getElementType
      val dps = for {
        DataPoint( t: joda.DateTime, v: Double ) <- ts.points
      } yield {
        val dp = new GenericRecordBuilder( dpSchema )
        dp.set( "timestamp", t.getMillis )
        dp.set( "value", v )
        dp.build
      }

      result.set( "points", dps.asJava )
      result.build
    }

    def writeSeries( ts: TimeSeries ): Task[Unit] = interpreter.write.run( TimeSeriesCohort(ts) )

    def readDestination( dest: BFile )( implicit sc: Schema ): List[GenericRecord] = {
      @tailrec def loop( st: DataFileStream[GenericRecord], acc: Seq[GenericRecord] ): List[GenericRecord] = {
        if ( !st.hasNext ) acc.toList
        else {
          val n = st.next()
          loop( st, acc :+ n )
        }
      }

      val dr = new GenericDatumReader[GenericRecord]( sc )
//      val stream = new DataFileReader[GenericRecord]( input(destination, hadoopConfiguration), dr )
      val stream = new DataFileReader[GenericRecord]( dest.toJava, dr )
      val result = loop( stream, Nil )
      stream.close()
      result
    }

    def cleanup(): Unit = {
      trace( s"CLEANING destination: [${destination.toString}]")
//      fileSystem.delete( destination, false )
      import better.files.Cmds._
      rm( destination )
    }

    val ts1 = TimeSeries(
      "series.one",
      Seq(
        DataPoint( new joda.DateTime("2015-10-15T10:45:28.595-07:00"), 0.8034021258073109 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:29.256-07:00" ), 0.631788600222419 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:29.917-07:00"), 0.9305270594056617 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:30.578-07:00"), 0.2585858044780553 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:31.239-07:00"), 0.766876744801969 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:31.900-07:00"), 0.7151072123356189 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:32.561-07:00"), 0.8149155488047959 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:33.222-07:00"), 0.6033127884838968 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:33.883-07:00"), 0.9573058709427263 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:34.544-07:00"), 0.1634853616052577 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:35.205-07:00"), 0.5470315925842234 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:35.866-07:00"), 0.9911521171959665 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:36.527-07:00"), 0.0350783910956784 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:37.188-07:00"), 0.135391590295268 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:37.849-07:00"), 0.4682428302903453 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:38.510-07:00"), 0.9959659286881288 )
      )
    )

    val ts2 = TimeSeries(
      "series.two",
      Seq(
        DataPoint( new joda.DateTime("2015-10-15T10:45:28.596-07:00"), 0.0160711903220879 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:29.596-07:00"), 0.0632516286548765 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:30.596-07:00"), 0.2370034405095286 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:31.596-07:00"), 0.7233312387846997 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:32.596-07:00"), 0.8004926311315658 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:33.596-07:00"), 0.6388167145425149 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:34.596-07:00"), 0.9229196790544877 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:35.596-07:00"), 0.2845557802737965 )
      )
    )

    val ts3 = TimeSeries(
      "series.three",
      Seq(
        DataPoint( new joda.DateTime("2015-10-15T10:45:28.596-07:00"), 0.6567099579829515 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:30.596-07:00"), 0.9017679562759263 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:32.596-07:00"), 0.3543300372394615 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:34.596-07:00"), 0.9151210477973734 )
      )
    )

    val ts4 = TimeSeries(
      "series.four",
      Seq(
        DataPoint( new joda.DateTime("2015-10-15T10:45:28.597-07:00"), 0.1443239738645753 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:29.264-07:00"), 0.4939782577300506 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:29.931-07:00"), 0.9998549544801372 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:30.598-07:00"), 5.800979266398E-4 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:31.265-07:00"), 0.0023190456521412 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:31.932-07:00"), 0.009254670717618 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:32.599-07:00"), 0.0366760871501057 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:33.266-07:00"), 0.1413238071258542 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:33.933-07:00"), 0.4854055546612341 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:34.600-07:00"), 0.999148008661015 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:35.267-07:00"), 0.0034050617989728 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:35.934-07:00"), 0.0135738694124718 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:36.601-07:00"), 0.0535584779265798 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:37.268-07:00"), 0.2027598694750713 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:37.935-07:00"), 0.6465932192220935 ),
        DataPoint( new joda.DateTime("2015-10-15T10:45:38.602-07:00"), 0.914041712312413 )
      )
    )
  }

  def createTestFixture(): Fixture = new Fixture

  override def withFixture( test: OneArgTest ): Outcome = {
    val fixture = createTestFixture()
    try {
      val result = test( fixture )
      if ( result.isSucceeded || result.isPending ) fixture.cleanup()
      result
    } finally {
    }
  }

  object WIP extends Tag( "wip" )


  "TrainingRepositoryAvroInterpreter" should {
    "series and record converters are associative" in { f: Fixture =>
      import f._
      val gr1 = seriesToRecord( ts1 )
      val actual = recordToSeries( gr1 )
      actual mustBe ts1
    }

    "make valid writer" in { f: Fixture =>
      import f._
      val w = interpreter.makeWriter.run( interpreter.context( TimeSeriesCohort(ts1) ) ).unsafePerformSync
      w.append( seriesToRecord(ts1) )
      w.close()

      val actual = readDestination( destination ) map { recordToSeries }
      actual mustBe List(ts1)
    }

    "close writer" in { f: Fixture =>
      import f._
      val w = mock[AvroFileTrainingRepositoryInterpreter.Writer]
      interpreter.closeWriter.run( w ).unsafePerformSync
      verify( w ).close()
    }

    "convert time series into avro's generic record" in { f: Fixture =>
      import f._
      val expected = seriesToRecord( ts1 )
      val actual = interpreter.genericRecords.run( interpreter.context( TimeSeriesCohort(ts1) ) ).unsafePerformSync
      actual mustBe List(expected)
    }

    "write once" in { f: Fixture =>
      import f._

      val expected = seriesToRecord( ts1 )
      val task = writeSeries( ts1 )
      task.unsafePerformSync
      val actual = readDestination( destination )
      actual mustBe List(expected)
    }

    "write multiple" taggedAs (WIP) in { f: Fixture =>
      import f._
trace("A")
      val e1 = seriesToRecord( ts1 )
      val t1 = writeSeries( ts1 )
      t1.unsafePerformSync
      val a1 = readDestination( destination )
      a1 mustBe List(e1)

trace("B")
      val e2 = seriesToRecord( ts2 )
      val t2 = writeSeries( ts2 )
      t2.unsafePerformSync
      val a2 = readDestination( destination )
      a2 mustBe List(e1, e2)

trace("C")
      val e3 = seriesToRecord( ts3 )
      val t3 = writeSeries( ts3 )
      t3.unsafePerformSync
      val a3 = readDestination( destination )
      a3 mustBe List(e1, e2, e3)
    }

    "write time series cohort" in { f: Fixture =>
      import f._
      import scalaz._, Scalaz._
      trace.block( "test cohort write" ) {
      val cohort = TimeSeriesCohort( ts1, ts2, ts3, ts4 )
      val expected = cohort.data map { seriesToRecord }
      val task = interpreter.write.run( cohort )
      task.unsafePerformSync
      val actual = readDestination( destination )
      actual mustBe expected
      }
    }
  }
}
