package lineup.app

import java.io.{ File => JFile }
import java.net.Socket

import scala.concurrent.duration._
import scala.util.{ Success, Try }

import better.files._
import com.github.nscala_time.time.Imports.{ richDateTime, richSDuration }
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math3.random.RandomDataGenerator
import org.joda.{ time => joda }
import org.parboiled2._
import peds.commons.log.Trace
import resource._
import scopt.OptionParser
import lineup.model.timeseries._
import lineup.protocol.PythonPickleProtocol


/**
  * Created by rolfsd on 11/23/15.
  */
object SendStats extends StrictLogging {
  val trace = Trace[SendStats.type]

  def main( args: Array[String] ): Unit = {
    val settings = Settings.parser.parse( args, Settings() ) getOrElse Settings()
    val sourceFileName = settings.source.map{ f: File => f.toJava.getCanonicalPath } getOrElse "<NO SOURCE>"
    val usageMessage = s"""
      |\nRunning SendStats using the following configuration:
      |\tbinding  : ${settings.host}:${settings.port}
      |\tsource   : ${sourceFileName}
    """.stripMargin
    System.out.println( usageMessage )

    if ( settings.source.isEmpty ) System.exit(-1)

    val source = settings.source map { Grammar.DataParser( _ ) } getOrElse Seq.empty[Try[TimeSeries]]
    val count = source count { _.isSuccess }
    logger.info( s"Sending ${count} data elements from ${sourceFileName}")

    if ( count > 0 ) {
      for {
        connection <- managed( new Socket(settings.host, settings.port) )
        outStream <- managed( connection.getOutputStream )
      } {
        logger.info(
          s"Sending to ${settings.host}:${settings.port} [${source.collect{case Success(d) => d.points.size}.sum}] data points"
        )
        val pickler = new PythonPickleProtocol

        for {
          line <- source
          data <- line
          message = pickler.pickleTimeSeries( data )
        } {
          outStream write message.toArray
          outStream.flush
        }
      }
    }
  }


  object Grammar {
    object DataParser {
      def apply( input: File ): Seq[Try[TimeSeries]] = {
        input.lines.toSeq.map{ DataParser( _ ).InputLine.run() }//.collect{ case Success(d) => d }
      }
    }

    case class DataParser( input: ParserInput ) extends Parser with StringBuilding {
      import CharPredicate.HexDigit

//      def file = rule { oneOrMore(TimeSeriesData).separatedBy(NL) ~ EOI }

      def InputLine = rule { TimeSeriesData }

      def TimeSeriesData = rule {
        "SimpleTimeSeries" ~
        TopicLabel ~
        oneOrMore( Point ).separatedBy( ws(',') ) ~
        ws(']') ~> { (t, pts) =>
          TimeSeries( topic = t, points = pts.toIndexedSeq )
        }
      }

      def TopicLabel: Rule1[Topic] = rule { ':' ~ Label ~ '[' ~> (Topic(_)) }
      def Label: Rule1[String] = rule { '"' ~ clearSB() ~ Characters ~ ws('"') ~ push(sb.toString) }

      def Point: Rule1[DataPoint] = rule { ws('(') ~ Timestamp ~ ws(',') ~ WhiteSpace ~ DoubleValue ~ ws(')') ~> ( (ts: joda.DateTime, v: Double) =>
        DataPoint( timestamp = ts, value = v ) )
      }
      def Timestamp: Rule1[joda.DateTime] = rule { capture(Digits) ~> { secondsFromEpoch: String =>
        new joda.DateTime( secondsFromEpoch.toLong * 1000L ) }
      }
      def Characters = rule { zeroOrMore(NormalChar | '\\' ~ EscapedChar) }
      def NormalChar = rule { !QuoteBackslash ~ ANY ~ appendSB() }
      def EscapedChar = rule (
        QuoteSlashBackSlash ~ appendSB()
        | 'b' ~ appendSB('\b')
        | 'f' ~ appendSB('\f')
        | 'n' ~ appendSB('\n')
        | 'r' ~ appendSB('\r')
        | 't' ~ appendSB('\t')
        | Unicode ~> { code => sb.append( code.asInstanceOf[Char] ); () }
      )

      def Unicode = rule { 'u' ~ capture(HexDigit ~ HexDigit ~ HexDigit ~ HexDigit) ~> ( java.lang.Integer.parseInt(_, 16) ) }

      def DoubleValue = rule { capture( Integer ~ optional(Frac) ~ optional(Exp) ) ~> (_.toDouble) }
      def Integer = rule { optional('-') ~ ( CharPredicate.Digit19 ~ Digits | CharPredicate.Digit ) }
      def Frac = rule { "." ~ Digits }
      def Exp = rule { ignoreCase('e') ~ optional(anyOf("+-")) ~ Digits }
      def Digits = rule { oneOrMore( CharPredicate.Digit ) }
      def WhiteSpace = rule {zeroOrMore( WhiteSpaceChar ) }
      def ws( c: Char ) = rule { c ~ WhiteSpace }
//      val ws = rule[Char]() { _ ~ WhiteSpace } for post2.1 parboiled2 version
      def NL = rule { optional('\r') ~ '\n' }
      val WhiteSpaceChar = CharPredicate( " \n\r\t\f" )
      val QuoteBackslash = CharPredicate("\"\\")
      val QuoteSlashBackSlash = QuoteBackslash ++ "/"
    }
  }

  case class Settings( host: String = "127.0.0.1", port: Int = 2004, source: Option[File] = None )
  object Settings {
    def parser = new OptionParser[Settings]( "SendStats" ) {
      head( "SendStats", "0.1.0" )

      opt[String]( 'h', "host" ) action { (e, c) =>
        c.copy( host = e )
      } text( "address to host" )

      opt[Int]( 'p', "port" ) action { (e, c) =>
        c.copy( port = e )
      } text( "address port" )

      arg[JFile]("<file>") action { (f, c) => c.copy( source = Some(f.toScala) ) } text("source data file")
    }
  }


  def makeDataPoints(
    values: Row[Double],
    start: joda.DateTime = joda.DateTime.now,
    period: FiniteDuration = 1.second,
    wiggleFactor: (Double, Double) = (1.0, 1.0)
  ): Row[DataPoint] = {
    val secs = start.getMillis / 1000L
    val epochStart = new joda.DateTime( secs * 1000L )
    val random = new RandomDataGenerator
    def nextFactor: Double = {
      if ( wiggleFactor._1 == wiggleFactor._2 ) wiggleFactor._1
      else random.nextUniform( wiggleFactor._1, wiggleFactor._2 )
    }

    values.zipWithIndex map { vi =>
      val (v, i) = vi
      val adj = (i * nextFactor) * period
      val ts = epochStart + adj.toJodaDuration
      DataPoint( timestamp = ts, value = v )
    }
  }

  val points: Row[Double] = Row(
    9.46,
    9.9,
    11.6,
    14.5,
    17.3,
    19.2,
    18.4,
    14.5,
    12.2,
    10.8,
    8.58,
    8.36,
    8.58,
    7.5,
    7.1,
    7.3,
    7.71,
    8.14,
    8.14,
    7.1,
    7.5,
    7.1,
    7.1,
    7.3,
    7.71,
    8.8,
    9.9,
    14.2,
    18.8,
    25.2,
    31.5,
    22,
    24.1,
    39.2
  )
}
