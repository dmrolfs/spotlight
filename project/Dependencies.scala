import sbt.Keys._
import sbt._

object Dependencies {
  val commonDependencies = Seq(
    "com.eaio.uuid" % "uuid" % "3.4",
    "com.typesafe" % "config" % "1.3.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "com.chuusai" %% "shapeless" % "2.2.5",
    "org.scalaz" %% "scalaz-core" % "7.1.4",
    // "org.typelevel" %% "scalaz-outlaws" % "0.2",
    "org.typelevel" %% "scalaz-contrib-210"        % "0.2",
    "org.typelevel" %% "scalaz-contrib-validation" % "0.2",
    "org.typelevel" %% "scalaz-contrib-undo"       % "0.2",
    // currently unavailable because there's no 2.11 build of Lift yet
    // "org.typelevel" %% "scalaz-lift"               % "0.2",
    "org.typelevel" %% "scalaz-nscala-time"        % "0.2",
    "org.typelevel" %% "scalaz-spire"              % "0.2",
    peds( "commons" ),
    peds( "archetype" ),
    "joda-time" % "joda-time" % "2.8.2",
    "org.joda" % "joda-convert" % "1.7",
    "com.github.nscala-time" %% "nscala-time" % "2.2.0",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.mockito" % "mockito-core" % "1.10.19" % "test"
  )
  
  val defaultDependencyOverrides = Set(
      // "org.scala-lang" % "scala-reflect" % "2.11.4",
      // // "com.google.guava" % "guava" % "15.0",
      // "org.scala-lang" % "scala-library" % "2.11.4",
      // "org.scala-lang" % "scalap" % "2.11.4",
      // "org.scala-lang" % "scala-compiler" % "2.11.4",
      // "org.scala-lang" % "scala-xml" % "2.11.4",
      // "joda-time" % "joda-time" % "2.8.2",
      // "com.chuusai" %% "shapeless" % "2.2.5",
      "org.scalaz" %% "scalaz-core" % "7.1.4"
      // "org.slf4j" % "slf4j-api" % "1.7.10",
      // "org.parboiled" %% "parboiled-scala" % "1.1.7",
      // "com.typesafe" % "config" % "1.2.1",
      // "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
      // "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      // "org.mockito" % "mockito-core" % "1.9.5" % "test"
    )

  // val slf4j = "org.slf4j" % "slf4j-api" % "1.7.7"
  // val logbackVersion = "1.1.2"
  // val logbackCore = "ch.qos.logback" % "logback-core" % logbackVersion
  // val logbackClassic = "ch.qos.logback" % "logback-classic" % logbackVersion
  // val loggingImplementations = Seq( logbackCore, logbackClassic )

  val sprayJson = "io.spray" %% "spray-json" % "1.3.1"

  val scopt = "com.github.scopt" %% "scopt" % "3.3.0"

  def akkaModule( id: String ) = "com.typesafe.akka" %% s"akka-$id" % "2.3.11"
  def sprayModule( id: String ) = "io.spray" %% s"spray-$id" % "1.3.3"
  def peds( id: String ) = "com.github.dmrolfs" %% s"peds-$id" % "0.1.6" % "compile" changing()
}
