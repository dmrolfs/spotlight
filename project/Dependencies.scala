import sbt.Keys._
import sbt._

object Dependencies {
  val logbackVersion = "1.1.3"

  val loggingDependencies = Seq(
    "org.slf4j" % "slf4j-api" % "1.7.13",
    "ch.qos.logback" % "logback-core" % logbackVersion,
    "ch.qos.logback" % "logback-classic" % logbackVersion
  )

  val commonDependencies = loggingDependencies ++ Seq(
    "com.eaio.uuid" % "uuid" % "3.4",
    "com.typesafe" % "config" % "1.3.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "com.chuusai" %% "shapeless" % "2.2.5",
    "org.scalaz" %% "scalaz-core" % "7.2.0",
    // "org.typelevel" %% "scalaz-outlaws" % "0.2",
//    "org.typelevel" %% "scalaz-contrib-210"        % "0.2",
//    "org.typelevel" %% "scalaz-contrib-validation" % "0.2",
//    "org.typelevel" %% "scalaz-contrib-undo"       % "0.2",
    // currently unavailable because there's no 2.11 build of Lift yet
    // "org.typelevel" %% "scalaz-lift"               % "0.2",
//    "org.typelevel" %% "scalaz-nscala-time"        % "0.2",
//    "org.typelevel" %% "scalaz-spire"              % "0.2",
    peds( "commons" ),
    peds( "archetype" ),
    "joda-time" % "joda-time" % "2.9",
    "org.joda" % "joda-convert" % "1.7",
    "com.github.nscala-time" %% "nscala-time" % "2.4.0",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.mockito" % "mockito-core" % "1.10.19" % "test"
  )

  val defaultDependencyOverrides = Set(
    "org.scalaz" %% "scalaz-core" % "7.2.0"
  )

  val sprayJson = "io.spray" %% "spray-json" % "1.3.1"

  def akkaModule( id: String ) = "com.typesafe.akka" %% s"akka-$id" % "2.4.1"
  def peds( id: String ) = "com.github.dmrolfs" %% s"peds-$id" % "0.1.6" % "compile" changing()
}
