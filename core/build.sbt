import Dependencies._

name := "lineup-core"

description := "lorem ipsum."

libraryDependencies ++= commonDependencies ++ Seq(
  akkaModule( "actor" ),
  peds( "akka" ),
  //  "com.github.melrief" %% "pureconfig" % "0.1.2",
  "com.typesafe.akka" % "akka-stream-experimental_2.11" % "1.0",
  "org.apache.commons" % "commons-math3" % "3.5",
  "com.github.dmrolfs" %% "demesne-core" % "0.8.0-SNAPSHOT" % "compile" changing(),
  "com.typesafe.akka" % "akka-stream-testkit-experimental_2.11" % "1.0" % "test",
  "com.github.dmrolfs" %% "demesne-testkit" % "0.8.0-SNAPSHOT" % "test" changing(),
  "com.github.marklister" %% "product-collections" % "1.4.2" % "test"
)
