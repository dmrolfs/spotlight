import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport.MergeStrategy
//import sbtdocker.DockerKeys._


//import sbtassembly.{MergeStrategy, Assembly}

name := "lineup-core"

description := "lorem ipsum."

libraryDependencies ++= commonDependencies ++ Seq(
  akkaModule( "actor" ),
  akkaModule( "testkit" ) % "test",
  peds( "akka" ),
  //  "com.github.melrief" %% "pureconfig" % "0.1.2",
  "com.typesafe.akka" % "akka-stream-experimental_2.11" % "1.0",
  "org.apache.commons" % "commons-math3" % "3.5",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.github.dmrolfs" %% "demesne-core" % "0.8.0-SNAPSHOT" % "compile" changing(),
  "com.typesafe.akka" % "akka-stream-testkit-experimental_2.11" % "1.0" % "test",
  "com.github.dmrolfs" %% "demesne-testkit" % "0.8.0-SNAPSHOT" % "test" changing(),
  "com.github.marklister" %% "product-collections" % "1.4.2" % "test"
)

mainClass in (Compile, run) := Some("lineup.stream.GraphiteModel")

mainClass in assembly := Some("lineup.stream.GraphiteModel")

assemblyJarName in assembly := "cdkLineup.jar"

assemblyMergeStrategy in assembly := {
  case x if Assembly.isConfigFile(x) => MergeStrategy.concat

  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) => MergeStrategy.rename

  case PathList("META-INF", xs @ _*) => {
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard

      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") => MergeStrategy.discard

      case "plexus" :: xs => MergeStrategy.discard
      case "services" :: xs => MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) => MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  }

  case _ => MergeStrategy.deduplicate
}

docker <<= (docker dependsOn assembly)

dockerfile in docker := {
  val artifact = ( assemblyOutputPath in assembly ).value
  val artifactTargetPath = s"/app/${artifact.name}"
  new Dockerfile {
    from( "java:8" )
//    add( artifact, artifactTargetPath )
    copy( artifact, artifactTargetPath )
    entryPoint( "java", "-jar", artifactTargetPath )
  }
}

imageNames in docker := Seq(
  ImageName( s"${organization.value}/${name.value}:latest" ), // Sets the latest tag
  ImageName(
    namespace = Some( organization.value ),
    repository = name.value,
    tag = Some( "v" + version.value )
  ) // Sets a name with a tag that contains the project version
)
