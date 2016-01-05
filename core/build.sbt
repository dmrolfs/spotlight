import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport.MergeStrategy


name := "lineup-core"

description := "lorem ipsum."

libraryDependencies ++= commonDependencies ++
  metrics.all ++
  facility.betterFiles.all ++
  Seq(
    //  facility.pureConfig,
    akka.streams,
    facility.math3,
    facility.scopt,
    demesne.core,
    facility.parboiled,
    facility.pyrolite
  ) ++ Dependencies.test(
    akka.streamsTestkit,
    demesne.testkit,
    "org.python" % "jython-standalone" % "2.5.3",
    "com.github.marklister" %% "product-collections" % "1.4.2"
  )

libraryDependencies += ( "com.jsuereth" %% "scala-arm" % "2.0.0-M1" )

resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven"

libraryDependencies += "org.velvia" % "msgpack4s_2.11" % "0.5.1"

mainClass in (Compile, run) := Some("lineup.stream.GraphiteModel")

mainClass in assembly := Some("lineup.stream.GraphiteModel")

assemblyJarName in assembly := s"${organizationName.value}-${name.value}-${version.value}.jar"

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
      case x => MergeStrategy.deduplicate
    }
  }

  case _ => MergeStrategy.deduplicate
}

docker <<= ( docker dependsOn assembly )

dockerfile in docker := {
  val artifact = ( assemblyOutputPath in assembly ).value
  val artifactTargetPath = s"/app/${artifact.name}"
  val mainclass = mainClass.in( Compile, run ).value.getOrElse( sys.error("Expected exactly one main class") )

  new Dockerfile {
    from( "java:8" )
    run( "apt-get", "update" )
    run( "apt-get", "-y", "install", "tmux" )
    copy( artifact, artifactTargetPath )

    entryPoint(
      "java",
//      "-Dcom.sun.management.jmxremote",
//      "-Dcom.sun.management.jmxremote.ssl=false",
//      "-Dcom.sun.management.jmxremote.authenticate=false",
//      "-Dcom.sun.management.jmxremote.port=9010",
//      "-Dcom.sun.management.jmxremote.rmi.port=9110",
//      "-Dcom.sun.management.jmxremote.local.only=false",
////      "-Djava.rmi.server.hostname='192.168.99.100'",
      "-cp", "/etc/lineup:" + artifactTargetPath,
      mainclass
    )

    env( "LOG_HOME", "/var/log" )
    env( "CONFIG_HOME", "/etc/lineup" )
    expose( 2004 )

    expose( 22 )
//    expose( 9010 )
//    expose( 9110 )
  }
}

imageNames in docker := Seq(
  ImageName( s"dmrolfs/${name.value}:latest" ), // Sets the latest tag
  ImageName(
    namespace = Some( "dmrolfs" ),
    repository = name.value,
    tag = Some( "v" + version.value )
  ) // Sets a name with a tag that contains the project version
)
