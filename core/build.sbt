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
//  "net.razorvine" % "pyrolite" % "4.10",
  //  "com.github.melrief" %% "pureconfig" % "0.1.2",
  "com.typesafe.akka" % "akka-stream-experimental_2.11" % "1.0",
  "org.apache.commons" % "commons-math3" % "3.5",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.github.dmrolfs" %% "demesne-core" % "0.8.0-SNAPSHOT" % "compile" changing(),
  "com.github.pathikrit" %% "better-files" % "2.13.0",
  "com.typesafe.akka" % "akka-stream-testkit-experimental_2.11" % "1.0" % "test",
  "com.github.dmrolfs" %% "demesne-testkit" % "0.8.0-SNAPSHOT" % "test" changing(),
  "com.github.marklister" %% "product-collections" % "1.4.2" % "test"
)

libraryDependencies += ( "org.python" % "jython" % "2.7.0" )
//                       .exclude( "com.google.guava", "guava" )
//                       .exclude( "org.fusesource.hawtjni", "hawtjni-runtime" )

resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven"

libraryDependencies += "org.velvia" % "msgpack4s_2.11" % "0.5.1"

mainClass in (Compile, run) := Some("lineup.stream.GraphiteModel")

mainClass in assembly := Some("lineup.stream.GraphiteModel")

assemblyJarName in assembly := s"${organizationName.value}-${name.value}-${version.value}.jar"

test in assembly := {}  //DMR: REMOVE ONCE JYTHON FIXED

assemblyMergeStrategy in assembly := {
  case x if Assembly.isConfigFile(x) => MergeStrategy.concat

  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) => MergeStrategy.rename


  case PathList( "META-INF", "maven", "com.google.guava", "guava", "pom.properties" ) => MergeStrategy.filterDistinctLines
  case PathList( "META-INF", "maven", "com.google.guava", "guava", "pom.xml" ) => MergeStrategy.filterDistinctLines

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

assemblyShadeRules in assembly := Seq(
  ShadeRule.zap( "org.fusesource.hawtjni.runtime.Callback" ).inLibrary( "org.python" % "jython" % "2.7.0"),
  ShadeRule.zap( "org.fusesource.hawtjni.runtime.JNIEnv" ).inLibrary( "org.python" % "jython" % "2.7.0"),
  ShadeRule.zap( "org.fusesource.hawtjni.runtime.Library" ).inLibrary( "org.python" % "jython" % "2.7.0"),
  ShadeRule.zap( "org.fusesource.hawtjni.runtime.PointerMath" ).inLibrary( "org.python" % "jython" % "2.7.0")
)

docker <<= ( docker dependsOn assembly )

dockerfile in docker := {
  val artifact = ( assemblyOutputPath in assembly ).value
  val artifactTargetPath = s"/app/${artifact.name}"
  new Dockerfile {
    from( "java:8" )
    run( "apt-get", "update" )
    run( "apt-get", "-y", "install", "tmux" )
    copy( artifact, artifactTargetPath )
    entryPoint( "java", "-jar", artifactTargetPath )
    expose( 2004 )
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
