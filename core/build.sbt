import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport.MergeStrategy


name := "lineup-core"

description := "lorem ipsum."

libraryDependencies ++=
  commonDependencies ++
  metrics.all ++
  facility.betterFiles.all ++
  facility.avro.all ++
  Seq(
    //  facility.pureConfig,
    akka.streams,
    facility.math3,
    facility.scopt,
    demesne.core,
    facility.parboiled,
    facility.hadoopClient,
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

testOptions in Test += Tests.Argument( "-oDF" )

mainClass in (Compile, run) := Some("lineup.app.GraphiteLineup")

mainClass in assembly := Some("lineup.stream.GraphiteModel")

assemblyJarName in assembly := s"${organizationName.value}-${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList( "org", "hyperic", "sigar", xs @ _* ) => MergeStrategy.last
  case PathList( "org", "apache", "commons", "beanutils", xs @ _* ) => MergeStrategy.last
  case PathList( "org", "apache", "commons", "collections", xs @ _* ) => MergeStrategy.last
  case PathList( "javax", "servlet", xs @ _* ) => MergeStrategy.last
  case PathList( "org", "apache", "hadoop", "yarn", xs @ _* ) => MergeStrategy.last
  case PathList( "org", "slf4j", xs @ _* ) => MergeStrategy.first
  case PathList( "META-INF", "maven", "com.fasterxml.jackson.core", xs @ _* ) => MergeStrategy.discard
  case PathList( "META-INF", "maven", "commons-logging", xs @ _* ) => MergeStrategy.discard
  case PathList( "META-INF", "maven", "org.apache.avro", xs @ _* ) => MergeStrategy.discard

  case x if Assembly.isConfigFile(x) => MergeStrategy.concat

  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) => MergeStrategy.rename

  case PathList("META-INF", xs @ _*) => {
    xs map {_.toLowerCase} match {
      case ("aop.xml" :: Nil) => MergeStrategy.first
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
  val targetBase = "/app"
  val artifactTargetPath = s"${targetBase}/${artifact.name}"
  val coreosPath = baseDirectory.value / ".." / "coreos"
  val entryScript = ( coreosPath ** "lineup.sh" ).get.headOption
  val aspectjArtifactName = ( coreosPath ** "aspectjweaver-*.jar" ).get.headOption
  val sigarBinary = ( coreosPath ** "libsigar-amd64-linux.so" ).get.headOption
  val mainclass = mainClass.in( Compile, run ).value.getOrElse( sys.error("Expected exactly one main class") )

  new Dockerfile {
//    from( "iron/java:1.8" )
    from( "java:8" )
    run( "apt-get", "update" )
    run( "apt-get", "-y", "install", "tmux" )
    copy( artifact, artifactTargetPath )

    val aspectAndSigar = for {
      aspectj <- aspectjArtifactName
      sigar <- sigarBinary
    } yield ( aspectj, sigar )

    (entryScript, aspectAndSigar) match {
      case ( Some(entry), Some( (aspectj, sigar) ) ) => {
        copy( entry, targetBase + "/" + entry.name )
        copy( aspectj, targetBase + "/" + aspectj.name )
        copy( sigar, targetBase + "/sigar-bin/" + sigar.name )

        entryPointShell(
          s"${targetBase}/${entry.name}",
          mainclass,
          "/etc/lineup:" + artifactTargetPath,
          "-Dconfig.resource=application-${LINEUP_ENV}.conf",
          s"-Djava.library.path=${targetBase}/sigar-bin/",
          s"-javaagent:${targetBase}/${aspectj.name}"
        )
//        entryPoint(
//          targetBase + "/" + entry.name,
//          "`-Dconfig.resource=application-$LINEUP_ENV.conf`",
//          s"-Djava.library.path=${targetBase}/sigar-bin/",
//          s"-javaagent:${targetBase}/${aspectj.name}",
//          mainclass
//        )
      }

//      case ( None, Some( (aspectj, sigar) ) ) => {
//        copy( aspectj, targetBase + "/" + aspectj.name )
//        copy( sigar, targetBase + "/sigar-bin/" + sigar.name )
//
//        entryPoint(
//          "java",
//          "-cp", "/etc/lineup:" + artifactTargetPath,
//          "-Dconfig.resource=application-devdocker.conf",
//          s"-Djava.library.path=${targetBase}/sigar-bin/",
//          s"-javaagent:${targetBase}/${aspectj.name}",
//          mainclass
//        )
//      }
//
//      case ( Some(entry), None ) => {
//        copy( entry, targetBase + "/" + entry.name )
//
//        entryPoint(
//          targetBase + "/" + entry.name,
//          "`-Dconfig.resource=application-$LINEUP_ENV.conf`",
//          mainclass
//        )
//      }
//
//      case ( None, None ) => {
//        entryPoint(
//          "java",
//          "-cp", "/etc/lineup:" + artifactTargetPath ,
//          mainclass
//        )
//      }
    }

    env( "LOG_HOME", "/var/log" )
//    env( "CONFIG_HOME", "/etc/lineup" )
    env( "LINEUP_ENV", "prod" )
    expose( 2004 )

    expose( 22 )
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
