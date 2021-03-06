import sbt.Keys._
import sbt._
import sbtdocker.DockerPlugin
import BuildSettings._


object SpotlightBuild extends Build {

  lazy val root = {
    ( project in file(".") )
    .settings( defaultBuildSettings ++ doNotPublishSettings )
    .aggregate( core, /*publisher,*/ graphite, batch )
  }

  // lazy val publisher = {
  //   ( project in file("publisher") )
  //   .settings( defaultBuildSettings ++ publishSettings )
  // }

//  lazy val subscriber = {
//    ( project in file("subscriber") )
//    .settings( defaultBuildSettings ++ publishSettings )
//    // .enablePlugins( DockerPlugin )
//  }

  lazy val core = {
    ( project in file("core") )
    .settings( defaultBuildSettings ++ publishSettings )
  }

  lazy val graphite = {
    ( project in file("app-graphite") )
    .settings( defaultBuildSettings ++ publishSettings )
    .dependsOn( core )
    .enablePlugins( DockerPlugin )
  }

  lazy val batch = {
    ( project in file("app-batch") )
    .settings( defaultBuildSettings ++ doNotPublishSettings )
    .dependsOn( core )
  }

  //  lazy val root = Project(
//           id = "spotlight-root",
//           base = file( "." ),
//           settings = defaultBuildSettings ++ Seq(
//                                                   publish := { },
//                                                   publishTo := Some("bogus" at "http://nowhere.com"),
//                                                   publishLocal := { }
//                                                 )
//         ).aggregate( core )

//  lazy val core = Project(
//                           id = "core",
//                           base = file( "core" ),
//                           settings = defaultBuildSettings
//                         )

  // lazy val sandbox = Project(
  //   id = "sandbox",
  //   base = file( "sandbox" ),
  //   settings = defaultBuildSettings
  // ) dependsOn( core )


  // lazy val cli = Project(
  //   id = "cli",
  //   base = file( "cli" ),
  //   settings = defaultBuildSettings
  // ) dependsOn( model )

  // lazy val webapp = Project(
  //   id = "webapp",
  //   base = file( "webapp" ),
  //   settings = defaultBuildSettings
  // ) dependsOn( core )
}
