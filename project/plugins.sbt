addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "latest.integration")

//addSbtPlugin( "com.typesafe.sbt" % "sbt-pgp" % "latest.integration" )

addSbtPlugin( "org.allenai.plugins" % "allenai-sbt-release" % "2014.11.06-0" )

addSbtPlugin( "com.typesafe.sbt" % "sbt-scalariform" % "1.3.0" )

addSbtPlugin( "io.spray" % "sbt-revolver" % "0.7.1" )

// Native packager, for deploys.
//addSbtPlugin( "com.typesafe.sbt" % "sbt-native-packager" % "latest.integration" )

// Check for updates.
//addSbtPlugin( "com.timushev.sbt" % "sbt-updates" % "latest.integration" )

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "latest.integration")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.3.11")

addSbtPlugin("com.typesafe.sbt" % "sbt-proguard" % "0.2.2")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "latest.integration")
