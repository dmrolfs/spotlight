//addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "latest.integration")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.1")

addSbtPlugin("com.tapad" % "sbt-docker-compose" % "1.0.18")

//addSbtPlugin( "com.typesafe.sbt" % "sbt-pgp" % "latest.integration" )

addSbtPlugin( "org.allenai.plugins" % "allenai-sbt-release" % "2014.11.06-0" )

addSbtPlugin( "org.scalariform" % "sbt-scalariform" % "1.6.0" )

addSbtPlugin( "io.spray" % "sbt-revolver" % "0.8.0" )

// Native packager, for deploys.
//addSbtPlugin( "com.typesafe.sbt" % "sbt-native-packager" % "latest.integration" )

// Check for updates.
//addSbtPlugin( "com.timushev.sbt" % "sbt-updates" % "latest.integration" )

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

//addSbtPlugin("org.scoverage" %% "sbt-coveralls" % "1.1.0")

addSbtPlugin( "com.eed3si9n" % "sbt-assembly" % "0.14.3" )

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.3.11")

addSbtPlugin("com.typesafe.sbt" % "sbt-proguard" % "0.2.2")

//addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "latest.integration")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.sksamuel.sbt-versions" % "sbt-versions" % "0.2.0")

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "0.14.0")

addSbtPlugin("org.oedura" % "scavro-plugin" % "1.0.2")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0-M12")
