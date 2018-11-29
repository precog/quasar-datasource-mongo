resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")
resolvers += Resolver.bintrayIvyRepo("djspiewak", "ivy")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M4")
addSbtPlugin("com.slamdata"    % "sbt-slamdata" % "1.4.0")
