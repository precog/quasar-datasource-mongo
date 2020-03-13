import scala.collection.Seq

scalaVersion in ThisBuild := "2.12.10"

ThisBuild / githubRepository := "quasar-datasource-mongo"

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-datasource-mongo"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-datasource-mongo"),
  "scm:git@github.com:precog/quasar-datasource-mongo.git"))

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

val mongoVersion = "2.7.0"
val catsEffectVersion = "2.0.0"
val shimsVersion = "2.0.0"
val slf4jVersion = "1.7.25"
val specsVersion = "4.7.1"
val refinedVersion = "0.9.9"
val nettyVersion = "4.1.38.Final"
val jsrVersion = "3.0.2"
val jschVersion = "0.1.55"

lazy val core = project
  .in(file("datasource"))
  .settings(addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
  .settings(parallelExecution in Test := false)
  .settings(
    name := "quasar-datasource-mongo",
    quasarPluginName := "mongo",
    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),
    quasarPluginDatasourceFqcn := Some("quasar.physical.mongo.MongoDataSourceModule$"),
    quasarPluginDependencies ++= Seq(
      "com.codecommit"             %% "shims"                      % shimsVersion,
      "eu.timepit"                 %% "refined-scalacheck"         % refinedVersion,
      "org.typelevel"              %% "cats-effect"                % catsEffectVersion,
      "org.mongodb.scala"          %% "mongo-scala-driver"         % mongoVersion,
      "io.netty"                   %  "netty-all"                  % nettyVersion,
      "com.jcraft"                 % "jsch"                        % jschVersion,

      "com.precog"                 %% "quasar-foundation"          % managedVersions.value("precog-quasar") % Test classifier "tests",
      "com.precog"                 %% "quasar-frontend"            % managedVersions.value("precog-quasar") % Test classifier "tests",
      "org.slf4j"                  %  "slf4j-log4j12"              % slf4jVersion % Test,
      "org.specs2"                 %% "specs2-core"                % specsVersion % Test,
      "org.specs2"                 %% "specs2-scalaz"              % specsVersion % Test,
      "org.specs2"                 %% "specs2-scalacheck"          % specsVersion % Test,

      // mongo's `getDatabase` and `getCollection` have `Nullable` annotations and they raise
      // warnings w/o this dependency
      "com.google.code.findbugs"   %  "jsr305"                      % jsrVersion % Provided
    ))
  .enablePlugins(QuasarPlugin)
