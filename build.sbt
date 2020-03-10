import scala.collection.Seq

scalaVersion in ThisBuild := "2.12.10"

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-datasource-mongo"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-datasource-mongo"),
  "scm:git@github.com:slamdata/quasar-datasource-mongo.git"))

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
    datasourceName := "mongo",
    datasourceQuasarVersion := managedVersions.value("slamdata-quasar"),
    datasourceModuleFqcn := "quasar.physical.mongo.MongoDataSourceModule$",
    datasourceDependencies ++= Seq(
      "com.codecommit"             %% "shims"                      % shimsVersion,
      "eu.timepit"                 %% "refined-scalacheck"         % refinedVersion,
      "org.typelevel"              %% "cats-effect"                % catsEffectVersion,
      "org.mongodb.scala"          %% "mongo-scala-driver"         % mongoVersion,
      "io.netty"                   %  "netty-all"                  % nettyVersion,
      "com.jcraft"                 % "jsch"                        % jschVersion,

      "com.slamdata"               %% "quasar-foundation"          % managedVersions.value("slamdata-quasar") % Test classifier "tests",
      "com.slamdata"               %% "quasar-frontend"            % managedVersions.value("slamdata-quasar") % Test classifier "tests",
      "org.slf4j"                  %  "slf4j-log4j12"              % slf4jVersion % Test,
      "org.specs2"                 %% "specs2-core"                % specsVersion % Test,
      "org.specs2"                 %% "specs2-scalaz"              % specsVersion % Test,
      "org.specs2"                 %% "specs2-scalacheck"          % specsVersion % Test,

      // mongo's `getDatabase` and `getCollection` have `Nullable` annotations and they raise
      // warnings w/o this dependency
      "com.google.code.findbugs"   %  "jsr305"                      % jsrVersion % Provided
    ))
  .enablePlugins(AutomateHeaderPlugin, DatasourcePlugin)
