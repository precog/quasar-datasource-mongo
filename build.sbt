import scala.collection.Seq

scalaVersion in ThisBuild := "2.12.10"

ThisBuild / githubRepository := "quasar-datasource-mongo"

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-datasource-mongo"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-datasource-mongo"),
  "scm:git@github.com:precog/quasar-datasource-mongo.git"))

ThisBuild / githubWorkflowBuildPreamble ++= Seq(
  WorkflowStep.Run(
    List(
      "chmod 0600 key_for_docker.pub",
      "docker-compose up -d"),
    name = Some("Start mongo instances and sshd server")),
    WorkflowStep.Tmate
  )

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

val mongoVersion = "2.7.0"
val shimsVersion = "2.0.0"
val slf4jVersion = "1.7.25"
val specsVersion = "4.7.1"
val jsrVersion = "3.0.2"
val jschVersion = "0.1.55"
val catsEffectVersion   = "2.1.0"
val iotaVersion = "0.3.10"
val drosteVersion = "0.8.0"
val catsMTLVersion = "0.7.0"

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
      "io.higherkindness"          %% "droste-core"                % drosteVersion,
      "io.frees"                   %% "iota-core"                  % iotaVersion,
      "org.typelevel"              %% "cats-effect"                % catsEffectVersion,
      "org.typelevel"              %% "cats-mtl-core"              % catsMTLVersion,
      "org.mongodb.scala"          %% "mongo-scala-driver"         % mongoVersion,
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
  .evictToLocal("QUASAR_PATH", "foundation", true)
  .enablePlugins(QuasarPlugin)
