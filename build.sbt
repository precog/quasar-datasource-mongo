import scala.collection.Seq

scalaVersion in ThisBuild := "2.12.12"

ThisBuild / githubRepository := "quasar-datasource-mongo"

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-datasource-mongo"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-datasource-mongo"),
  "scm:git@github.com:precog/quasar-datasource-mongo.git"))

ThisBuild / githubWorkflowBuildPreamble ++= Seq(
  WorkflowStep.Run(
    List(
      """ssh-keygen -t rsa -N "passphrase" -f key_for_docker -m PEM""",
      "docker swarm init",
      "docker stack deploy -c docker-compose.yml teststack"),
    name = Some("Start mongo instances and sshd server")))

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

val shimsVersion = "2.2.0"
val mongoVersion = "4.0.5"
val slf4jVersion = "1.7.25"
val specsVersion = "4.10.5"
val jsrVersion = "3.0.2"
val jschVersion = "0.1.55"
val catsEffectVersion   = "2.2.0"
val iotaVersion = "0.3.10"
val drosteVersion = "0.8.0"
val catsMTLVersion = "0.7.1"
val reactiveVersion = "1.0.3"

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
      "com.codecommit"      %% "shims"              % shimsVersion,
      "io.higherkindness"   %% "droste-core"        % drosteVersion,
      "io.frees"            %% "iota-core"          % iotaVersion,
      "org.typelevel"       %% "cats-effect"        % catsEffectVersion,
      "org.typelevel"       %% "cats-mtl-core"      % catsMTLVersion,
      "org.mongodb.scala"   %% "mongo-scala-driver" % mongoVersion,
      "org.reactivestreams" %  "reactive-streams"   % reactiveVersion,
      "com.jcraft"          %  "jsch"               % jschVersion,
      "com.precog"          %% "quasar-foundation"  % managedVersions.value("precog-quasar") % Test classifier "tests",
      "com.precog"          %% "quasar-frontend"    % managedVersions.value("precog-quasar") % Test classifier "tests",
      "org.slf4j"           %  "slf4j-log4j12"      % slf4jVersion % Test,
      "org.specs2"          %% "specs2-core"        % specsVersion % Test,
      "org.specs2"          %% "specs2-scalaz"      % specsVersion % Test,
      "org.specs2"          %% "specs2-scalacheck"  % specsVersion % Test,

      // mongo's `getDatabase` and `getCollection` have `Nullable` annotations and they raise
      // warnings w/o this dependency
      "com.google.code.findbugs"   %  "jsr305"      % jsrVersion % Provided
    ))
  .evictToLocal("QUASAR_PATH", "foundation", true)
  .evictToLocal("QUASAR_PATH", "api", true)
  .evictToLocal("QUASAR_PATH", "connector", true)
  .evictToLocal("QUASAR_PATH", "frontend", true)
  .enablePlugins(QuasarPlugin)
