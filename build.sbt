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
    List("docker run -d -p 127.0.0.1:27018:27017 --name mongodb -e MONGODB_ROOT_PASSWORD=secret bitnami/mongodb:4.1.4"),
    name = Some("Start a plain mongo on port 27018")),

  WorkflowStep.Run(
    List(
      """docker create -p 127.0.0.1:27019:27017 --name mongodb-ssl -e MONGODB_EXTRA_FLAGS="--sslMode requireSSL --sslPEMKeyFile /cert.pem" bitnami/mongodb:4.1.4""",
      "docker cp ./certs/quasar-mongo-travis.pem mongodb-ssl:/cert.pem",
      "docker start mongodb-ssl"),
    name = Some("Start an SSL-enabled mongo on port 27019")),

  WorkflowStep.Run(
    List(
      """docker create -p 127.0.0.1:27020:27017 --name mongodb-ssl-client -e MONGODB_EXTRA_FLAGS="--sslMode requireSSL --sslPEMKeyFile /cert.pem --sslCAFile /ca.pem" bitnami/mongodb:4.1.4""",
      "docker cp ./certs/quasar-mongo-travis.pem mongodb-ssl-client:/cert.pem",
      "docker cp ./certs/client.crt mongodb-ssl-client:/ca.pem",
      "docker start mongodb-ssl-client"),
    name = Some("Start an SSL-enabled mongo with client-required key on port 27020")),

  WorkflowStep.Run(
    List("docker-compose up -d"),
    name = Some("Create an SSH tunnel to the mongo container")),

  WorkflowStep.Run(
    List(
      "sudo apt-get update",
      "sudo apt-get install sshpass",
      """ssh-keygen -t rsa -N "passphrase" -f key_for_docker""",
      "sshpass -p root ssh-copy-id -i key_for_docker -o StrictHostKeyChecking=no root@localhost -p 22222"),
    name = Some("Install sshpass")))

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
