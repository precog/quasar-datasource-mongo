import scala.collection.Seq

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-datasource-mongo"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-datasource-mongo"),
  "scm:git@github.com:slamdata/quasar-datasource-mongo.git"))

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

val quasarVersion = IO.read(file("./quasar-version")).trim
val qdataVersion = IO.read(file("./qdata-version")).trim
val mongoVersion = "2.5.0"
val catsEffectVersion = "1.0.0"
val fs2Version = "1.0.0"
val shimsVersion = "1.2.1"
val slf4jVersion = "1.7.25"
val specsVersion = "4.3.3"
val refinedVersion = "0.8.5"
val nettyVersion = "4.1.28.Final"

lazy val core = project
  .in(file("datasource"))
  .settings(addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.2.4"))
  .settings(parallelExecution in Test := false)
  .settings(
    name := "quasar-datasource-mongo",
    datasourceName := "mongo",
    datasourceQuasarVersion := quasarVersion,
    datasourceModuleFqcn := "quasar.physical.mongo.MongoDataSourceModule$",
    datasourceDependencies ++= Seq(
      "com.codecommit"             %% "shims"                      % shimsVersion,
      "eu.timepit"                 %% "refined-scalacheck"         % refinedVersion,
      "org.typelevel"              %% "cats-effect"                % catsEffectVersion,
      "org.mongodb.scala"          %% "mongo-scala-driver"         % mongoVersion,
      "com.slamdata"               %% "qdata-json"                 % qdataVersion,

      "io.netty"                   %  "netty-all"                  % nettyVersion,
      "com.slamdata"               %% "quasar-foundation-internal" % quasarVersion % Test classifier "tests",
      "org.mongodb.scala"          %% "mongo-scala-driver"         % mongoVersion % Test classifier "tests",
      "com.slamdata"               %% "qdata-json"                 % qdataVersion % Test,
      "org.slf4j"                  %  "slf4j-log4j12"              % slf4jVersion % Test,
      "org.specs2"                 %% "specs2-core"                % specsVersion % Test,
      "org.specs2"                 %% "specs2-scalaz"              % specsVersion % Test,
      "org.specs2"                 %% "specs2-scalacheck"          % specsVersion % Test
    ))
  .enablePlugins(AutomateHeaderPlugin, DatasourcePlugin)
