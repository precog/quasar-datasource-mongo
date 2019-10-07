disablePlugins(TravisCiPlugin)

libraryDependencies += "org.kohsuke" % "github-api" % "1.95" exclude("org.jenkins-ci", "annotation-indexer")

// used to fetch dependencies to form a coursier cache,
// for packaging the datasource.
// should remain the newest coursier version.
libraryDependencies ++= Seq(
  "io.get-coursier" %% "coursier" % "1.0.3",
  "io.get-coursier" %% "coursier-cache" % "1.0.3",
  "io.circe" %% "circe-core" % "0.12.2"
)

scalacOptions --= Seq(
  "-Ywarn-unused:imports",
  "-Yinduction-heuristics",
  "-Ykind-polymorphism",
  "-Xstrict-patmat-analysis")

// sbt/sbt#2572
scalacOptions in (Compile, console) --= Seq(
  "-Yno-imports",
  "-Ywarn-unused:imports")
