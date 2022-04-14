import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-destination-snowflake"

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-destination-snowflake"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-destination-snowflake"),
  "scm:git@github.com:precog/quasar-destination-snowflake.git"))

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

val DoobieVersion = "0.9.2"

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-snowflake")
  .settings(
    performMavenCentralSync := false,
    publishAsOSSProject := true,
    quasarPluginName := "snowflake",
    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),
    quasarPluginDestinationFqcn := Some("quasar.destination.snowflake.SnowflakeDestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % "1.7.25",
      "net.snowflake" % "snowflake-jdbc" % "3.13.17",
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "com.github.tototoshi" %% "scala-csv" % "1.3.6" % Test,
      "com.precog" %% "quasar-lib-jdbc" % managedVersions.value("precog-quasar-lib-jdbc"),
      "com.precog" %% "quasar-foundation" % quasarPluginQuasarVersion.value,
      "com.precog" %% "quasar-foundation" % quasarPluginQuasarVersion.value % "test->test" classifier "tests",
      "org.specs2" %% "specs2-core" % "4.9.4" % Test))
  .enablePlugins(QuasarPlugin)
  .evictToLocal("QUASAR_LIB_JDBC_PATH", "core", true)
