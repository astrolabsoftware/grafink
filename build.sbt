import BuildHelper._

name := "grafink"

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.astrolabsoftware"
ThisBuild / organizationName := "astrolabsoftware"

scalacOptions ++= Seq("-Ypartial-unification", "-deprecation")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

lazy val api =
  (project in file("api"))
    .settings(
      // Add support for scala version 2.11
      crossScalaVersions := Seq("2.11.11", (ThisBuild / scalaVersion).value),
      apiSettings
    ).dependsOn(common)

lazy val common =
  (project in file("common"))
    .settings(
      // Add support for scala version 2.11
      crossScalaVersions := Seq("2.11.11", (ThisBuild / scalaVersion).value),
      commonSettings
    )

lazy val core =
  (project in file("core"))
    .settings(
      // Add support for scala version 2.11
      crossScalaVersions := Seq("2.11.11", (ThisBuild / scalaVersion).value),
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
      buildInfoPackage := "com.astrolabsoftware.grafink",
      stdSettings
    ).dependsOn(common)

lazy val js =
  (project in file("js"))
    .settings(
      coverageEnabled := false,
      resolverSettings ++ scalaJSSettings
    ).enablePlugins(ScalaJSPlugin, ScalaJSBundlerPlugin)

lazy val root = (project in file(".")).settings(
  skip in publish := true
).aggregate(
  api,
  core,
  js
)

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

