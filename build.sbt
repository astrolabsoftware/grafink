import BuildHelper._

name := "grafink"

lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.11"
lazy val supportedScalaVersions = List(scala212, scala211)

ThisBuild / scalaVersion     := scala212
ThisBuild / organization     := "com.astrolabsoftware"
ThisBuild / organizationName := "astrolabsoftware"

scalacOptions ++= Seq(
  "-Ypartial-unification",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-deprecation"
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

lazy val api =
  (project in file("api"))
    .settings(
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
      buildInfoPackage := "com.astrolabsoftware.grafink.api",
      apiSettings
    ).dependsOn(common)

lazy val common =
  (project in file("common"))
    .settings(
      // Add support for scala version 2.11
      crossScalaVersions := supportedScalaVersions,
      commonSettings
    )

lazy val core =
  (project in file("core"))
    .settings(
      // Add support for scala version 2.11
      crossScalaVersions := supportedScalaVersions,
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
      buildInfoPackage := "com.astrolabsoftware.grafink",
      stdSettings
    ).dependsOn(common)

lazy val root = (project in file(".")).settings(
  crossScalaVersions := Nil,
  skip in publish := true
).aggregate(
  api,
  core
)

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

