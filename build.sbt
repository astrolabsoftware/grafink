import BuildHelper._

name := "grafink"

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.astrolabsoftware"
ThisBuild / organizationName := "astrolabsoftware"

scalacOptions ++= Seq("-Ypartial-unification", "-deprecation", "-feature", "-Ywarn-unused:imports")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

enablePlugins(BuildInfoPlugin, JavaAppPackaging)

lazy val root = (project in file(".")).settings(stdSettings)

