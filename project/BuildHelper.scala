/*
 * Copyright 2020 AstroLab Software
 * Author: Yash Datta
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import sbt._
import sbt.Keys._
import scoverage.ScoverageKeys._

object BuildHelper {

  val zioVersion = "1.0.0-RC21-2"
  val zioCatsVersion = "2.1.4.0-RC17"
  val pureConfigVersion = "0.12.3"
  val fastParseVersion = "2.1.0"
  val zioLoggingVersion = "0.3.0"
  val sparkVersion = "2.4.4"
  val sparkDariaVersion = "v0.35.0"
  val scoptVersion = "3.7.1"
  val hbaseVersion = "2.0.5"
  val janusGraphVersion = "0.5.1"
  val ammoniteVersion = "2.1.4"
  val asciiRenderVersion = "1.3.1"
  val scalaJSVersion = "1.0.0"
  val scalaJSReactVersion = "1.7.2"
  val scalaTagsVersion = "0.9.1"
  val http4sVersion = "1.0.0-M3"
  val circeVersion = "0.13.0"
  val kindProjectorPluginVersion = "0.11.0"

  val scalaTestVersion = "3.1.0"
  val uTestVersion = "0.7.4"
  val logbackVersion = "1.2.3"
  val scalaLoggingVersion  = "3.9.2"

  // Helper method to pattern match against the scala version and return the correct ammonite version
  def ammonite(scalaVersion: String): ModuleID =
    (scalaVersion match {
      case "2.11.11"   => "com.lihaoyi" % "ammonite" % "1.6.7-2-c28002d"
      case _ => "com.lihaoyi" % "ammonite" % ammoniteVersion
    }) cross CrossVersion.full

  lazy val testSettings = Seq(
    libraryDependencies ++= Seq(
      "org.scalatest"   %% "scalatest" % scalaTestVersion % Test,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
    // Do not execute test in parallel
    parallelExecution in Test := false,
    // Fail the test suite if statement coverage is < 70%
    coverageFailOnMinimum := true,
    // TODO: Increase this to 70+
    coverageMinimum := 60,
    // Put nice colors on the coverage report
    coverageHighlighting := true,
    // Do not publish artifact in test
    publishArtifact in Test := false
  )

  lazy val resolverSettings = Seq(
    resolvers ++= Seq(
      "central" at "https://repo1.maven.org/maven2/",
      // For spark daria
      "jitpack" at "https://jitpack.io"
    )
  )

  lazy val apiSettings = Seq(
    libraryDependencies ++=
      Seq(
        "org.http4s"               %% "http4s-blaze-server" % http4sVersion,
        "org.http4s"               %% "http4s-circe"        % http4sVersion,
        "org.http4s"               %% "http4s-dsl"          % http4sVersion,
        "io.circe"                 %% "circe-core"          % circeVersion,
        "io.circe"                 %% "circe-generic"       % circeVersion,
        "dev.zio"                  %% "zio-interop-cats"    % zioCatsVersion,
        compilerPlugin(
          ("org.typelevel" % "kind-projector" % kindProjectorPluginVersion).cross(CrossVersion.full)
        ),
        // Test
        "io.circe"                 %% "circe-literal"       % circeVersion % "test",
      )
  ) ++ commonSettings ++ hBaseSettings

  lazy val commonSettings = resolverSettings ++ testSettings ++ Seq(
    libraryDependencies ++=
      Seq(
        "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
        "com.github.scopt" %% "scopt" % scoptVersion,
        "dev.zio" %% "zio" % zioVersion,
        "dev.zio" %% "zio-logging-slf4j" % zioLoggingVersion,
        "org.janusgraph" % "janusgraph-core" % janusGraphVersion,
        "org.janusgraph" % "janusgraph-hbase" % janusGraphVersion,
        "org.janusgraph" % "janusgraph-inmemory" % janusGraphVersion,
        "org.janusgraph" % "janusgraph-es" % janusGraphVersion
      )
  )

  lazy val hBaseSettings = Seq(
    libraryDependencies ++=
      Seq(
        "org.apache.hbase" % "hbase-client" % hbaseVersion excludeAll(
          ExclusionRule(organization = "junit"),
          ExclusionRule(organization = "org.slf4j"),
          ExclusionRule(organization = "com.fasterxml.jackson.core")
        ),
        "org.apache.hbase" % "hbase-common" % hbaseVersion excludeAll(
          ExclusionRule(organization = "junit"),
          ExclusionRule(organization = "org.slf4j"),
          ExclusionRule(organization = "com.fasterxml.jackson.core")
        )
      )
  )

  val stdSettings = Seq(
    parallelExecution in Test := true,
    libraryDependencies ++=
      Seq(
        "com.lihaoyi" %% "fastparse" % fastParseVersion,
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-sql" % sparkVersion,
        "com.github.mrpowers" % "spark-daria" % sparkDariaVersion,
        "io.leego" % "banana" % asciiRenderVersion,
        ammonite(scalaVersion.value)
      )
  ) ++ commonSettings ++ hBaseSettings
}
