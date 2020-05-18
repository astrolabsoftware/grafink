import sbt._
import sbt.Keys._

object BuildHelper {

  val pureConfigVersion = "0.10.0"
  val scalaTestVersion = "3.1.0"
  val logbackVersion = "1.2.3"
  val scalaLoggingVersion  = "3.9.2"
  val mysqlVersion = "5.1.26"
  val scalikejdbcVersion = "3.4.1"

  private val stdOptions = Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked"
  )

  lazy val testSettings = Seq(
    libraryDependencies ++= Seq(
      "org.scalatest"   %% "scalatest" % scalaTestVersion % Test,
    )
  )

  lazy val basicSettings = Seq(
    resolvers ++= Seq(
      Resolver.sonatypeRepo("public")
    )
  ) ++ testSettings

  val stdSettings = Seq(
    parallelExecution in Test := true,
    libraryDependencies ++=
      Seq(
        "com.github.pureconfig" %% "pureconfig" % pureConfigVersion excludeAll (
          ExclusionRule(organization = "org.scala-lang")
        ),
        "ch.qos.logback" % "logback-classic" % logbackVersion,
        "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
      )
  ) ++ basicSettings
}
