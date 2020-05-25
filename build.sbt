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

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// removes all jar mappings in universal and appends the fat jar
mappings in Universal := {
  // universalMappings: Seq[(File,String)]
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in Compile).value

  val filtered = universalMappings filter {
    case (_, name) => !name.endsWith(".jar")
  }

  // add the fat jar to our sequence of things that we've filtered
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

// Put conf files inside conf directory in the package
mappings in Universal ++= {

  ((sourceDirectory in Compile).value / "resources" * "*.conf").get.map { f =>
    f -> s"conf/${f.name}"
  }
}

// the bash scripts classpath only needs the fat jar
scriptClasspath := Seq((assemblyJarName in assembly).value)
// name of start script
bashScriptConfigLocation := Some("")
executableScriptName := "start.sh"
// do not create bat script
makeBatScripts := Seq()

bashScriptExtraDefines += s"""JARNAME=${(assemblyJarName in assembly).value}"""