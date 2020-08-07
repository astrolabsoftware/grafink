import com.typesafe.sbt.packager.universal.ZipHelper
import sbt.Keys.name

name := "grafink-api"

enablePlugins(BuildInfoPlugin, JavaServerAppPackaging)

// Put conf files inside conf directory in the package
mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" * "*.conf").get.map { f =>
    f -> s"conf/${f.name}"
  }
}

executableScriptName := "grafinkapi"
// do not create bat script
makeBatScripts := Seq()

publishMavenStyle := true