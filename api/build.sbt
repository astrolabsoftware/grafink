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

packageBin in Universal := {
  val originalFileName = (packageBin in Universal).value
  val (base, ext) = originalFileName.baseAndExt
  val newFileName = file(originalFileName.getParent) / (base + "_dist." + ext)
  val extractedFiles = IO.unzip(originalFileName, file(originalFileName.getParent))
  val mappings: Set[(File, String)] = extractedFiles.map(f => (f, f.getAbsolutePath.substring(originalFileName.getParent.size + base.size + 2)))
  val binFiles = mappings.filter { case (file, path) => path.startsWith("bin/") }
  for (f <- binFiles) f._1.setExecutable(true)
  ZipHelper.zip(mappings, newFileName)
  IO.move(newFileName, originalFileName)
  IO.delete(file(originalFileName.getParent + "/" + originalFileName.base))
  originalFileName
}

executableScriptName := "grafinkapi"
// do not create bat script
makeBatScripts := Seq()

publishMavenStyle := true