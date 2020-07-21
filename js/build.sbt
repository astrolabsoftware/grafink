name := "grafink-js"

npmDependencies in Compile ++=
  Seq(
    "react" -> "16.13.1",
    "react-dom" -> "16.13.1",
    "vis-network" -> "7.10.0"
  )