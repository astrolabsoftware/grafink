package com.astrolabsoftware.grafink

import scala.scalajs.js.annotation.JSExportTopLevel

import org.scalajs.dom
import org.scalajs.dom.document

// import org.scalajs.dom.html
// import scalatags.JsDom.all._

object GrafinkApp {

  def appendPar(targetNode: dom.Node, text: String): Unit = {
    val parNode = document.createElement("p")
    parNode.textContent = text
    targetNode.appendChild(parNode)
  }

  def setupUI(): Unit =
    appendPar(document.body, "Hello Grafink")

  def main(args: Array[String]): Unit =
    document.addEventListener("DOMContentLoaded", (e: dom.Event) => setupUI())
}
