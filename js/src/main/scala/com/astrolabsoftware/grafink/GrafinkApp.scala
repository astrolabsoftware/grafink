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

  def addClickedMessage(): Unit = {
    appendPar(document.body, "You clicked the button!")
  }

  def setupUI(): Unit = {
    val button = document.createElement("button")
    button.textContent = "Click me!"
    button.addEventListener("click", { (e: dom.MouseEvent) =>
      addClickedMessage()
    })
    document.body.appendChild(button)

    appendPar(document.body, "Hello World")
  }

  def main(args: Array[String]): Unit = {
    document.addEventListener("DOMContentLoaded", { (e: dom.Event) =>
      setupUI()
    })
  }
}
