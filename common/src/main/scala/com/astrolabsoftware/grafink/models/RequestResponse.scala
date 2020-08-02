package com.astrolabsoftware.grafink.models

case class InfoRequest(tableName: String)
case class InfoResponse(vertexLabels: List[String], error: Option[String] = None)
