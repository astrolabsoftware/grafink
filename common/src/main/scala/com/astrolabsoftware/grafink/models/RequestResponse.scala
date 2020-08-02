package com.astrolabsoftware.grafink.models

case class InfoRequest(tableName: String)
case class InfoResponse(vertexLabels: List[String], edgeLabels: List[String], error: String = "")
