package com.astrolabsoftware.grafink.models

case class InfoRequest(tableName: String)
case class InfoResponse(schema: SchemaInfo, error: String = "")

case class QueryRequest(tableName: String, query: String)
case class QueryResponse()

case class SchemaInfo(
    vertexLabels: List[VertexLabelInfo],
    edgeLabels: List[EdgeLabelInfo],
    propertyKeys: List[PropertyKeyInfo],
    vertexIndexes: List[VertexIndexInfo],
    edgeIndexes: List[EdgeIndexInfo],
    relationIndexes: List[RelationIndexInfo]
)
case class VertexLabelInfo(labelName: String, isPartitioned: Boolean, isStatic: Boolean)
case class EdgeLabelInfo(labelName: String, isDirected: Boolean, isUnidirected: Boolean, multiplicity: String)
case class PropertyKeyInfo(propertyKeyName: String, cardinality: String, dataType: String)

case class VertexIndexInfo(
    name: String,
    `type`: String,
    isUnique: Boolean,
    backingIndexName: String,
    keyStatus: List[String])
case class EdgeIndexInfo(
    name: String,
    `type`: String,
    isUnique: Boolean,
    backingIndexName: String,
    keyStatus: List[String])
case class RelationIndexInfo(
    name: String,
    `type`: String,
    direction: String,
    sortKey: String,
    sortOrder: String,
    status: String)
