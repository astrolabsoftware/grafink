# Grafink API
This document describes the currently supported APIs in grafink api module.

### Info API

The info API returns detailed schema information about the graph backed by target HBase tablename.

Sample Request

```POST host:9073/info```
```json
{
"tableName": "TestJanusGraph"
}
```

Sample Response
```json
{
  "schema": {
    "vertexLabels": [
      {
        "labelName": "alert",
        "isPartitioned": false,
        "isStatic": false
      }
    ],
    "edgeLabels": [
      {
        "labelName": "similarity",
        "isDirected": true,
        "isUnidirected": false,
        "multiplicity": "MULTI"
      }
    ],
    "propertyKeys": [
      {
        "propertyKeyName": "value",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Integer"
      },
      {
        "propertyKeyName": "rfscore",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Double"
      },
      {
        "propertyKeyName": "snnscore",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Double"
      },
      {
        "propertyKeyName": "mulens_class_1",
        "cardinality": "SINGLE",
        "dataType": "java.lang.String"
      },
      {
        "propertyKeyName": "mulens_class_2",
        "cardinality": "SINGLE",
        "dataType": "java.lang.String"
      },
      {
        "propertyKeyName": "cdsxmatch",
        "cardinality": "SINGLE",
        "dataType": "java.lang.String"
      },
      {
        "propertyKeyName": "roid",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Integer"
      },
      {
        "propertyKeyName": "classtar",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Float"
      },
      {
        "propertyKeyName": "objectId",
        "cardinality": "SINGLE",
        "dataType": "java.lang.String"
      },
      {
        "propertyKeyName": "rowkey",
        "cardinality": "SINGLE",
        "dataType": "java.lang.String"
      },
      {
        "propertyKeyName": "candid",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Long"
      },
      {
        "propertyKeyName": "jd",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Double"
      },
      {
        "propertyKeyName": "magpsf",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Float"
      },
      {
        "propertyKeyName": "sigmapsf",
        "cardinality": "SINGLE",
        "dataType": "java.lang.Float"
      }
    ],
    "vertexIndexes": [
      {
        "name": "objectIdIndex",
        "type": "Composite",
        "isUnique": false,
        "backingIndexName": "internalindex",
        "keyStatus": [
          "objectId:ENABLED"
        ]
      },
      {
        "name": "rowkeyIndex",
        "type": "Composite",
        "isUnique": false,
        "backingIndexName": "internalindex",
        "keyStatus": [
          "rowkey:ENABLED"
        ]
      }
    ],
    "edgeIndexes": [],
    "relationIndexes": [
      {
        "name": "similarityIndex",
        "type": "similarity",
        "direction": "BOTH",
        "sortKey": "value",
        "sortOrder": "asc",
        "status": "ENABLED"
      }
    ]
  },
  "error": ""
}
```