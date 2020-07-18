# Grafink
[![Build Status](https://travis-ci.org/astrolabsoftware/grafink.svg?branch=master)](https://travis-ci.org/astrolabsoftware/grafink)
[![codecov](https://codecov.io/gh/astrolabsoftware/grafink/branch/master/graph/badge.svg?style=platic)](https://codecov.io/gh/astrolabsoftware/grafink)

Grafink is a spark ETL job to bulk load data into JanusGraph. It was created to load [Fink](https://fink-broker.org/) data into JanusGraph.

The Architecture is described [here](docs/Architecture.md)

Grafink is highly configurable. It has 4 major components:

1. SchemaLoader: Loads the graph schema to the configured storage backend.
2. IDManager: Maintains external copy of data with the custom ids generated for loading the vertices
3. VertexProcessor: Loads the vertices data into JanusGraph.
4. EdgeProcessor: Creates edges between the loaded vertices based on specified rules.


## Configuration

There are a number of configuration options using which we can customize grafink.
Here is a sample config file with example options and inline comments for explanation

```hocon
// Data Reader options
reader {
  // Specify the base path to data
  basePath = "/test/base/path"
  // Format of the data to read
  format = "parquet"
  // Columns to keep when reading the data, since we migt not be interested in all the data
  keepCols = ["objectId", "schemavsn", "publisher", "fink_broker_version", "fink_science_version", "candidate", "cdsxmatch", "rfscore", "snnscore", "roid"]
  // Renames a few columns after reading, to simplify processing
  keepColsRenamed =
    [ { "f": "mulens.class_1", "t": "mulens_class_1" },
      { "f": "mulens.class_2", "t": "mulens_class_2" },
      { "f": "cutoutScience.stampData", "t": "cutoutScience" },
      { "f": "cutoutTemplate.stampData", "t": "cutoutTemplate" },
      { "f": "cutoutDifference.stampData", "t": "cutoutDifference" },
      { "f": "candidate.classtar", "t": "classtar" }
    ]
}

// IDManager options
idManager {
  // This is used by IDManagerSparkService to store the data along with generated ids
  spark {
    // The base path where data will be generated. Note that partitioning of the original data is maintained
    dataPath = "/test/intermediate/base/path"
    // Whether to clear IDManager data when running grafink in delete mode to delete data from JanusGraph
    clearOnDelete = false
  }
  // This configuration is used by IDManager backed by HBase
  hbase {
    // The table name that stores current id offset
    tableName = "IDManagement"
    // Column family
    cf = "cf1"
    // Qualifier / column name
    qualifier = "id"
  }
}

// Options specific to JanusGraph, vertex and edge loaders
janusgraph {
  // Specifies the schema of the vertices to be loaded
  schema {
    // These columns from data will be converted to vertex properties in the graph
    vertexPropertyCols = ["rfscore", "snnscore", "mulens_class_1", "mulens_class_2",
      "cdsxmatch", "roid", "objectId"]
    // Currently only one vertex label with label `alert` will be created
    vertexLabel = "alert"
    // List of edge labels and their properties to be created
    edgeLabels = [
      {
        name = "similarity"
        properties = {
          key = "value"
          typ = "int"
        }
      }
    ]
    // List of indices to add while loading the schema
    index {
      // Adds composite indices, handled by JanusGraph storage backend
      composite = [
        {
          // Name of the index, should be unique
          name = "objectIdIndex"
          // Vertex property keys to index
          properties = ["objectId"]
        }
      ]
      // Adds mixed indices, handled by JanusGraph indexing backend
      mixed = [
        {
          // Name of the index, should be unique
          name = "rfScoreAndcdsx"
          // Vertex property keys to index
          properties = ["rfscore", "cdsxmatch"]
        }
      ]
      // Adds vertex centric indices, handled by JanusGraph storage backend 
      edge = [
        {
          // Name of the index, should be unique
          name = "similarityIndex"
          // Edge property keys to index, handled as 'RelationType' 
          properties = ["value"]
          // Edge Label for which to create the index
          label = "similarity"
        }
      ]
    }
  }
  // VertexLoader batch settings
  vertexLoader {
    batchSize = 100
  }
  // EdgeLoader settings
  edgeLoader = {
    batchSize = 100
    parallelism = 10
    taskSize = 25000
    rules {
      similarityClassifer {
        similarityExp = "(rfscore AND snnscore) OR mulens OR cdsxmatch OR objectId OR roid"
      }
    }
  }
  // JanusGraph storage settings, currently using hbase as storage backend
  storage {
    host: "127.0.0.1"
    port: 8182
    tableName = "TestJanusGraph"
    // Additional configurations to be passed to hbase when opening connection to it
    extraConf = ["zookeeper.recovery.retry=3", "hbase.client.retries.number=0"]
  }
  // JanusGraph Indexing Backend settings
  indexBackend {
    // We use ElasticSearch here, but other backends like Solr can also be used
    name = "elastic"
    // Name of the Elasticsearch index to create for mixed indices
    indexName = "elastictest"
    // Host Port for Elasticsearch
    host: "127.0.0.1:9200"
  }
}

// HBase client settings, in case we use HBase backed IDManager, not being used currently
hbase {
  zookeeper {
    quoram = "hbase-1.lal.in2p3.fr"
  }
}
```

## SchemaLoader

The schema model is described [here](docs/Schema-Model.md)
Grafink tries to take advantage of bulk-loading feature in Janusgraph and disables
the schema checks in place while loading vertices and edges.
Hence it pre-creates the required graph schema.
The supported Graph Elements while creating the schema include
- Vertex Labels and Properties
- Edge Label and Properties
- Composite Index
- Mixed Index
- Vertex-Centric (Edge) Index

There is a mechanism to check if the schema in the target storage table already exists
and then load the schema only if needed.

## IDManager

Grafink generates custom ids to load vertices into JanusGraph. ```IDManager``` will generate
these custom ids and maintain the loaded data along with these ids.
When new data is loaded, it gets the max last id used and then adds the new ids starting from
the last max.

## VertexProcessor

Loads the vertices using custom ids into Janusgraph. Each alert data row is ingested as a vertex.
The alert data is processed as a dataframe, and then for each partition of the dataframe, an embedded
instance of janusgraph is created, and they are loaded parallely from spark executors.
Each partition while loading the vertices will add ```vertexLoader.batchSize``` vertices
to the transaction before committing and the loading continues in this way.

## EdgeProcessor

The EdgeProcessor will load edges between the generated vertices as well as between generated and old vertices
in the graph.
EdgeProcessor can be supplied a list of rules. Each rule will generate a ```Dataset``` of ```MakeEdge```,
where each row represents an edge to be added.
Each sets of these edges are then converted into JanusGraph edges.

Like the VertexProcessor, EdgeProcessor will also load edge partitions in parallel.
Each partition, while loading the edges will add ```edgeLoader.batchSize``` edges to
the transaction before committing and the loading continues in this way.
The ```edgeLoader.parallelism``` controls the number of partitions being loaded in parallel,
in case the number of edges to load is less than ```edgeLoader.taskSize```. In case edges to load
are more than that, number of partitions are calculated as ```(number of edges to load / edgeLoader.taskSize) + 1```

Currently supported rules to generate edges are described below.

### SimilarityClassifer

This rule generates an edge between 2 alerts if one or more of the columns match,
as specified by a ```similarity``` expression.
The ```similarity``` expression is described in terms of column names, AND and OR operators.
For eg: given an expression ```(rfscore AND snnscore) OR objectId```, the rule will create an edge if
the ```rfscore``` and ```snnscore``` match or ```objectId``` match.

The definition for a 'match' varies per column:

| Column Name | Match Condition |
|-------------|-----------------|
|rfscore|<code>rfscore1 > 0.9 && rfscore2 > 0.9</code>|
|snnscore|<code>snnscore1 > 0.9 && snnscore2 > 0.9</code>|
|cdsxmatch|<code>(cdsxmatch1 =!= "Unknown") && (cdsxmatch1 === cdsxmatch2)</code>|
|roid|<code>(roid1, roid2) => (roid1 > 1) && (roid2 > 1)</code>|
|mulens|<code>(mulens1_class_1 === "ML" && mulens1_class_2 === "ML") && (mulens2_class_1 === "ML" && mulens2_class_2 === "ML")</code>|

Here the column name followed by 1 or 2 specifies the column for the first or the second row being matched.

### Compiling from source

```
sbt compile
```

To compile against scala 2.11

```
sbt ++2.11.11 compile
```

### Code format

This project uses scalafmt to format code. For formatting code:

```
sbt scalafmt        // format sources
sbt test:scalafmt   // format test sources
sbt sbt:scalafmt    // format.sbt source
```

### Running unit tests

```
sbt test
```

### Creating Assembly jar

```
sbt assembly
```

### Creating Distribution

```
sbt dist
```

The above creates a deployable zip file `grafink-<version>.zip`. The contents of the zip file are:

  - conf/application.conf  // Modify this config file according to the job requirements.
  - grafink assembly jar   // The main executable jar for running spark job.
  - bin/grafink            // The main executable script that user can invoke to start the job.

For compiling and packaging against scala 2.11:

```
sbt ++2.11.11 dist
```

### Running Job

To run locally

```
./bin/grafink --config conf/application.conf --startdate <yyyy-MM-dd> --duration 1 --num-executors 2 --driver-memory 2g --executor-memory 2g
```

To run over Mesos cluster

```
export SPARK_MASTER="mesos://<host>:<port>"
```

Then run

```
./bin/grafink --config conf/application.conf --startdate <YYYY-mm-dd> --duration <# of days> --driver-memory 2g --executor-memory 3g --conf spark.mesos.principal=<principal> --conf spark.mesos.secret=<secret> --conf spark.mesos.role=<role> --conf spark.cores.max=100 --conf spark.executor.cores=2
```

for eg:

```
./bin/grafink --config conf/application.conf --startdate 2019-11-01 --duration 1 --driver-memory 2g --executor-memory 3g --conf spark.mesos.principal=lsst --conf spark.mesos.secret=secret --conf spark.mesos.role=lsst --conf spark.cores.max=100 --conf spark.executor.cores=2
```

Note that by default grafink runs in ```client``` mode, but this is easily modifiable.

## Benchmarks

Some benchmarks are specified [here](docs/Benchmarks.md)
