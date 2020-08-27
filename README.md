# Grafink
[![Build Status](https://travis-ci.org/astrolabsoftware/grafink.svg?branch=master)](https://travis-ci.org/astrolabsoftware/grafink)
[![codecov](https://codecov.io/gh/astrolabsoftware/grafink/branch/master/graph/badge.svg?style=platic)](https://codecov.io/gh/astrolabsoftware/grafink)

Grafink is a data loading and analysis tool for loading and analysing data into / from JanusGraph. It was created to load [Fink](https://fink-broker.org/) data into JanusGraph.

Grafink has 3 components in general

1. Api : This module exposes a REST interface on top of the loaded data in JanusGraph. The source is in ```api``` folder
2. Core: This is the Spark job that helps in loading data into Janusgraph. The source is in ```core``` folder.
The Architecture of the core job is described [here](docs/Architecture.md)
3. Shell: This provides a scala REPL for ad-hoc data analysis of the loaded data. This uses same configuration as the core module.
The source is in ```core/src/scala/com/astrolabsoftware/grafink/shell``` folder

## Grafink Core

Grafink Core is highly configurable. It has 5 major components:

1. SchemaLoader: Loads the graph schema to the configured storage backend.
2. IDManager: Maintains external copy of data with the custom ids generated for loading the vertices
3. VertexProcessor: Loads the vertices data into JanusGraph.
4. EdgeProcessor: Creates edges between the loaded vertices based on specified rules.
5. VertexClassifier: These are classes that describe an algorithm for connecting vertices, thereby
introducing a set of edges in the graph.

### Configuration

There are a number of configuration options using which we can customize grafink core job.
Here is a sample config file with example options and inline comments for explanation

```hocon
// Data Reader options
reader {
  // Specify the base path to data
  basePath = "/test/base/path"
  // Format of the data to read
  format = "parquet"
  // Columns to keep when reading the data, since we might not be interested in all the data
  keepCols = ["objectId", "schemavsn", "publisher", "fink_broker_version", "fink_science_version", "candidate", "cdsxmatch", "rfscore", "snn_snia_vs_nonia", "roid"]
  // Renames a few columns after reading, to simplify processing
  keepColsRenamed =
    [ { "f": "mulens.class_1", "t": "mulens_class_1" },
      { "f": "mulens.class_2", "t": "mulens_class_2" },
      { "f": "cutoutScience.stampData", "t": "cutoutScience" },
      { "f": "cutoutTemplate.stampData", "t": "cutoutTemplate" },
      { "f": "cutoutDifference.stampData", "t": "cutoutDifference" },
      { "f": "candidate.classtar", "t": "classtar" },
      { "f": "candidate.jd", "t": "jd" }
    ]
  // Adds new columns to the data by applying specified sql expression on existing columns
  // The columns being passed to the expression can be renamed columns as mentioned above as well
  // For eg: below adds a 'rowkey' column to the data by concating objectId and jd columns
  // where jd is a renamed column mentioned above
  newCols = [
    { "name": "rowkey", "expr": "objectId || '_' || jd as rowkey" }
  ]
}

// IDManager options
idManager {
  // This is used by IDManagerSparkService to store the data along with generated ids
  spark {
    // This introduces a reserved space for custom id generated for loading the data
    // This is reserved for adding some fixed vertices to the graph, different from the data being loaded,
    // for example for adding 'similarity' vertices for specific recipes to which the data (alert) vertices
    // can be connected via edges later.
    // So in this case, custom ids will be generated from 201 onwards instead of 1
    reservedIdSpace = 200
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

// Options specific to data loading job, vertex and edge loaders
job {
  // Specifies the schema of the vertices to be loaded
  schema {
    // These vertex labels will be created together with the specified properties
    vertexLabels = [
          {
            // Vertex label name
            name = "alert"
            // Here we can specify any property that we want to create, which is not present in the data
            properties = []
            // These columns from data will be converted to vertex properties in the graph
            propertiesFromData = [
              "rfscore",
              "snn_snia_vs_nonia",
              "mulens_class_1",
              "mulens_class_2",
              "cdsxmatch",
              "roid",
              "classtar",
              "objectId",
              "rowkey",
              "candid",
              "jd",
              "magpsf",
              "sigmapsf"
            ]
          },
          {
            name = "similarity"
            // So these 2 properties will be created for similarity vertex label
            properties = [
              {
                name = "recipe"
                typ = "string"
              },
              {
                name = "equals"
                typ = "string"
              }
            ]
            propertiesFromData = []
          }
        ]
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
    // Currently not being used, intended to batch the janusgraph vertex loading transactions
    batchSize = 100
    // The vertices created from the data will be labelled as this label
    label = "alert"
    // This config specifies path to a file that describes certain fixed vertices
    // that will be created before loading the data. There is a check in place to make sure these vertices
    // are not added again on every run.
    fixedVertices = "/fixedvertices.csv"
  }
  // EdgeLoader settings
  edgeLoader = {
    // Currently not being used, intended to batch the janusgraph edge loading transactions
    batchSize = 100
    // Default parallelism for loading edges when the total edges to be loaded per classifier is less than taskSize
    parallelism = 50
    taskSize = 25000
    // This config defines the rules to be applied for creating edges in the graph.
    // Note that each rule specified here will add a SET of edges to the graph, as described
    // by the algorithm of the rule (see VertexClassifiers)
    rulesToApply = ["twoModeClassifier", "sameValueClassifier"]
    // Configurations for each of the supported rules that can be applied to generate edges in the graph
    // See the section on VertexClassifer to read more about these
    rules {
      similarityClassifer {
        similarityExp = "(rfscore AND snnscore) OR mulens OR cdsxmatch OR objectId OR roid"
      }
      twoModeClassifier {
        recipes = ["supernova", "microlensing", "catalog", "asteroids"]
      }
      sameValueClassifier {
        colsToConnect = ["objectId"]
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

### SchemaLoader

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

### IDManager

Grafink generates custom ids to load vertices into JanusGraph. ```IDManager``` will generate
these custom ids and maintain the loaded data along with these ids.
When new data is loaded, it gets the max last id used and then adds the new ids starting from
the last max.

### VertexProcessor

Loads the vertices using custom ids into Janusgraph. Each alert data row is ingested as a vertex.
The alert data is processed as a dataframe, and then for each partition of the dataframe, an embedded
instance of janusgraph is created, and they are loaded parallely from spark executors.

### EdgeProcessor

The EdgeProcessor will load edges between the generated vertices as well as between generated and old vertices
in the graph.
EdgeProcessor can be supplied a list of rules. Each rule will generate a ```Dataset``` of ```MakeEdge```,
where each row represents an edge to be added.
Each sets of these edges are then converted into JanusGraph edges.

Like the VertexProcessor, EdgeProcessor will also load edge partitions in parallel.
The ```edgeLoader.parallelism``` controls the number of partitions being loaded in parallel,
in case the number of edges to load is less than ```edgeLoader.taskSize```. In case edges to load
are more than that, number of partitions are calculated as ```(number of edges to load / edgeLoader.taskSize) + 1```

### VertexClassifer

VertexClassifiers are rules, each of which creates a set of edges in the graph.
In grafink we can configure any number of such rules to add edges to the graph, when ingesting data.

Supported classifiers are described in detail in this document: [VertexClassifiers](docs/classifiers/VertexClassifiers.md)
Any of the supported classifiers can be configured as a rule to be applied to create the corresponding edges in the graph.

### Compiling from source

```
sbt compile
```

To compile against scala 2.11

```
sbt ++2.11.11 core/compile
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
sbt core/dist
```

The above creates a deployable zip file `grafink-<version>.zip`. The contents of the zip file are:

  - conf/application.conf  // Modify this config file according to the job requirements.
  - grafink assembly jar   // The main executable jar for running spark job.
  - bin/grafink            // The main executable script that user can invoke to start the job.

For compiling and packaging against scala 2.11:

```
sbt ++2.11.11 core/dist
```

### Running Job

Grafink command line can be passed the following parameters

| Parameter | Description | Mandatory | Default value if Not specified |
|-----------|-------------|-----------|--------------------------------|
|--config|Path to the configuration file|Yes|-|
|--startdate|Start date for which to run the job in <yyyy-MM-dd> format|No|Yesterday's Date|
|--duration|Number of days data for which the job will run, starting from startdate|No|1|
|--num-executors|Spark config passed along to spark submit|No|-|
|--driver-memory|Spark config passed along to spark submit|No|-|
|--executor-memory|Spark config passed along to spark submit|No|-|
|--executor-cores|Spark config passed along to spark submit|No|-|
|--total-executor-cores|Spark config passed along to spark submit|No|-|
|--total-executor-cores|Spark config passed along to spark submit|No|-|
|--conf|Spark config|No|-|

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

## Grafink Shell

Grafink supports querying the loaded data interactively via REPL shell. It is based off Ammonite REPL
and comes with all the goodness that Ammonite provides out of the box like multi-line editing, syntax
highlighting, pretty printing etc.
Note that Grafink disables autoloading and saving of scripts that is the default mode in Ammonite since
it is not multi-user friendly, hence all the shell storage is in-memory.
Grafink adds to Ammonite to provide a preconfigured connection to the desired JanusGraph storage backend
by simply passing in the same configuration file which was used to load the data into JanusGraph

To run the shell, simply:

```
./bin/grafink-shell --config conf/application.conf
```

Here is a snapshot of the welcome screen

```

  .oooooo.                        .o88o.  o8o              oooo        
 d8P'  `Y8b                       888 `"  `"'              `888        
888           oooo d8b  .oooo.   o888oo  oooo  ooo. .oo.    888  oooo  
888           `888""8P `P  )88b   888    `888  `888P"Y88b   888 .8P'   
888     ooooo  888      .oP"888   888     888   888   888   888888.    
`88.    .88'   888     d8(  888   888     888   888   888   888 `88b.  
 `Y8bood8P'   d888b    `Y888""8o o888o   o888o o888o o888o o888o o888o 
                                                                       
                                                                       
                                                                       
Welcome to Grafink Shell 0.1.0-SNAPSHOT
JanusGraphConfig available as janusConfig
JanusGraph available as graph, traversal as g
grafink>

```

Some sample command executions

```
grafink>val mgmt = graph.openManagement
mgmt: org.janusgraph.core.schema.JanusGraphManagement = org.janusgraph.graphdb.database.management.ManagementSystem@1c815814

grafink>mgmt.getGraphIndexes(classOf[Vertex]).asScala.toList
res3: List[org.janusgraph.core.schema.JanusGraphIndex] = List(objectIdIndex, rfScoreAndcdsx)

grafink>g.V().has("objectId", "ZTF19acmcetc").next()
res4: Vertex = v[256]

grafink>g.V().count().next()
632899 [main] WARN  org.janusgraph.graphdb.transaction.StandardJanusGraphTx  - Query requires iterating over all vertices [()]. For better performance, use indexes
res0: java.lang.Long = 1046L

grafink>g.V().outE("similarity").has("value", 2).asScala.toList
219832 [main] WARN  org.janusgraph.graphdb.transaction.StandardJanusGraphTx  - Query requires iterating over all vertices [()]. For better performance, use indexes
res11: List[Edge] = List(
  e[6rzbi8-mbk-6c5-1fk0][28928-similarity->66816],
  e[7ghekg-10cg-6c5-2pz4][47104-similarity->126976],
  e[5fbzls-1edc-6c5-1rsw][65280-similarity->82688],
  e[6rzb40-1fk0-6c5-mbk][66816-similarity->28928],
  e[5fbz7k-1rsw-6c5-1edc][82688-similarity->65280],
  e[21kfeo-296o-6c5-42kg][105216-similarity->189952],
  e[7ghe68-2pz4-6c5-10cg][126976-similarity->47104],
  e[21kf0g-42kg-6c5-296o][189952-similarity->105216]
)

grafink>g.V("28928").outE("similarity").has("value", 2).asScala.toList
res12: List[Edge] = List(e[6rzbi8-mbk-6c5-1fk0][28928-similarity->66816])

grafink>val mgmt = graph.openManagement
grafink>show(mgmt.printSchema)
"""------------------------------------------------------------------------------------------------
Vertex Label Name              | Partitioned | Static                                             |
---------------------------------------------------------------------------------------------------
alert                          | false       | false                                              |
---------------------------------------------------------------------------------------------------
Edge Label Name                | Directed    | Unidirected | Multiplicity                         |
---------------------------------------------------------------------------------------------------
similarity                     | true        | false       | MULTI                                |
---------------------------------------------------------------------------------------------------
Property Key Name              | Cardinality | Data Type                                          |
---------------------------------------------------------------------------------------------------
rfscore                        | SINGLE      | class java.lang.Double                             |
snnscore                       | SINGLE      | class java.lang.Double                             |
mulens_class_1                 | SINGLE      | class java.lang.String                             |
mulens_class_2                 | SINGLE      | class java.lang.String                             |
cdsxmatch                      | SINGLE      | class java.lang.String                             |
roid                           | SINGLE      | class java.lang.Integer                            |
classtar                       | SINGLE      | class java.lang.Float                              |
objectId                       | SINGLE      | class java.lang.String                             |
rowkey                         | SINGLE      | class java.lang.String                             |
candid                         | SINGLE      | class java.lang.Long                               |
jd                             | SINGLE      | class java.lang.Double                             |
magpsf                         | SINGLE      | class java.lang.Float                              |
sigmapsf                       | SINGLE      | class java.lang.Float                              |
value                          | SINGLE      | class java.lang.Integer                            |
---------------------------------------------------------------------------------------------------
Vertex Index Name              | Type        | Unique    | Backing        | Key:           Status |
---------------------------------------------------------------------------------------------------
objectIdIndex                  | Composite   | false     | internalindex  | objectId:     ENABLED |
rowkeyIndex                    | Composite   | false     | internalindex  | rowkey:       ENABLED |
---------------------------------------------------------------------------------------------------
Edge Index (VCI) Name          | Type        | Unique    | Backing        | Key:           Status |
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
Relation Index                 | Type        | Direction | Sort Key       | Order    |     Status |
---------------------------------------------------------------------------------------------------
similarityIndex                | similarity  | BOTH      | value          | asc      |    ENABLED |
---------------------------------------------------------------------------------------------------
"""

```

## Grafink API
Find detailed information about the API module and supported APIs [here](docs/API.md)

## Benchmarks

Some benchmarks are specified [here](docs/Benchmarks.md)
