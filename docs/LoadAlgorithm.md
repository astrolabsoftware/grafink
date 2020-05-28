# Data Loading Approaches

This document describes the data load approach that grafink takes for bulk loading data into Janusgraph

All approaches will first tune JanusGraph and load the schema, and then proceed with data loading with
```storage.batch-loading``` enabled.

### 1. Tune JanusGraph for bulk loading

1. ```ids.block-size``` : This is the # of globally unique IDs to be reserved by each JanusGraph instance id block thread and it defaults to 10,000
   Setting this value too low will make transactions to block waiting for this reservation. Setting it too high could potentially cause unused IDs
   to be lost if the JanusGraph instance is shutdown (unused IDs are not returned to the pool upon restart)
   Recommendation is to set this value to # of vertices expected to be inserted per hour per JanusGraph instance.
2. ```storage.batch-loading```: This turns off internal constraint checkings in JanusGraph. To use this properly, we need to
   cleanup our data before pushing into JanusGraph.

   What all properties are disabled:
     - Disables automatic schema. To be doubly sure set ```schema.default``` to ```none```.
       This disables automatic type creation by JanusGraph, since concurrent type creation can lead to data integrity issues.
     - Disables transaction logging.
     - Disables transactions on storage  backend.
     - Bigger dirty transaction cache.
     - Disables external vertex existence checks.
     - Disables consistency checks (verify uniqueness, acquire locks etc)

3. ```cache.db-cache```: Better to turn this off, since cache is not distributed. Therefore each node will have a different view of the data.
4. ```threadPoolWorker```: The default value for this is 1. This is essentially a traffic cop to disseminate work. It should be set at most 2 * number of cores in the JanusGraph instance.
5. ```gremlinPool```: The number of traversals that can be run in parallel. This pool runs until query completes. 


### 2. Load schema
This will involve creating
  - Vertex Labels
    Each vertex will have one label which is a kind of its 'type'.
  - Edge Labels (Still to describe what will be edge label)
  - Property Keys
    - Vertex property ```Rowkey```
    - All [```link```](Schema-Model.md#link-properties) properties as vertex properties.

### 3. Read Parquet Data

- Spark job will read the partitioned data directory for the data to be ingested for the particular day.
- This means we should invoke the job with ```startDate``` param.
- We should also control overwrite capability in case this data is already ingested?

### 4. Load Vertices and Edges

Every alert (single row in hbase) will be modelled as a graph vertex.

<div id="option1" />
#### Option 1

This approach focusses on using bulk loading offered by JanusGraph to push the data.

- Read all the rows, group by the interesting columnnames (link properties).
- Vertices in each group become ```cliques``` of the graph, since each of them needs to be connected to all the others in the group.
- Load all these as vertices of the graph and add the label and properties.
- Then for each of these groups:
   - **Search for all vertices with the same link properties.**
   - For each vertex in the group, add edges to all the returned vertices in the above step.

##### Advantages

- Fully reliant on Janusgraph

##### Disadvantages

- A LOT of read queries required. This will definitely slow down the process.
- As data grows, time taken by the queries will increase, and so will the overall data loading time.

#### Option 2

This approach involves, directly generating HBase Files from spark job, in a format that
JanusGraph would write, if we used [Option1](#option1)

- There is already an [open ticket in JanusGraph](https://github.com/JanusGraph/janusgraph/issues/885)
- This uses the bulk loading mechanism in Hbase to achieve high write speeds
- Example of [bulk-loading hbase using spark](https://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/)
- **But this approach cannot be used because we want to add edges to existing vertices while writing data**
  This would involve updating existing data files, by reading, modifying and rewriting them back.


<div id="option3" />
#### Option 3

This takes a hybrid approach between the aforementioned 2 options.

- We store the vertex data and their properties along with their JanusGraphIds as intermediate data
- So essentially, this is another copy of JanusGraph's vertex data that we keep in hdfs, following the same partitioning scheme
  as the input parquet data.

  How do we get the id?
  When using addVertex API, it returns us ```Vertex``` object and we can call ```id()``` method to get the id.

  The steps now will be as follows:
- Read all the rows, group by the interesting columnnames (link properties).
- Vertices in each group become ```cliques``` of the graph, since each of them needs to be connected to all the others in the group.
- Load all these as vertices of the graph and add the label and properties.
- Then we check if intermediate data for this day exists or not. If not, create, cache and store intermediate data. Also mark
  the intermediate data for this day as available.
- We read the entire intermediate data into a Dataframe and cache it.
- Then for each of the groups:
  - We query the Dataframe for all vertices with same ```link``` properties and get their ids.
  - We also have the ids of all vertices in the group already.
  - Then for each pair of the ids, we query Janusgraph to get the vertices and add an edge between them.

##### Advantages

- Offloads JanusGraph from the significant read queries and moves them to spark.
- Intermediate data can be used as a snashot repository for JanusGraph if need be.
- Should be a LOT faster than [Option1](#option1)

##### Disadvantages

- Creates another, albeit partial copy of the data (since we do not need to store edges)

#### Option 4

This option builds on the [Option 3](#option3) by using custom ids for adding vertices.
This means that:

- We no longer need a JanusGraph server running. We can use embedded JanusGraph instances within the executors.
- To create id for the vertices:
 - We maintain our own pool of ids with the intermediate data, and read the start offset for the ids.
 - In the job, we assign ids while adding vertices by converting our own ids into valid JanusGraph Ids by using below functions

 ```
 public long toVertexId(long id)
 public long fromVertexId(long id)
 ```
 - The custom id supplied should be less than ```getVertexCountBound``` which is defined as

 ```java
 vertexCountBound = (1L << (TOTAL_BITS - partitionBits - USERVERTEX_PADDING_BITWIDTH));
 // here TOTAL_BITS = 64, partitionBits = 16, USERVERTEX_PADDING_BITWIDTH = 3 for Normal Vertex
 // So this evaluates to 17592186044416 ~ 17.5 trillion
 ```
 - Though the latest master of JanusGraph now has [support for custom ids when adding edges](https://github.com/JanusGraph/janusgraph/pull/2118), I think we would not need that feature,
   even if we use embedded JanusGraph instances. This is because, edge id is a combinations of
   ```
   long_encode(relationId) + long_encode(inV) + long_encode(relationtype.longId()) + long_encode(outV) 
   ```
   where ```relationId``` is the unique id from JanusGraph pool. So even when we are using embedded instances
   which might use the same ```relationId```, the edgeId would still be globally unique in the graph since they will use different in vertex id ```inV``` and out vertex id ```outV```
   Because at least one of ```inV``` or ```outV``` would be new every time we are running the job.

 - So for this approach to work, we just need to maintain the custom ids in the intermediate data and the last offset from where
   the next job will start assigning the ids.

##### Advantages

- Makes the spark job independent of JanusGraph cluster.
- Has all the advantages of Option3
- Should be faster than [Option3](#option3)

## Questions

1. What JanusGraph version we are using?

2. Are we using defaults, or have we changed any params for JanusGraph?

3. Is the parquet data to be loaded partitioned by day or any such other column?

Here are certain [technical limitations](https://docs.janusgraph.org/basics/technical-limitations/) in JanusGraph
