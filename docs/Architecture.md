# Architecture

This document describes the architecture for grafink

![Architecture](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/saucam/grafink/master/docs/architecture.puml)

We follow the [algorithm described here](LoadAlgorithm.md#option4)

The steps to load data are as follows:

1.  Load the schema (from config) by using embedded Janusgraph server on spark driver.
    This will load the schema data into configured Hbase server. This step is optional
    and should be made configurable to turn off or on.
2.  Query using ```IDManager``` (Get max id from ```IDManager``` data) to get the current offset for Ids.
3.  Check if intermediate data for the given ```startdate``` exists or not. If it does we jump to step 9. [TODO]
4.  If intermediate data does not exist we read parquet data for ```startdate```.
5.  Get count of alerts to load, which gives the total vertices to be added. [OPTIONAL]
6.  Based on current offset, generate new ids to be used for the data.
7.  Transform the alerts data into JanusGraph format with valid JanusGraph ids.
8.  Write this data to intermediate data directory.
9.  Load the vertices from executors using embedded Janusgraph instances into HBase.
10. Read entire vertices data from the intermediate data dump that we maintain.
11. Use configuration rules to add edges. Based on the data and the rules, we add edges from new alerts to
    all the existing vertices. This is done in a parallel way using spark.

The ```IDManager``` that uses HBase as the backend has a table with schema that is as follows:

- Key will be a combination of JanusGraph table name and input ```startdate```.
- There will be a column family with columns ```startOffset```, ```endOffset``` that stores id ranges being used to load data
  for that day into JanusGraph.

Note that we use ```IDManagerSparkService``` in the job currently which does not use HBase.