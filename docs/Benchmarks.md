# Benchmarks

Some benchmarks for the grafink job runs

### 1

Environment details

HBase
Similarity value Datatype Long
Spark cluster running over mesos

| # of executors | Executor memory | Executor cores | Driver memory |
|----------------|-----------------|----------------|---------------|
|10|2g|3|2g

Run details

| # of vertices | # of edges | vertexLoader batchsize | edgeLoader batchsize |  Vertex load time  | Edge load time | Total Job time |
|---------------|------------|------------------------|----------------------|--------------------|----------------|----------------|
|3019|109518|100|100|5 s|6 s|47 s|

### 2

Environment details

HBase
Similarity value Dataype Long
Spark cluster running over mesos

| # of executors | Executor memory | Executor cores | Driver memory |
|----------------|-----------------|----------------|---------------|
|50|3g|2|2g

Run details

| # of vertices | # of edges | vertexLoader batchsize | edgeLoader batchsize |  Vertex load time  | Edge load time | Total Job time |
|---------------|------------|------------------------|----------------------|--------------------|----------------|----------------|
|107619|128480018|100|100|18 s|55 min|1.1 h|

### 3

Environment details

HBase
Similarity value Dataype Int
Spark cluster running over mesos

| # of executors | Executor memory | Executor cores | Driver memory |
|----------------|-----------------|----------------|---------------|
|50|2g|2|2g

Run details

JobId in history server: 594f9a37-08ab-4400-a8fa-e9990dc90d3b-0225

| # of vertices | # of edges | vertexLoader batchsize | edgeLoader batchsize |  Vertex load time  | Edge load time | Total Job time | # of tasks failed |
|---------------|------------|------------------------|----------------------|--------------------|----------------|----------------|-------------------|
|107619|128480018|100|500|12 s|47 min|55 min|1 failure in edgeLoader, 6 failures in counting edges|

