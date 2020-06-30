# Benchmarks

Some benchmarks for the grafink job runs

### 1

Environment details

HBase
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
Spark cluster running over mesos

| # of executors | Executor memory | Executor cores | Driver memory |
|----------------|-----------------|----------------|---------------|
|50|3g|2|2g

Run details

| # of vertices | # of edges | vertexLoader batchsize | edgeLoader batchsize |  Vertex load time  | Edge load time | Total Job time |
|---------------|------------|------------------------|----------------------|--------------------|----------------|----------------|
|107619|128480018|100|100|18 s|55 min|1.1 h|
