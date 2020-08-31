# Benchmarks

Some benchmarks for the grafink job runs

### 1 Using SimilarityClassifier (unless specified otherwise)

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

### 4

Environment details

HBase
Similarity value Dataype Int
But we use DataFrame instead of Dataset[EdgeClass] (now onwards)
Spark cluster running over mesos

| # of executors | Executor memory | Executor cores | Driver memory |
|----------------|-----------------|----------------|---------------|
|50|2g|2|2g

Run details

JobId in history server: 594f9a37-08ab-4400-a8fa-e9990dc90d3b-0302

| # of vertices | # of edges | vertexLoader batchsize | edgeLoader batchsize |  Vertex load time  | Edge load time | Total Job time | # of tasks failed |
|---------------|------------|------------------------|----------------------|--------------------|----------------|----------------|-------------------|
|107619|128480018|100|500|10 s|47 min|56 min|0|

### 5 Using TwoModeClassifier and SameValueClassifier

Environment details

HBase
Spark cluster running over mesos
We now commit the transaction after adding every edge/vertex in JanusGraph

| # of executors | Executor memory | Executor cores | Driver memory |
|----------------|-----------------|----------------|---------------|
|50|2g|2|2g

Run details

JobId in history server: d0b4e985-6627-438f-960d-d44284c2b70f-0389

| # of vertices | # of edges |  Vertex load time  | Edge load time | Total Job time | # of tasks failed |
|---------------|------------|--------------------|----------------|----------------|-------------------|
|477314|981364|1 min|2.3 min|7.8 min|0|

### 6 Simulation of ingestion pipeline

Environment details

HBase
Spark cluster running over mesos
We continue to commit the transaction after adding every edge/vertex in JanusGraph

| # of executors | Executor memory | Executor cores | Driver memory |
|----------------|-----------------|----------------|---------------|
|50|2g|2|2g

We run a series of jobs, each ingesting vertices on top of existing data, and end up
ingesting 10098333 + 35 vertices in total
* Note that we have modified the benchmark version to run for longer duration of the data (normal limit is 7 days)

| Start Date | Job duration (days)  | # of vertices | # of edges |  Vertex load time  | Edge load time | Total Job time | # of tasks failed | Job Link |
|------------|----------------------|---------------|------------|--------------------|----------------|----------------|-------------------|----------|
| 2020-08-04 | 1 |82378| - |18 s|18s + 12 min|16 min|145|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0423/jobs/|
| 2020-07-21 | 11 - 1(day 28th missing) |746811| - |1.5 min|2.7 min + 38 min|49 min|3|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0422/jobs/|
| 2020-07-01 | 20 |1293556|-|2.7 min|2.9 min + 0 s|29 min (time spent in count)|5 + 32|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0421/jobs/|
| 2020-06-21 | 10 |594367|-|1.3 min|1.6 min + 18 min|26 min|0|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0418/jobs/|
| 2020-06-01 | 20 |737956|-|1.6 min|2.5 min + 26 min|35 min|0|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0417/jobs/|
| 2020-05-21 | 11 |534313|-|1.2 min|1.9 min + 14 min|22 min|0|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0416/jobs/|
| 2020-05-01 | 20 |741998|-|1.6 min|2.6 min + 18 min|27 min|0|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0413/jobs/|
| 2020-04-21 | 5 |218519|-|43 s|59 s + 4.8 min|9 min|0|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0412/jobs/|
| 2020-04-01 | 20 |526093|-|1.2 min|1.7 min + 6.7 min|0|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0406/jobs/|
| 2020-02-01 | 20 |180126|-|32 s|54 s + 3.5 min|0|https://wuip.lal.in2p3.fr:8443/history/d0b4e985-6627-438f-960d-d44284c2b70f-0401/jobs/|
