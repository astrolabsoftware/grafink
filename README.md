# grafink
Grafink is a spark ETL job to load data into Janusgraph.

### Compiling from source

```
sbt compile
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
  - bin/start.sh           // The main executable script that user can invoke to start the job.

### Running Job

```
./bin/start.sh --config conf/application.conf
```
