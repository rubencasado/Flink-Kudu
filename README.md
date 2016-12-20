# Flink-Kudu
This is a simple PoC for working with Kudu with Apache Flink

## Requirements

* Unix-like environment (we're using Linux)
* Apache Flink (version 1.1.3)
* Apache Kudu (version 1.1.0)
* Maven (version 3.3.9)
* Java (Version 8)

```
git clone https://github.com/rubencasado/Flink-Kudu.git
cd Flink-Kudu
```

# Development

We recommend IntelliJ IDEA for working with project. For open the project at IDEA, only must go to "*File / Open / Flink-Kudu*".

## Progress
### Completed
- [x] Github project's structure
- [x] Sink implementation for Batch Processing and its Job.
- [x] Sink implementation for Streaming Processing and its Job.

### Working
- [ ] Source implementation for Batch Processing and its Job.
- [ ] Unit Tests for main classes.
- [ ] Kafka integration

# Build Jobs

There are some avaliable Jobs for executing directly at IntelliJ or at Flink JobMaster through the generated JAR.
For generate the JAR and submit the Job to Flink cluster (local mode by the moment):

**1. Start Flink (Local Mode)**
```
<Flink-instalation-folder>/bin/start-local.sh
```
For example:
```
/opt/flink-1.1.3/bin/start-local.sh
```

**2. Build with Maven**
```
mvn clean install -DskipTests 
mvn package -DskipTests
```
After that, the generated JAR will be located at "*Flink-Kudu / target / flink-kudu-1.0-SNAPSHOT.jar*"

**3. Execute Job**

```
<Flink-instalation-folder>/bin/flink run -c <Job-package-path> target/flink-kudu-1.0-SNAPSHOT.jar param1 param2 ...
```
For example:
```
/opt/flink-1.1.3/bin/flink run -c es.accenture.flink.Job.JobBatchSink target/flink-kudu-1.0-SNAPSHOT.jar mytable CREATE
```
**List of available Jobs**
- [ ] JobSource (**es.accenture.flink.Job.JobSource**): it takes the data from a table of Kudu and prints it on the screen (*Batch Mode*).
- [x] JobBatchSink (**es.accenture.flink.Job.JobBatchSink**): it takes the data from a collection of elements and stores it in a table of Kudu (*Batch Mode*). Allows params an executing: *table_name* and *table_mode* (CREATE, APPEND or OVERRIDE).
- [x] JobStreamingSink (**es.accenture.flink.Job.JobStreamingSink**): it takes the data from a collection of elements and stores it in a table of Kudu (*Streaming Mode*). Allows params an executing: *table_name*
- [ ] JobBatchInputOutput (**es.accenture.flink.Job.JobBatchInputOutput**): it takes the data from a table of Kudu, applies a minimal transformation and stores it in a table of Kudu (*Batch Mode*).
- [ ] JobStreamingInputOutput (**es.accenture.flink.Job.JobStreamingInputOutput**): it takes the data from Kafka, applies a minimal transformation and stores it in a table of Kudu (*Streaming Mode*).
