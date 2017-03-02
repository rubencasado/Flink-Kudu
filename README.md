# Flink-Kudu
Java library to integrate Apache Kudu and Apache Flink. Main goal is to be able to read/write data from/to Kudu using the DataSet and DataStream Flink's APIs.

Data flows patterns:
* Batch
  * Kudu -> DataSet\<RowSerializable\> -> Kudu
  * Kudu -> DataSet\<RowSerializable\> -> other source
  * Other source -> DataSet\<RowSerializable\> -> other source
* Stream
  * Other source -> DataStream \<RowSerializable\> -> Kudu


```java

/* Batch mode - DataSet API -*/

DataSet<RowSerializable> input = KuduInputBuilder.build(TABLE_SOURCE, KUDU_MASTER)
               
// DataSet operations --> .map(), .filter(), reduce(), etc.
//result = input.map(...)

result.output(new KuduOutputFormat(KUDU_MASTER, TABLE_SINK, columnNames, KuduOutputFormat.CREATE));

KuduInputBuilder.env.execute();

```

```java

/* Streaming mode - DataSream API - */
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> stream = env.fromElements("data1 data2 data3");
DataStream<RowSerializable> stream2 = stream.map(new MapToRowSerializable());

stream2.addSink(new KuduSink(KUDU_MASTER, DEST_TABLE, columnNames));
env.execute();


```


## Requirements

* Flink and Kudu compatible OS
* Java (version 8)
* Scala (version 2.12.1)
* Apache Flink (version 1.1.3)
* Apache Kudu (version 1.2.0)
* Apache Kafka (version 0.10.1.1) (only for streaming example)
* Maven (version 3.3.9) (only for building)



## Build library

```shell
git clone https://github.com/rubencasado/Flink-Kudu.git
cd Flink-Kudu
mvn clean install -DskipTests 
mvn package -DskipTests
```
Generated JAR will be located at "*Flink-Kudu / target / flink-kudu-1.0-SNAPSHOT.jar*"

## Execution

Start Flink Job Manager
```
<Flink-installation-folder>/bin/start-local.sh
```
Submit the job
```
<Flink-instalation-folder>/bin/flink run -c <Job-package-path> target/flink-kudu-1.0-SNAPSHOT.jar param1 param2 ...
```
Example:
```shell
/opt/flink-1.1.3/bin/flink run -c es.accenture.flink.Job.JobBatchSink target/flink-kudu-1.0-SNAPSHOT.jar mytable CREATE localhost
```

**Included examples**

- [x] JobBatchSink (**es.accenture.flink.Job.JobBatchSink**): Saves data from a DataSet into a Kudu table(*DataSet API, Batch Mode*). Input parameters: *table_name*, *table_mode* (CREATE, APPEND or OVERRIDE) and *host*.

- [x] JobSource (**es.accenture.flink.Job.JobSource**): Reads data from a Kudu table and prints the information (*DataSet API, Batch Mode*).

- [x] JobBatchInputOutput (**es.accenture.flink.Job.JobBatchInputOutput**): Reads data from a Kudu table, executes some basic operations using DataSet API and save the result into a Kudu table(*DataSet API, Batch Mode*).

- [x] JobStreamingInputOutput (**es.accenture.flink.Job.JobStreamingInputOutput**): Reads data from Kafka, executes some basic operations using DataStream API and saves results into a Kudu table (*DataStream API, Streaming Mode*).

- [x] JobStreamingSink (**es.accenture.flink.Job.JobStreamingSink**): Saves data from a DataStream into a Kudu table (*DataStream API, Streaming Mode*). Input parameters: *table_name* and *host*.

