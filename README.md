# spark-clickhouse-connector
Spark Clickhouse Connector

## Description

*Package for integration between Yandex Clickhouse and Apache Spark.*

This assembly provides functionality to represent a Clickhouse table as ClickhouseRdd.

 - allows to execute SQL queries
 - allows to filter rows on the server side
 - allows to manage spark partition granularity
 - provides failover by Clickhouse replica
 - provides data locality if Clickhouse nodes are collocated with Spark nodes
 - provides load-balancing by Clickhouse replica
 - provides Clickhouse cluster auto-discovery
 - can be used with both drivers: ru.yandex.clickhouse.clickhouse-jdbc or com.github.housepower.clickhouse-native-jdbc
 - allows to throttle consuming database resources

ClickhouseRDD is the main entry point for analyzing data in Clickhouse database with Spark. You can obtain object of this class by calling SparkClickhouseFunctions.clickhouseTable()

Configuration properties should be passed in the org.apache.spark.SparkConf SparkConf configuration of org.apache.spark.SparkContext SparkContext. ClickhouseRDD needs to open connection to Clickhouse, therefore it requires appropriate connection property values to be present in org.apache.spark.SparkConf SparkConf. For the list of required and available properties, see ConnectorConf.

A 'ClickhouseRDD' object gets serialized and sent to every Spark Executor, which then calls the 'compute' method to fetch the data on every node. The 'getPreferredLocations' method tells Spark the preferred nodes to fetch a partition from, so that the data for the partition are at the same node the task was sent to. If Clickhouse nodes are collocated  with Spark nodes, the queries are always sent to the Clickhouse process running on the same node as the Spark Executor process, hence data are not transferred between nodes. If a Clickhouse node fails or gets overloaded during read, the queries are retried to a different node.

## Build

In the root directory run

scala 2.11:

    sbt '++2.11.7 assembly'
    
or scala 2.12:

    sbt '++2.12.9 assembly'

or two versions of scala

    sbt '+ assembly'

A jar with shaded dependencies will be generated to directory spark-clickhouse-connector/target/scala-2.11 e.g. spark-clickhouse-connector_2.11-2.4.0_0.23.jar

To publish to local maven nexus

    publish

Or to publish to local repository

	publishM2

You need to provide in home directory two files: 
 - Credentials(Path.userHome / ".sbt" / "credentials") 
 - Path.userHome.absolutePath + "/.sbt/nexus_url"

## Usage

### Prerequisites 

* Copy spark-clickhouse-connector_2.11-2.4.0_0.23.jar to spark lib directory
* Copy ru.yandex.clickhouse.clickhouse-jdbc to spark lib directory
* Add dependency to your spark project
```
        <dependency>
            <groupId>io.clickhouse</groupId>
            <artifactId>spark-clickhouse-connector_2.11</artifactId>
            <version>0.23</version>
        </dependency>
 ```
### Set parameters

```scala
val sparkConf = new SparkConf()
 .set("spark.clickhouse.driver","ru.yandex.clickhouse.ClickHouseDriver")
 .set("spark.clickhouse.url", "jdbc:clickhouse://192.168.1.1:8123,192.168.1.2:8123")
 .set("spark.clickhouse.user", null)
 .set("spark.clickhouse.password", null)
 .set("spark.clickhouse.connection.per.executor.max", "5")
 .set("spark.clickhouse.metrics.enable", "false")
 .set("spark.clickhouse.socket.timeout.ms", "10000")
 .set("spark.clickhouse.cluster.auto-discovery", "false")

val ss = SparkSession.builder()
 .master("spark_master_url")
 .appName("test_app")
 .config(sparkConf)
 .getOrCreate()

```

### Add functions on the `SparkContext` and `RDD`:

```scala
import io.clickhouse.spark.connector._
```
### Loading from Clickhouse

Sample table with index by date

```sql
CREATE TABLE IF NOT EXISTS data.some_table_local on cluster some_cluster \
( \
 dated Date DEFAULT toDate(started), \
 started DateTime, \
 counter_id UInt32, \
 col1 String, \
 col2 String, \
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/some_table_local', '{replica}') \
 PARTITION BY (toYYYYMM(dated)) \
 ORDER BY (dated, counter_id) \
 
```

Loading data to ClickhouseRDD with spark partitions by `Shard` by `DAY`. For example you have cluster with 2 shards and you need to analyze 30 days period. In this case connector creates 60 spark-partitions.

```scala

val sc = ss.sparkContext
val query = s"select started, counter_id, col1, col2 from data.some_table_local " +
	s" where started >='${startDate.toString("yyyy-MM-dd HH:mm:ss")}' and started <= '${endDate.toString("yyyy-MM-dd HH:mm:ss")}'"

sc.clickhouseTable(query, "some_cluster")
 .withPeriod(startDate, endDate, partitioner.RangeType.DAY, "dated")
 .map(row => {
 		val counterId = row.getAs[Long]("counter_id")
 		val started = new DateTime(row.getAs[Timestamp]("started"))
 		val col1 = row.getAs[String]("col1")
 		val col2 = row.getAs[String]("col2")

 		(started, counterId, col1, col2)
 	})
 .filter ()
 .groupBy()
 .<...>

```

Loading data to ClickhouseRDD with spark partitions by `Shard` without date range. For example you have cluster with 2 shards and you need to analyze 30 days period. In this case connector creates only 2 spark-partitions.


```scala

val sc = ss.sparkContext
val query = s"select started, counter_id, col1, col2 from data.some_table_local " + 
	s" where dated >='${startDate.toString("yyyy-MM-dd")}' and dated <= '${endDate.toString("yyyy-MM-dd")} " + 
	s" and counter_id in (1,2,3)'"

sc.clickhouseTable(query, "some_cluster")
 .map(row => {
 		val counterId = row.getAs[Long]("counter_id")
 		val started = new DateTime(row.getAs[Timestamp]("started"))
 		val col1 = row.getAs[String]("col1")
 		val col2 = row.getAs[String]("col2")

 		(started, counterId, col1, col2)
 	})
 .filter ()
 .groupBy()
 .<...>

```

Loading data to ClickhouseRDD with spark partitions by `Shard` with custom date range. 
For example you have cluster with 2 shards and you need to analyze 30 days period. 
In this case connector creates only 4 spark-partitions.


```scala

val sc = ss.sparkContext
val query = s"select started, counter_id, col1, col2 from data.some_table_local " + 
	s" where counter_id in (1,2,3)'"

sc.clickhouseTable(query, "some_cluster")
 .withCustomPartitioning(Seq("dated >= '2019-01-01' and dated < '2019-01-16'",
                             "dated >= '2019-01-16' and dated < '2019-02-01'"))
 .map(row => {
 		val counterId = row.getAs[Long]("counter_id")
 		val started = new DateTime(row.getAs[Timestamp]("started"))
 		val col1 = row.getAs[String]("col1")
 		val col2 = row.getAs[String]("col2")

 		(started, counterId, col1, col2)
 	})
 .filter ()
 .groupBy()
 .<...>

```
