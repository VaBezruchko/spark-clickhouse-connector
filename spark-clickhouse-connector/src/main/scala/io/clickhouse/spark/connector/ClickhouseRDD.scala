package io.clickhouse.spark.connector

import io.clickhouse.spark.connector.partitioner._
import org.apache.spark.metrics.clickhouse.InputMetricsUpdater
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ClickhouseRowFactory, Row}
import org.apache.spark._
import org.joda.time.DateTime


/** RDD representing a Table Scan of a Clickhouse table.
 *
 * This class is the main entry point for analyzing data in Clickhouse database with Spark.
 * Obtain objects of this class by calling SparkClickhouseFunctions.clickhouseTable()
 *
 * Configuration properties should be passed in the [[org.apache.spark.SparkConf SparkConf]]
 * configuration of [[org.apache.spark.SparkContext SparkContext]].
 * `ClickhouseRDD` needs to open connection to Clickhouse, therefore it requires appropriate
 * connection property values to be present in [[org.apache.spark.SparkConf SparkConf]].
 * For the list of required and available properties, see [[ConnectorConf]].
 *
 * `ClickhouseRDD` divides the data set into smaller partitions, processed locally on every
 * cluster node. There are several partition strategy:
 * - DatedClickhousePartitioner that provides functionality for splitting shard into small partitions by date range.
 * Supported several range types e.g. Daily, Hourly.
 * - SimpleClickhousePartitioner that provides functionality for splitting RDD with partitions by shards.
 * - CustomClickhousePartitioner with custom split strategy for each shard.
 *
 * A `ClickhouseRDD` object gets serialized and sent to every Spark Executor, which then
 * calls the `compute` method to fetch the data on every node. The `getPreferredLocations`
 * method tells Spark the preferred nodes to fetch a partition from, so that the data for
 * the partition are at the same node the task was sent to. If Clickhouse nodes are collocated
 * with Spark nodes, the queries are always sent to the Clickhouse process running on the same
 * node as the Spark Executor process, hence data are not transferred between nodes.
 * If a Clickhouse node fails or gets overloaded during read, the queries are retried
 * to a different node.
 *
 */
class ClickhouseRDD
(
  @transient val sc: SparkContext,
  val connector: ClickhouseConnector,
  val query: String,
  val connectorConf: ConnectorConf,
  clickhousePartitioner: ClickhousePartitioner
) extends RDD[Row](sc, Seq.empty) with Logging {

  type Self = ClickhouseRDD

  //don't override partitioner
  //@transient override val partitioner = Some(clickhousePartitioner)

  /** Allows to copy this RDD with changing some of the properties */
  protected def copy(
                      query: String = query,
                      connectorConf: ConnectorConf = connectorConf,
                      connector: ClickhouseConnector = connector,
                      clickhousePartitioner: ClickhousePartitioner = clickhousePartitioner
                    ): Self = {

    new ClickhouseRDD(
      sc = sc,
      connector = connector,
      query = query,
      connectorConf = connectorConf,
      clickhousePartitioner = clickhousePartitioner
    )
  }

  def query(sql: String, cluster: String): Self = {
    copy(query = query)
  }

  /**
   * Partitioning strategy: which is used for split each shard with small parts by date range.
   *
   * @param startPeriod begin of date range
   * @param endPeriod   end of date range
   * @param rangeType   type of date range. e.g. Hour, Day.
   * @param pk          name of primary key
   */
  def withPeriod(startPeriod: DateTime, endPeriod: DateTime, rangeType: RangeType, pk: String = "dated"): Self = {
    copy(
      clickhousePartitioner = new DatedClickhousePartitioner(connector, (startPeriod, endPeriod), rangeType, pk)
    )
  }

  /**
   * Base partitioning strategy: single partition for each shard.
   */
  def withoutPartitioning(): Self = {
    copy(
      clickhousePartitioner = new SimpleClickhousePartitioner(connector)
    )
  }

  /**
   * User defined partitioning strategy
   *
   * @param customPartitions Sequence of partition for splitting each shard with small parts.
   */
  def withCustomPartitioning(customPartitions: Seq[String]): Self = {
    copy(
      clickhousePartitioner = new CustomClickhousePartitioner(connector, customPartitions)
    )
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    val partition = split.asInstanceOf[ClickhousePartition]
    logDebug(s" Start computation fot partition $partition")
    val metricsUpdater = InputMetricsUpdater(context, connectorConf)

    val scanner = connector.execute(partition, query)

    val scannerWithMetrics = scanner.map(metricsUpdater.updateMetrics)

    context.addTaskCompletionListener[Unit] { context =>
      val duration = metricsUpdater.finish() / 1000000000d
      logInfo(s"Complete computation for partition $partition in $duration%.3f s. Fetched ${scanner.count} rows")
      scanner.closeIfNeeded()
    }

    val rowFactory = ClickhouseRowFactory(scanner)

    scannerWithMetrics.map(rowFactory.create)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    logDebug(s"Get preferred locations for $split")
    val locations = split.asInstanceOf[ClickhousePartition].endpoints
      .flatMap(ev => NodeAddress.hostNames(ev)).toList

    logTrace(s" Locations:\n${locations.mkString("\n")}")
    locations
  }

  override protected def getPartitions: Array[Partition] = {

    val partitions = clickhousePartitioner.partitions

    logDebug(s"Created total ${partitions.length} partitions for datasource with $clickhousePartitioner.")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }
}
