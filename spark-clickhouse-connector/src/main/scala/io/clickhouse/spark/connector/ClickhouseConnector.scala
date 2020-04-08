package io.clickhouse.spark.connector

import java.net.InetAddress
import java.util.ConcurrentModificationException

import io.clickhouse.spark.connection.{ClickHouseDataSource, ConnectionPooledDBUrl, JdbcConnection}
import io.clickhouse.spark.connector.ClickhouseConnector.getConnectionPool
import io.clickhouse.spark.connector.partitioner.{ClickhousePartition, PartitionQuery}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import scala.collection.concurrent.TrieMap

final case class ShardUnavailableException(private val message: String = "",
                                           private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

class ClickhouseConnector(conf: ConnectorConf,
                          initDataSource: ClickHouseDataSource,
                          cluster: Option[String]
                         )
  extends Serializable with Logging {

  val (
    dataSource: Map[Int, Seq[InetAddress]],
    theDataSource: ClickHouseDataSource
    ) = makeDataSource()

  def execute(partition: ClickhousePartition, query: String): TableScanner = {

    try {
      executeStatement(
        partition.endpoints.map(_.getHostAddress).iterator,
        PartitionQuery.queryForPartition(query, partition),
        getConnectionPool(conf, theDataSource)
      )
    }
    catch {
      case e: ShardUnavailableException =>
        throw ShardUnavailableException(
          s"all servers for shard (${partition.shardId}) are not accessible: (${partition.endpoints.map(_.getHostAddress).mkString(",")})", e)
    }
  }

  @scala.annotation.tailrec
  private def executeStatement(shardNodes: Iterator[String], query: String, cp: ConnectionPooledDBUrl): TableScanner = {

    if (!shardNodes.hasNext) throw ShardUnavailableException() //there are no replica left

    val replicaServer: String = shardNodes.next()
    var jdbc: JdbcConnection = null
    try {
      jdbc = cp.getConnection(replicaServer)

      val statement = jdbc.connection.prepareStatement(query)

      new TableScanner(cp, jdbc, statement)
    }
    catch {
      case e: Throwable =>
        // go to the next replica with warning message
        logWarning(s"Failed to execute query at $replicaServer", e)

        if (jdbc != null) {
          cp.releaseConnection(jdbc)
        }
        executeStatement(shardNodes, query, cp)
    }
  }

  private def getClusterMetadata = {
    val query =
      s"select shard_num, groupArray(host_name) as names, groupArray(host_address) as addresses from system.clusters " +
        s"where cluster = '${cluster.get}' group by shard_num"

    executeStatement(initDataSource.value.keys.iterator, query, getConnectionPool(conf, initDataSource))
      .map(rs => (rs.getInt("shard_num"),
        rs.getArray("names").getArray.asInstanceOf[Array[String]],
        rs.getArray("addresses").getArray.asInstanceOf[Array[String]]))
      .toList
  }

  /** find host in cluster metadata and detect shard
   * return shard_num */
  private def detectShard(clusterMetadata: List[(Int, Array[String], Array[String])], host: String): Option[Int] = {
    clusterMetadata.find(v => v._2.contains(host) || v._3.contains(host)).map(_._1)
  }

  private def makeDataSource(): (Map[Int, Seq[InetAddress]], ClickHouseDataSource) = {

    if (cluster.isDefined) {

      val clusterMeta = getClusterMetadata

      if (!conf.clickhouseAutoDiscoveryEnable) {

        //for each host in data_source detects shard_id, after that performed group by replicas.
        //Also performed filtering hosts which doesn't contained into cluster metadata.
        val ds =
          initDataSource.value.keys
            .map(v => (detectShard(clusterMeta, v), v)) //(shard_num, host)
            .filter(_._1.isDefined) //filter undefined hosts
            .map(v => (v._1.get, v._2)) //remove Option[]
            .groupBy(_._1) //group by shard_num
            .map(v => (v._1, v._2.map(m => InetAddress.getByName(m._2)).toList)) // (shard_num, List(InetAddress))
        (ds, initDataSource)
      }
      else {
        logDebug("cluster auto-discovery enabled")
        //cluster auto-discovery enabled, make new datasource from cluster metadata
        val newDataSource =
          ClickHouseDataSource(clusterMeta.flatMap(_._3), conf.clickhousePortDefault, initDataSource.database)

        val ds = clusterMeta
          .map(v => (v._1, v._3.map(m => InetAddress.getByName(m)).toList))
          .toMap

        (ds, newDataSource)
      }
    }
    else {
      //Used for clickhouse installation without 'cluster' option e.g. single server installation.
      //It's assumed, that all hosts in datasource are single shard and contains the same data.
      (Map(0 -> initDataSource.value.keys.map(InetAddress.getByName).toList), initDataSource)
    }
  }
}


object ClickhouseConnector {

  private val connectionPoolCache = new TrieMap[(ConnectorConf, ClickHouseDataSource), ConnectionPooledDBUrl]

  def apply(sc: SparkContext, cluster: Option[String]): ClickhouseConnector = {
    val conf: ConnectorConf = ConnectorConf.fromSparkConf(sc.getConf)

    val dataSource = ClickHouseDataSource(conf.сlickhouseUrl)

    new ClickhouseConnector(conf, dataSource, cluster)
  }

  def getConnectionPool(conf: ConnectorConf, ds: ClickHouseDataSource): ConnectionPooledDBUrl = synchronized {

    connectionPoolCache.get((conf, ds)) match {
      case Some(value) =>
        value
      case None =>
        val value = new ConnectionPooledDBUrl(ds.value, conf.сlickhouseDriver,
          conf.maxConnectionsPerExecutor, conf.сlickhouseSocketTimeoutMs,
          conf.clickhouseUser, conf.clickhousePassword)
        connectionPoolCache.putIfAbsent((conf, ds), value) match {
          case None =>
            value
          case Some(_) =>
            throw new ConcurrentModificationException("It shouldn't reach here as it is synchronized")
        }
    }
  }

}
