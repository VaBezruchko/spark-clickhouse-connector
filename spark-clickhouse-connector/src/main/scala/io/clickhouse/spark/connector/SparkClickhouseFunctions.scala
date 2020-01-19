package io.clickhouse.spark.connector

import io.clickhouse.spark.connector.partitioner.SimpleClickhousePartitioner
import org.apache.spark.SparkContext

/** Provides Clickhouse-specific methods on org.apache.spark.SparkContext SparkContext */
class SparkClickhouseFunctions(@transient val sc: SparkContext) extends Serializable {

  def clickhouseTable(query: String, cluster: String)
                     (implicit connector: ClickhouseConnector = ClickhouseConnector(sc, Some(cluster)),
                         readConf: ConnectorConf = ConnectorConf.fromSparkConf(sc.getConf)
                         ) = new ClickhouseRDD(sc, connector, query, readConf,  SimpleClickhousePartitioner(connector))

  /**
    * Used for clickhouse installation without 'cluster' option e.g. single server installation.
    * It's assumed, that all hosts in datasource are single shard and contains the same data.
    */
  def clickhouseTableWithoutCluster(query: String)
                     (implicit connector: ClickhouseConnector = ClickhouseConnector(sc, None),
                      readConf: ConnectorConf = ConnectorConf.fromSparkConf(sc.getConf)
                     ) = new ClickhouseRDD(sc, connector, query, readConf,  SimpleClickhousePartitioner(connector))

}
