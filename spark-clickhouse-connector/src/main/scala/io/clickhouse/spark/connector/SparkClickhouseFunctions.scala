package io.clickhouse.spark.connector

import io.clickhouse.spark.connector.partitioner.SimpleClickhousePartitioner
import org.apache.spark.SparkContext
import org.joda.time.DateTime

/** Provides Clickhouse-specific methods on org.apache.spark.SparkContext SparkContext */
class SparkClickhouseFunctions(@transient val sc: SparkContext) extends Serializable {

  def clickhouseTable(query: String, cluster: String)
                     (implicit connector: ClickhouseConnector = ClickhouseConnector(sc, cluster),
                         readConf: ConnectorConf = ConnectorConf.fromSparkConf(sc.getConf)
                         ) = new ClickhouseRDD(sc, connector, query, readConf,  SimpleClickhousePartitioner(connector))

}
