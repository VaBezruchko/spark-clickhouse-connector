package io.clickhouse.spark.connector

import org.apache.spark.SparkConf

case class ConnectorConf (сlickhouseDriver:String = ConnectorConf.DefaultClickhouseDriver,
                          сlickhouseUrl:String = ConnectorConf.DefaultClickhouseUrl,
                          maxConnectionsPerExecutor:Int = ConnectorConf.DefaultMaxConnectionsPerExecutor,
                          clickhouseMetricsEnable:Boolean = ConnectorConf.DefaultClickhouseMetricsEnable,
                          сlickhouseSocketTimeoutMs:Int = ConnectorConf.DefaultClickhouseSocketTimeoutMs,
                          clickhouseAutoDiscoveryEnable:Boolean = ConnectorConf.DefaultClickhouseAutoDiscoveryEnable,
                          clickhousePortDefault:Int = ConnectorConf.DefaultClickhousePortDefault,
                          clickhouseUser:String = ConnectorConf.DefaultClickhouseUser,
                          clickhousePassword:String = ConnectorConf.DefaultClickhousePassword
                         )

object ConnectorConf {

  val ClickhouseDriverProperty = "spark.clickhouse.driver"
  val ClickhouseUrlProperty = "spark.clickhouse.url"
  val ClickhouseUserProperty = "spark.clickhouse.user"
  val ClickhousePasswordProperty = "spark.clickhouse.password"
  val ClickhouseAutoDiscoveryProperty = "spark.clickhouse.cluster.auto-discovery"
  val ClickhouseHttpPortDefaultProperty = "spark.clickhouse.cluster.port.default"//is used with auto-discovery options
  val ClickhouseSocketTimeoutProperty = "spark.clickhouse.socket.timeout.ms"
  val MaxConnectionsPerExecutorProperty ="spark.clickhouse.connection.per.executor.max"
  val ClickhouseMetricsEnableProperty = "spark.clickhouse.metrics.enable"

  val DefaultClickhouseDriver = "ru.yandex.clickhouse.ClickHouseDriver"
  val DefaultClickhouseUrl = "jdbc:clickhouse://127.0.0.1:8123"
  val DefaultMaxConnectionsPerExecutor:Int = 1
  val DefaultClickhouseSocketTimeoutMs:Int = 60000
  val DefaultClickhouseMetricsEnable:Boolean = false
  val DefaultClickhouseAutoDiscoveryEnable:Boolean = false
  val DefaultClickhousePortDefault:Int = 8123

  val DefaultClickhouseUser:String = null
  val DefaultClickhousePassword:String = null


  def fromSparkConf(conf: SparkConf): ConnectorConf = {


    ConnectorConf(
      сlickhouseDriver = conf.get(ClickhouseDriverProperty, DefaultClickhouseDriver),
      сlickhouseUrl = conf.get(ClickhouseUrlProperty, DefaultClickhouseUrl),
      maxConnectionsPerExecutor = conf.getInt(MaxConnectionsPerExecutorProperty, DefaultMaxConnectionsPerExecutor),
      clickhouseMetricsEnable = conf.getBoolean(ClickhouseMetricsEnableProperty, DefaultClickhouseMetricsEnable),
      сlickhouseSocketTimeoutMs = conf.getInt(ClickhouseSocketTimeoutProperty, DefaultClickhouseSocketTimeoutMs),
      clickhouseAutoDiscoveryEnable = conf.getBoolean(ClickhouseAutoDiscoveryProperty, DefaultClickhouseAutoDiscoveryEnable),
      clickhousePortDefault =  conf.getInt(ClickhouseHttpPortDefaultProperty, DefaultClickhousePortDefault),
      clickhouseUser =  conf.get(ClickhouseUserProperty, DefaultClickhouseUser),
      clickhousePassword =  conf.get(ClickhousePasswordProperty, DefaultClickhousePassword)
    )
  }

}

