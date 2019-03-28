package io.clickhouse.spark.connection

import java.util.regex.Pattern

case class ClickHouseDataSource(value: Map[String, String], database: String)

object ClickHouseDataSource {
  private val JDBC_PREFIX = "jdbc:"
  private val JDBC_CLICKHOUSE_PREFIX = JDBC_PREFIX + "clickhouse:"
  private val URL_TEMPLATE = Pattern.compile(JDBC_CLICKHOUSE_PREFIX + "//([a-zA-Z0-9_:,.-]+)(/[a-zA-Z0-9_]+)?")

  def apply(url: String): ClickHouseDataSource = splitUrl(url)

  private def splitUrl(url: String):ClickHouseDataSource = {
    val m = URL_TEMPLATE.matcher(url)
    if (!m.matches) throw new IllegalArgumentException("Incorrect url")
    var database = m.group(2)
    if (database == null) database = ""
    val hosts = m.group(1).split(",")

    val value =
      hosts.map(hostWithPort =>
        (hostWithPort.split(":")(0), JDBC_CLICKHOUSE_PREFIX + "//" + hostWithPort + database)).toMap

    new ClickHouseDataSource(value, database)

  }

  def apply(hosts: Iterable[String], port: Int, database: String): ClickHouseDataSource = {

    val value = hosts.map(host => (host, JDBC_CLICKHOUSE_PREFIX + s"//$host:$port" +  database)).toMap

    new ClickHouseDataSource(value, database)
  }
}


