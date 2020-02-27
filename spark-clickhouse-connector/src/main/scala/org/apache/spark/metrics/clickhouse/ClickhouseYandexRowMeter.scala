package org.apache.spark.metrics.clickhouse

import java.sql.ResultSet

import ru.yandex.clickhouse.response.ClickHouseResultSet

/** Class that provide a method to calculate row_size from the yandex driver result_set  */
class ClickhouseYandexRowMeter extends JdbcRowMeter {

  def sizeOf(resultSet: ResultSet): Int = {
    resultSet.asInstanceOf[ClickHouseResultSet].getValues.map(_.getLen).sum
  }
}
