package io.clickhouse.spark.sql

import java.sql.ResultSet

import org.apache.spark.sql.Row

trait RowReaderFactory {

  def create(rs: ResultSet): Row
}
