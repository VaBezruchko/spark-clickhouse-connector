package org.apache.spark.sql

import java.sql.{ResultSet, ResultSetMetaData}

import io.clickhouse.spark.connector.TableScanner
import io.clickhouse.spark.sql.RowReaderFactory
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType


final class ClickhouseRow(values: Array[Any],
                          override val schema: StructType
                         )
  extends GenericRowWithSchema(values, schema) {
}


class ClickhouseRowFactory(metadata: StructType) extends RowReaderFactory {

  private def resultSetToObjectArray(rs: ResultSet): Array[Any] = {
    Array.tabulate[Any](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }

  def create(rs: ResultSet): ClickhouseRow = {
    new ClickhouseRow(resultSetToObjectArray(rs), metadata)
  }
}

object ClickhouseRowFactory {

  def apply(scanner: TableScanner): ClickhouseRowFactory =
    new ClickhouseRowFactory(
      JdbcUtils.getSchema(scanner.resultSet, JdbcDialects.get("jdbc:clickhouse")))
}
