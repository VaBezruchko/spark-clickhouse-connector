package io.clickhouse.spark.connector

import java.sql.{PreparedStatement, ResultSet}

import io.clickhouse.spark.connection.{ConnectionPooledDBUrl, JdbcConnection}
import org.apache.spark.TableIterator
import org.apache.spark.internal.Logging

class TableScanner(connectionPool: ConnectionPooledDBUrl,
                   connection: JdbcConnection,
                   statement: PreparedStatement) extends TableIterator[ResultSet] with Logging {

  private var _count = 0

  val resultSet: ResultSet = statement.executeQuery()

  /** Returns the number of successful invocations of `next` */
  def count: Int = _count

  override protected def getNext: ResultSet = {
    if (resultSet.next()) {
      _count += 1
      resultSet
    } else {
      finished = true
      null.asInstanceOf[ResultSet]
    }
  }

  def close(): Unit = {

    try {
      if (null != resultSet) {
        resultSet.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing resultset", e)
    }
    try {
      if (null != statement) {
        statement.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      if (null != connection) {
        connectionPool.releaseConnection(connection)
      }
      logDebug(s"release connection: ${connection.shard}")
    } catch {
      case e: Exception => logWarning(s"Exception releasing connection: ${connection.shard}", e)
    }
  }

}

