package org.apache.spark.metrics.clickhouse

import java.sql.ResultSet

import io.clickhouse.spark.connector.ConnectorConf
import org.apache.spark.TaskContext
import org.apache.spark.executor.InputMetrics

/** A trait that provides a method to update read metrics which are collected for connector related tasks.
  * The appropriate instance is created by the companion object.
  *
  */
trait InputMetricsUpdater {
  /** Updates the metrics being collected for the connector after reading each single row. This method
    * is not thread-safe.
    *
    * @param row the row which has just been read
    */
  def updateMetrics(row: ResultSet): ResultSet = row

  def finish(): Long

  private[metrics] def updateTaskMetrics(count: Int, dataLength: Int): Unit = {}

}

trait JdbcRowMeter {
  def sizeOf(rs:ResultSet):Int
}

object InputMetricsUpdater {

  /** Creates the appropriate instance of `InputMetricsUpdater`.
    *
    * The created instance will be updating task metrics so
    * that Spark will report them in the UI. Remember that this is supported for Spark 1.2+.
    *
     */
  def apply(
    taskContext: TaskContext,
    conf: ConnectorConf
  ): InputMetricsUpdater = {

    val tm = taskContext.taskMetrics()
    val inputMetrics = tm.inputMetrics

    if (conf.clickhouseMetricsEnable) {

      val jdbcRowMeter: JdbcRowMeter =
        if (conf.сlickhouseDriver == ConnectorConf.DefaultClickhouseDriver) {
          //metrics supported for yandex driver only
          new ClickhouseYandexRowMeter
        } else {
          null
        }
      new ClickhouseInputMetricsUpdater(jdbcRowMeter, inputMetrics)
    }
    else {
      new StubInputMetricsUpdater
    }
  }

  trait Timer {
    private val startTime = System.nanoTime()

    def stopTimer(): Long = System.nanoTime() - startTime
  }

  private class ClickhouseInputMetricsUpdater(rowMeter: JdbcRowMeter, inputMetrics: InputMetrics)
    extends InputMetricsUpdater  with Timer {

    def getRowBinarySize(row: ResultSet):Int = {

      if (rowMeter != null) {
        rowMeter.sizeOf(row)
      }
      else {
        0
      }
    }

    override def updateMetrics(row: ResultSet): ResultSet = {
      val binarySize = getRowBinarySize(row)

      updateTaskMetrics(1, binarySize)

      row
    }

    def finish(): Long = stopTimer()

    override def updateTaskMetrics(count: Int, dataLength: Int): Unit = {
      inputMetrics.incBytesRead(dataLength)
      inputMetrics.incRecordsRead(count)
    }

  }

  /** The implementation of [[InputMetricsUpdater]] which does not update anything. */
  private class StubInputMetricsUpdater extends InputMetricsUpdater with Timer {
    def finish(): Long = stopTimer()
  }

}