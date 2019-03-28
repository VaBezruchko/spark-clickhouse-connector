package io.clickhouse.spark

import org.apache.spark.SparkContext

package object connector {
  implicit def toSparkClickhouseFunctions(sc: SparkContext): SparkClickhouseFunctions =
    new SparkClickhouseFunctions(sc)
}
