package io.clickhouse.spark.connector.partitioner

object PartitionQuery {

  def queryForPartition(query: String, partition: ClickhousePartition):String ={

    partition.partitionSplit match {
      case Some(split: String) => s"$query and $split"
      case None => query
    }
  }
}
