package io.clickhouse.spark.connector.partitioner

import io.clickhouse.spark.connector.ClickhouseConnector
import org.apache.spark.Partition
import org.joda.time.DateTime

trait ClickhousePartitioner extends Serializable {

  val partitions: Array[Partition]

  def numPartitions: Int = partitions.length
}

/**
 * Partitioner that provides functionality for splitting shard into small partitions by date range.
 * Supported several range types e.g. Daily, Hourly
 */
class DatedClickhousePartitioner(connector: ClickhouseConnector,
                                 dated: (DateTime, DateTime),
                                 rangeType: RangeType,
                                 primaryKeyName: String
                                ) extends SupportPartitionReplica with ClickhousePartitioner {


  override val partitions: Array[Partition] = {

    for (source <- connector.dataSource) yield { // (shard_num, List(InetAddress))

      var i = 0
      for (date <- DateRange.range(dated._1, dated._2, rangeType)) yield {

        val rotatedHosts = rotateRight(source._2, i)
        val shardId = source._1
        i += 1
        // partition index will be set later
        ClickhousePartition(0, shardId, rotatedHosts, Some(DateRange(date, rangeType, primaryKeyName).sql()))
      }
    }
  }.flatMap(_.seq)
    .zipWithIndex
    .map { case (p, index) => p.copy(index = index) }
    .toArray[Partition]

  override def toString: String = s"DatedPartitioner with period ${dated._1} - ${dated._2} by $rangeType "
}

/**
 * Partitioner that provides functionality for splitting RDD with partitions by shards
 */
class SimpleClickhousePartitioner(connector: ClickhouseConnector) extends ClickhousePartitioner {

  override val partitions: Array[Partition] = (for {
    source <- connector.dataSource
  } yield {

    val shardId = source._1
    val hosts = source._2
    // partition index will be set later
    ClickhousePartition(0, shardId, hosts, None)
  }).zipWithIndex
    .map { case (p, index) => p.copy(index = index) }
    .toArray[Partition]

  override def toString: String = s"SimplePartitioner"
}

/**
 * Partitioner with custom split strategy for each shard
 */
class CustomClickhousePartitioner(connector: ClickhouseConnector,
                                  partitionSeq: Seq[String]
                                 ) extends SupportPartitionReplica with ClickhousePartitioner {

  override val partitions: Array[Partition] = {

    for (source <- connector.dataSource) yield { // (shard_num, List(InetAddress))

      var i = 0
      for (part <- partitionSeq) yield {

        val rotatedHosts = rotateRight(source._2, i)
        val shardId = source._1
        i += 1
        // partition index will be set later
        ClickhousePartition(0, shardId, rotatedHosts, Some(part))
      }
    }
  }.flatMap(_.seq)
    .zipWithIndex
    .map { case (p, index) => p.copy(index = index) }
    .toArray[Partition]

  override def toString: String = s"CustomClickhousePartitioner"
}

object SimpleClickhousePartitioner {
  def apply(connector: ClickhouseConnector): SimpleClickhousePartitioner = new SimpleClickhousePartitioner(connector)
}

/** Support replica placement */
abstract class SupportPartitionReplica {

  /** circular shift of a Scala collection */
  def rotateRight[A](seq: Seq[A], i: Int): Seq[A] = {

    val size = seq.size
    if (i != 0 && size > 1) {
      seq.drop(size - (i % size)) ++ seq.take(size - (i % size))
    }
    else {
      seq.toList
    }
  }


}


