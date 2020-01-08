package io.clickhouse.spark.connector.partitioner

import java.net.InetAddress

import org.apache.spark.Partition
import org.joda.time.{DateTime, Days, Hours}

sealed trait RangeType {}
object RangeType {
  case object HOUR extends RangeType
  case object DAY extends RangeType
}

case class DateRange(dated: DateTime,
                     rType: RangeType,
                     pk: String // primary key name for partitioning
                    ) {

  def sql(): String = {
    if (rType == RangeType.HOUR)
      s"toStartOfHour($pk) = '${dated.hourOfDay.roundFloorCopy.toString("yyyy-MM-dd HH:mm:ss")}'"
    else
      s"toYYYYMMDD($pk) = '${dated.toString("yyyyMMdd")}'"
  }

  override def toString: String = {
    if (rType == RangeType.HOUR)
      s"DateRange(${dated.toString("yyyy-MM-dd HH")})"
    else
      s"DateRange(${dated.toString("yyyy-MM-dd")})"
  }
}

trait EndpointPartition extends Partition {
  def endpoints: Iterable[InetAddress]
}

case class ClickhousePartition(
                              index: Int,
                              shardId:Int,
                              endpoints: Iterable[InetAddress],
                              partitionSplit: Option[String] //addition primary key clause for spark partition splitting.
                              ) extends EndpointPartition {
  override def toString: String = super.toString
}


object DateRange {

  def range(startDate: DateTime, endDate: DateTime, rType: RangeType ): Seq[DateTime] = {

    if (rType == RangeType.DAY)
      rangeByDay(startDate, endDate)
    else
      rangeByHour(startDate, endDate)
  }

  def rangeByHour(startDate: DateTime, endDate: DateTime ): Seq[DateTime] = {

    val hours = Hours.hoursBetween(
      startDate.hourOfDay().roundFloorCopy(),
      endDate.hourOfDay().roundFloorCopy().plus(1)
    ).getHours
    (0 to hours).map(i => startDate.plusHours(i)).toList
  }
  def rangeByDay(startDate: DateTime, endDate: DateTime ): Seq[DateTime] = {

    val days = Days.daysBetween(
      startDate.withTimeAtStartOfDay(),
      endDate.withTimeAtStartOfDay().plus(1)
    ).getDays
    (0 to days).map(i => startDate.plusDays(i))
  }
}