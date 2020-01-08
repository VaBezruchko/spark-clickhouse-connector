package io.clickhouse.spark.connector.partitioner

import java.net.InetAddress

import scala.collection.concurrent.TrieMap

object NodeAddress {

  private val addressCache = new TrieMap[InetAddress, Set[String]]

  /** Returns a list of IP-addresses and host names that identify a node.
    * Useful for giving Spark the list of preferred nodes for the Spark partition. */
  def hostNames(rpcAddress: InetAddress): Set[String] = {

    addressCache.get(rpcAddress) match {
      case Some(value) =>
        value
      case None =>
        val address = Set(
          rpcAddress.getHostAddress,
          rpcAddress.getHostName
        )
        addressCache.putIfAbsent(rpcAddress, address)
        address
    }
  }
}
