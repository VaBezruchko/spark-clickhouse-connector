import Versions.JDK

import scala.util.Properties

object Versions {

  val scala211 = "2.11.7"
  val scala212 = "2.12.9"
  val Spark           = "2.4.0"
  val Slf4j           = "1.6.1"
  val SparkJetty      = "8.1.14.v20131031"
  val CodaHaleMetrics = "3.0.2"
  val commons_pool    =  "2.5.0"
  val clickhouse_jdbc = "0.1.50"
  val JDK             = "1.8"
  val joda_version    = "2.10.6"

  val status = (versionInReapply: String, binaryInReapply: String) =>
    println(s"""
               |  Scala: $versionInReapply
               |  Scala Binary: $binaryInReapply
               |  Java: target=$JDK user=${Properties.javaVersion}
        """.stripMargin)
}
