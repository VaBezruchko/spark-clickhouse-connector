package io.clickhouse.spark.connection

import java.io.Serializable
import java.sql.{Connection, Driver, SQLException, Statement}
import java.util.{NoSuchElementException, Properties}

import org.apache.commons.pool2.{KeyedPooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericKeyedObjectPool, GenericKeyedObjectPoolConfig}
import org.slf4j.LoggerFactory

case class JdbcConnection(shard: String, connection: Connection)


class ConnectionPooledDBUrl(val dataSource: Map[String, String],
                            val driverName: String,
                            val poolSizePetShard: Int,
                            val socketTimeoutMs: Int,
                            val user: String,
                            val password: String) extends Serializable {

  private val LOG = LoggerFactory.getLogger(classOf[ConnectionPooledDBUrl])

  private val driver = Class.forName(driverName).newInstance.asInstanceOf[Driver]

  private val connectionProperties = {
    val prop = new Properties
    prop.put("socket_timeout", socketTimeoutMs.toString)

    if (user != null) {
      prop.put("user", user)
    }
    if (password != null) {
      prop.put("password", password)
    }
    prop
  }

  private val pool = {

    val config = new GenericKeyedObjectPoolConfig

    config.setMaxTotalPerKey(poolSizePetShard)
    config.setTestOnBorrow(true)
    config.setTestOnReturn(false)

    new GenericKeyedObjectPool[String, JdbcConnection](new PoolableFactory, config)
  }

  def getConnection(shard: String): JdbcConnection = try
    this.pool.borrowObject(shard)
  catch {
    case ex: NoSuchElementException =>
      throw ex
    case ex: Exception =>
      throw new RuntimeException(String.format("Exception while getting DB connection:  Cannot connect to database server: %s, url: %s", shard, dataSource.get(shard)), ex)
  }

  def releaseConnection(con: JdbcConnection): Unit = {
    try
      this.pool.returnObject(con.shard, con)
    catch {
      case ex: Exception =>
        LOG.warn("Can not close connection.", ex)
    }
  }

  implicit def funcToRunnable(func: () => Unit): Runnable = () => func()

  def close(): Unit = {
    new Thread(() => {

      try {
        val p = this.pool
        LOG.debug(">>>> Clearing pool, active: {}, idle: {}", p.getNumActive, p.getNumIdle)
        p.clear()
        while ( {
          p.getNumActive > 0
        }) {
          p.setMaxTotal(p.getNumActive)
          try
            Thread.sleep(p.getMaxWaitMillis)
          catch {
            case _: InterruptedException =>
            //do noting
          }
        }
        LOG.debug(">>>> Closing pool, active: {}, idle: {}", p.getNumActive, p.getNumIdle)
        p.close()
      } catch {
        case ex: Exception =>
          LOG.warn(">>>> Exception closing pool", ex)
      }

    }).start()
  }


  private class PoolableFactory extends KeyedPooledObjectFactory[String, JdbcConnection] {
    @throws[SQLException]
    override def makeObject(shard: String): PooledObject[JdbcConnection] = {
      val dbURL = dataSource(shard)
      val connection = driver.connect(dbURL, connectionProperties)
      new DefaultPooledObject[JdbcConnection](JdbcConnection(shard, connection))
    }

    @throws[SQLException]
    override def destroyObject(key: String, obj: PooledObject[JdbcConnection]): Unit = {
      val dbURL = dataSource.get(key)
      LOG.debug("---- Closing connection in pool {}", dbURL)
      obj.getObject.connection.close()
    }

    override def validateObject(key: String, obj: PooledObject[JdbcConnection]): Boolean = {
      val dbURL = dataSource.get(key)
      val connection = obj.getObject.connection
      var st: Statement = null
      try {
        st = connection.createStatement
        st.execute("SELECT 1")
        return true
      } catch {
        case _: SQLException =>
          LOG.info("Invalidate connection for url: {}", dbURL)
      } finally try
        if (st != null) st.close()
      catch {
        case ex: SQLException =>
          LOG.info("Exception closing statement", ex)
      }
      false
    }

    override def activateObject(key: String, `object`: PooledObject[JdbcConnection]): Unit = {
    }

    override def passivateObject(key: String, `object`: PooledObject[JdbcConnection]): Unit = {
    }
  }


}

