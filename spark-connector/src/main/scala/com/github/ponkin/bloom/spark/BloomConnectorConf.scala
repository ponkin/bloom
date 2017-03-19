package com.github.ponkin.bloom.spark

import org.apache.spark.SparkConf

/**
 * Configuration for bloom server connector
 *
 * @author Alexey Ponkin
 */
case class BloomConnectorConf(
  host: String,
  port: Int,
  putBatchSize: Int,
  poolSize: Int,
  reqTimeout: Int
)

object BloomConnectorConf {

  val ConnectionPortParam = ConfigParameter[Int](
    name = "bloom-server.connection.port",
    default = 22022,
    description = "Bloom server native connection port"
  )

  val ConnectionHostParam = ConfigParameter[String](
    name = "bloom-server.connection.host",
    default = "",
    description = "Bloom server connection host"
  )

  /**
   * 0 and 1 - means that connector will
   * put elements from RDD one by one in parallel
   * negative value means that
   * connector will put all elements from RDD partition
   * in one request(good for small partitions),
   * other values will mean that connector will
   * put group of elements in bloom filter
   */
  val BatchPutElementParam = ConfigParameter[Int](
    name = "bloom-server.put.batch.size",
    default = 0,
    description = "Put elements in bloom filter batch size"
  )

  val ConnPoolSize = ConfigParameter[Int](
    name = "bloom-server.connection.pool.size",
    default = 5,
    description = "Maxximum number of active connections in the pool"
  )

  val RequestTimeout = ConfigParameter[Int](
    name = "bloom-server.request.timeout.seconds",
    default = 25,
    description = "Maximum seconds to wait for response"
  )

  def apply(conf: SparkConf): BloomConnectorConf = {
    val putBatchSize = conf.getInt(BatchPutElementParam.name, BatchPutElementParam.default)
    val port = conf.getInt(ConnectionPortParam.name, ConnectionPortParam.default)
    val host = conf.get(ConnectionHostParam.name, ConnectionHostParam.default)
    val poolSize = conf.getInt(ConnPoolSize.name, ConnPoolSize.default)
    val reqTimeout = conf.getInt(RequestTimeout.name, RequestTimeout.default)
    BloomConnectorConf(host, port, putBatchSize, poolSize, reqTimeout)
  }
}
