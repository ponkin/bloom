package com.github.ponkin.bloom.driver

import com.twitter.finagle.Thrift
import com.twitter.finagle.client.DefaultPool
import com.twitter.util.Future
import com.twitter.conversions.time._

/**
 * Client for BloomFilterStore
 *
 * @param address - address in form of "127.0.0.1:8080"
 * @param requestTimeout - wait in seconds for response
 * @param poolSize - maximum number of connections in pool
 *
 * @author Alexey Ponkin
 */
class Client(address: String, requestTimeout: Int = 25, poolSize: Int = 5) extends Serializable {

  @transient lazy val client = Thrift
    .client
    .withRequestTimeout(requestTimeout.seconds)
    .configured(DefaultPool.Param(
      low = 0, high = poolSize,
      idleTime = 30.seconds,
      bufferSize = 0,
      maxWaiters = Int.MaxValue
    ))
    .newIface[BloomFilterStore.FutureIface](address)

  def create(name: String, filter: Filter): Future[Unit] = client.create(name, filter)

  def destroy(name: String): Future[Unit] = client.destroy(name)

  def put(name: String, elements: Set[String]): Future[Unit] = client.put(name, elements)

  def mightContain(name: String, element: String): Future[Boolean] = client.mightContain(name, element)

  def clear(name: String): Future[Unit] = client.clear(name)

  def remove(name: String, elements: Set[String]) = client.remove(name, elements)

  def info(name: String) = client.info(name)

  def close(): Unit = Unit

}

object Client {

  def apply(address: String): Client = new Client(address)
  def apply(address: String, requestTimeout: Int, poolSize: Int): Client = new Client(address, requestTimeout, poolSize)

}

