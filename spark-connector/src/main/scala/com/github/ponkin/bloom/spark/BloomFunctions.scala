package com.github.ponkin.bloom.spark

import com.github.ponkin.bloom.driver.Client
import com.twitter.util.{ Await, Future }
import org.apache.spark.{ TaskContext, SparkContext }
import org.apache.spark.rdd.RDD

/**
 * Enrich standart RDD with additional
 * functions to put rdd in remote bloom filter
 */
class BloomFunctions[T](rdd: RDD[(String, T)]) {

  private val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Put all keys from RDD to
   * remote bloom filter with `name`
   */
  def putInBloomFilter(
    name: String
  )(
    implicit
    conn: BloomConnector = new BloomConnector(BloomConnectorConf(sparkContext.getConf))
  ): Unit = {
    sparkContext.runJob(rdd, put(name))
  }

  private[spark] def put(
    filter: String
  )(
    implicit
    conn: BloomConnector
  ): (TaskContext, Iterator[(String, T)]) => Unit = { (ctx, partition) =>
    conn.withClientDo { client =>
      Await.result(
        conn.conf.putBatchSize match {
          case 0 | 1 =>
            Future.collect(
              partition.map(row => client.put(filter, Set(row._1)))
              .toSeq
            ).unit
          case n if n > 1 =>
            Future.collect(
              partition.map(_._1)
              .sliding(n, n)
              .map(seq => client.put(filter, seq.toSet))
              .toSeq
            ).unit
          case _ =>
            client.put(filter, partition.map(_._1).toSet)
        }
      )
    }
  }

  /**
   * Create RDD with flag - whether
   * key is inside bloom filter
   */
  def mightContain(
    filterName: String
  )(
    implicit
    conn: BloomConnector = new BloomConnector(BloomConnectorConf(sparkContext.getConf))
  ): BloomFilterRDD[T] = {
    new BloomFilterRDD(rdd, filterName, conn)
  }

}

object BloomFunctions {

  implicit def addBloomFucntions[T](rdd: RDD[(String, T)]) = new BloomFunctions(rdd)

}
