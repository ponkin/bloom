package com.github.ponkin.bloom.spark

import scala.reflect.ClassTag
import com.twitter.util.Await
import org.apache.spark.rdd.RDD
import org.apache.spark.{
  SparkContext,
  Dependency,
  Partition,
  TaskContext
}

import scala.reflect.ClassTag

/**
 * @author Alexey Ponkin
 */
class BloomFilterRDD[T](
    left: RDD[(String, T)],
    filterName: String,
    val connector: BloomConnector
) extends RDD[(String, (T, Boolean))](left) {

  override def compute(split: Partition, context: TaskContext): Iterator[(String, (T, Boolean))] = {
    val requests = connector.withClientDo { client =>
      left.iterator(split, context).map {
        case (key, row) => client.mightContain(filterName, key)
      }
    }
    left.iterator(split, context).zip(requests).map {
      case ((key, row), future) => (key, (row, Await.result(future)))
    }
  }

  override protected def getPartitions: Array[Partition] = left.partitions

}
