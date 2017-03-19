package com.github.ponkin.bloom.server

import com.twitter.util.{ Future, FuturePool }

import scala.collection.Set
import scala.collection.concurrent.TrieMap
import scala.util.{ Try, Success, Failure }

import collection.JavaConverters._

import java.util.concurrent.Executors
import java.io.File
import java.nio.file.{ Path, Files }

import com.github.ponkin.bloom.driver.{ BloomFilterStore, Filter => TFilter, FilterType }
import com.github.ponkin.bloom.{
  Filter,
  BloomFilter,
  CuckooFilter,
  StableBloomFilter
}

import org.log4s._

/**
 *
 * @author Alexey Ponkin
 */
class BloomFilterStoreImpl(val storage: StoreManager) extends BloomFilterStore[Future] {

  private[this] val logger = getLogger

  private[this] val futurePool = FuturePool(Executors.newWorkStealingPool())

  private[this] val sharedMem = new File("/dev/shm")

  /**
   * Load filter descriptors from
   * underlying storage and create
   * filters according to descriptor
   */
  private[this] val filters: TrieMap[String, FilterEntity] =
    TrieMap(storage.listAll.map(fd => (fd.name, FilterEntity(fd, actualFilter(fd)))): _*)

  /**
   * Create new filter
   * will return exeception if filter with the
   * same name is already exists
   */
  def create(name: String, meta: TFilter): Future[Unit] = {
    val fd = descriptor(name, meta) // create descriptor for filter
    storage.save(fd) match { // this block is thread safe
      case Success(_) => Future(filters.put(name, FilterEntity(fd, actualFilter(fd))))
      case Failure(err) => Future.exception(err)
    }
  }

  def destroy(name: String): Future[Unit] = {
    filters.remove(name) match {
      case Some(entity) =>
        entity.filter.close()
        entity.descriptor.dataPath match {
          case Some(path) => Try(Files.delete(dataFile(path).toPath))
          case None => Success(Unit)
        }
        storage.delete(entity.descriptor) match {
          case Success(_) => Future.Unit
          case Failure(err) => Future.exception(err)
        }
      case None => Future.Unit
    }
  }

  def put(name: String, elements: Set[String]): Future[Unit] = filters.get(name) match {
    case Some(entity) => futurePool(entity.filter.put(elements.asJava))
    case None => Future.exception(NoSuchFilterFound(name))
  }

  def mightContain(name: String, element: String): Future[Boolean] = filters.get(name) match {
    case Some(entity) => futurePool(entity.filter.mightContain(element))
    case None => Future.exception(NoSuchFilterFound(name))
  }

  def clear(name: String): Future[Unit] = filters.get(name) match {
    case Some(entity) => futurePool(entity.filter.clear())
    case None => Future.exception(NoSuchFilterFound(name))
  }

  def remove(name: String, elements: Set[String]): Future[Unit] = filters.get(name) match {
    case Some(entity) => futurePool(entity.filter.remove(elements.asJava))
    case None => Future.exception(NoSuchFilterFound(name))
  }

  def info(name: String): Future[String] = filters.get(name) match {
    case Some(entity) => Future(entity.descriptor.toString)
    case None => Future.exception(NoSuchFilterFound(name))
  }

  def close(): Unit = {
    filters.values.foreach(_.filter.close())
  }

  private def actualFilter(desc: FilterDescriptor): Filter = {
    val useOffHeap = desc.options.get("useOffHeap").map(_.toBoolean).getOrElse(true)
    val bitsPerBucket = desc.options.get("bitsPerBucket").map(_.toInt).getOrElse(1)
    val bldr = desc.filterType match {
      case BloomType.Standart =>
        BloomFilter.builder()
      case BloomType.Stable =>
        StableBloomFilter.builder().withBitsPerBucket(bitsPerBucket)
      case BloomType.Cuckoo =>
        CuckooFilter.builder()
    }

    val wfbldr = desc.dataPath match {
      case Some(path) =>
        val backedFile = dataFile(path)
        backedFile.createNewFile() // create if not exists
        bldr.withFileMapped(backedFile)
      case None => bldr
    }
    wfbldr.useOffHeapMemory(useOffHeap)
      .withFalsePositiveRate(desc.fpp)
      .withExpectedNumberOfItems(desc.maxElements)
      .build()
  }

  private[this] def descriptor(name: String, meta: TFilter): FilterDescriptor = {
    val filterType: BloomType.Value = meta.filterType match {
      case FilterType.Stable => BloomType.Stable
      case FilterType.Standart => BloomType.Standart
      case FilterType.Cuckoo => BloomType.Cuckoo
    }
    val path = meta.params.get("persist").map(_.toBoolean) match {
      case Some(true) => Some(s"""$name.data""")
      case _ => None
    }
    FilterDescriptor(name, filterType, meta.numEntries, meta.fpProbe, path, meta.params)
  }

  private[this] def dataFile(name: String): File = new File(sharedMem, name)
}

case class NoSuchFilterFound(filterName: String) extends Exception(s"There is no filter with name '$filterName'")
