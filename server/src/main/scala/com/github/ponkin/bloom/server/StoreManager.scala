package com.github.ponkin.bloom.server

import java.nio.file.Files
import java.io.File
import java.io.{ FileOutputStream, FileInputStream }

import org.apache.commons.io.{ FilenameUtils, IOUtils }
import org.apache.commons.lang3.StringUtils
import scala.util.{ Try, Success, Failure }

import org.log4s._

/**
 * Base trait for underlying
 * filter storage implementation.
 * filter persistence
 *
 * @author Alexey Ponkin
 */
trait StoreManager {

  def listAll: Seq[FilterDescriptor]

  def save(filter: FilterDescriptor): Try[Unit]

  def delete(filter: FilterDescriptor): Try[Unit]

}

/**
 * Disk Filter storage.
 * Filters are stored as simple files inside
 * <code>dataDir</code>. Storing
 * only meta information about filters @see [[FilterDescriptor]]
 *
 * @author Alexey Ponkin
 */
class DiskStoreManager(val dataDir: File)(implicit ser: Serializer[FilterDescriptor]) extends StoreManager {

  private[this] val logger = getLogger

  /**
   * list all filter descriptors
   * in current <code>dataDir</code>
   */
  val listAll = getListOfIndexes(dataDir).map { file =>
    Try(new FileInputStream(file)) match {
      case Success(input) => ser.readFrom(input)
      case Failure(err) =>
        logger.error(err)(s"""Something is wrong with $file""")
        None
    }
  }.flatten

  def save(filter: FilterDescriptor): Try[Unit] = {
    val indexFile = indexFileByName(filter.name)
    Try(Files.createFile(indexFile.toPath)).map(_ => saveFilterMeta(indexFile, filter))
  }

  def delete(filter: FilterDescriptor): Try[Unit] = Try(removeIndexFile(filter.name))

  private[bloom] def saveFilterMeta(indexFile: File, desc: FilterDescriptor): Unit = {
    Try(new FileOutputStream(indexFile)) match {
      case Success(output) =>
        ser.writeTo(desc, output)
        IOUtils.closeQuietly(output)
      case Failure(err) =>
        logger.error(err)(s"""Something is wrong with $indexFile""")
    }
  }

  @throws[Exception]
  private[bloom] def removeIndexFile(name: String): Unit = Files.delete(indexFileByName(name).toPath)

  private[bloom] def indexFileByName(name: String): File = new File(dataDir, s"$name.index")

  private[bloom] def getListOfIndexes(dir: File): List[File] = Option(dir.listFiles) match {
    case Some(files) => files.filter { f =>
      f.isFile &&
        StringUtils.isNotBlank(f.getName) &&
        FilenameUtils.getExtension(f.getPath) == "index"
    }.toList
    case None => List.empty
  }
}

object DiskStoreManager {

  def apply(conf: StorageConfig)(implicit ser: Serializer[FilterDescriptor]): StoreManager =
    new DiskStoreManager(conf.dataDir.getOrElse(Files.createTempDirectory("filters")).toFile)
}

