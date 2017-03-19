package com.github.ponkin.bloom.server

import org.apache.commons.io.IOUtils

import scala.util.{ Try, Success, Failure }
import java.io.{ InputStream, OutputStream }

import com.twitter.chill.ScalaKryoInstantiator

import org.log4s._

/**
 * Serializer that uses
 * scala Kryo lib
 */
object KryoSerializer {

  private[this] val logger = getLogger

  implicit val descriptorSerializer = new Serializer[FilterDescriptor] {

    def readFrom(input: InputStream): Option[FilterDescriptor] =
      Try(IOUtils.toByteArray(input)) match {
        case Success(bytes) =>
          Try(ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[FilterDescriptor]).toOption
        case Failure(err) =>
          logger.error(err)("Error while deserialize FilterDescriptor")
          None
      }

    def writeTo(meta: FilterDescriptor, output: OutputStream): Try[Unit] = {
      val bytes = ScalaKryoInstantiator.defaultPool.toBytesWithClass(meta)
      Try(output.write(bytes, 0, bytes.length))
    }
  }
}
