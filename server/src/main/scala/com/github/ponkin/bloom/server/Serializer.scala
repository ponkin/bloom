package com.github.ponkin.bloom.server

import java.io.{ OutputStream, InputStream }

import scala.util.Try

trait Serializer[T] {

  def readFrom(input: InputStream): Option[T]

  def writeTo(meta: T, output: OutputStream): Try[Unit]
}

