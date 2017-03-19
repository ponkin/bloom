package com.github.ponkin.bloom.server

import scala.collection.Map

object BloomType extends Enumeration {
  val Standart, Stable, Cuckoo = Value
}

case class FilterDescriptor(
  name: String,
  filterType: BloomType.Value,
  maxElements: Long,
  fpp: Double,
  dataPath: Option[String] = None,
  options: Map[String, String] = Map.empty
)

