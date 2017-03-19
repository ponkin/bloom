package com.github.ponkin.bloom.spark

case class ConfigParameter[T](name: String, default: T, description: String)
