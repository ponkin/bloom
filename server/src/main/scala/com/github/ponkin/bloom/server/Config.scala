package com.github.ponkin.bloom.server

import java.nio.file.Path

/**
 * Main configuration model
 *
 * @author Alexey Pokin
 */
case class Config(bloom: BloomConfig)
case class BloomConfig(server: ServerConfig, storage: StorageConfig)
case class ServerConfig(
  host: String,
  port: Int,
  maxConcurrentRequests: Int,
  maxWaiters: Int,
  threadPoolSize: Option[Int]
)
case class StorageConfig(dataDir: Option[Path])

