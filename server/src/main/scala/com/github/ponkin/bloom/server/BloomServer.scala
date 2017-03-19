package com.github.ponkin.bloom.server

import com.twitter.finagle.Thrift

class BloomServer(val conf: BloomConfig) {

  import KryoSerializer._

  val storeImpl = new BloomFilterStoreImpl(DiskStoreManager(conf.storage))

  def run = Thrift
    .server
    .withLabel("BloomServer")
    .serveIface(s"${conf.server.host}:${conf.server.port}", storeImpl)
}

object BloomServer {
  def apply(conf: BloomConfig) = new BloomServer(conf)
}
