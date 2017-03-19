package com.github.ponkin.bloom.spark

import com.github.ponkin.bloom.driver.Client

/**
 * Bloom server connector
 *
 * @author Alexey Ponkin
 */
class BloomConnector(val conf: BloomConnectorConf)
    extends Serializable {

  @transient
  private lazy val client = Client(s"${conf.host}:${conf.port}", conf.reqTimeout, conf.poolSize)

  def withClientDo[T](code: Client => T): T = {
    closeResourceAfterUse(client) { client =>
      code(client)
    }
  }

  private[spark] def closeResourceAfterUse[T, C <: { def close() }](closeable: C)(code: C => T): T =
    try code(closeable) finally {
      closeable.close()
    }
}
