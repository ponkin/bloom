package com.github.ponkin.bloom.server

import com.twitter.util.{ Await, Future }
import pureconfig._

import java.nio.file.{ Path, Paths }
import scala.util.{ Try, Success, Failure }

import org.log4s._

/**
 * Server instance.
 * All configuration is set in application.conf
 *
 * @author Alexey Ponkin
 */
object Server extends App {

  private[this] val logger = getLogger

  implicit val deriveStringConvertForPath = ConfigConvert.fromString[Path](s => Try(Paths.get(s)))

  val serverProc = loadConfig[Config] match {
    case Success(settings) =>
      logger.debug(s"Starting server with config $settings")
      BloomServer(settings.bloom).run
    case Failure(err) =>
      logger.error(err)(s"Something is wrong with application.conf")
      Future.exception(err)
  }

  Await.ready(serverProc)
}

