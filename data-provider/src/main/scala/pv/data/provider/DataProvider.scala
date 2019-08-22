package pv.data.provider

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import okhttp3.OkHttpClient

object DataProvider extends App with DataProviderProcess {

  val logger = Logger("data-provider")

  implicit val client = new OkHttpClient

  process.handleErrorWith {
    case e if e.getMessage.contains("Failed to connect to") =>
      IO.raiseError(new RuntimeException("Recipient service is not running"))
    case e => IO.raiseError(e)
  }.map(_ =>
    logger.info("All sent. Shutting down...")
  ).unsafeRunSync()
}
