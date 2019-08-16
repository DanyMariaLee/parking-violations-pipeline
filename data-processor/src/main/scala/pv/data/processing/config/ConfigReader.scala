package pv.data.processing.config

import cats.effect.IO
import pureconfig.generic.auto._

trait ConfigReader {

  def readConfig: IO[DataProcessingConfig] = pureconfig.loadConfig[DataProcessingConfig] match {
    case Left(e) => IO.raiseError(new RuntimeException(s"Failed to parse pv.view.config: $e"))
    case Right(c) => IO.pure(c)
  }
}
