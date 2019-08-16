package pv.data.provider.config

import cats.effect.IO
import pureconfig.generic.auto._

trait ConfigReader {

  def readConfig: IO[DataProviderConfig] = pureconfig.loadConfig[DataProviderConfig] match {
    case Left(e) => IO.raiseError(new RuntimeException(s"Failed to parse pv.view.config: $e"))
    case Right(c) => IO.pure(c)
  }
}
