package pv.aggregation.config

import cats.effect.IO
import pureconfig.generic.auto._

trait ConfigReader {

  def readConfig: IO[AggregationConfig] = pureconfig.loadConfig[AggregationConfig] match {
    case Left(e) => IO.raiseError(new RuntimeException(s"Failed to parse config: $e"))
    case Right(c) => IO.pure(c)
  }
}
