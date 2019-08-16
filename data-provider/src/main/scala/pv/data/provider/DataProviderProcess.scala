package pv.data.provider

import cats.effect.IO
import org.apache.http.impl.client.CloseableHttpClient
import pv.common.json.JsonProtocols
import pv.common.parser.CSVParser
import pv.data.provider.config.ConfigReader
import pv.data.provider.service.RequestService

trait DataProviderProcess
  extends RequestService
    with CSVParser
    with ConfigReader
    with JsonProtocols {

  implicit def client: CloseableHttpClient

  def process: IO[Unit] =
    readConfig
      .flatMap { config =>
        IO {
          config.files.foreach { fileName =>
            readFile(fileName)
              .foreach { row =>
                fromString(row).collect {
                  case wrappedData => send(config.host, config.port, wrappedData)
                }.getOrElse {
                  logger.warn(s"No useful data available from row: $row")
                }
              }
          }
        }
      }

  private[provider] def readFile(fileName: String): Iterator[String] = {
    scala.io.Source
      //.fromFile(getClass.getResource(s"/$fileName").getPath)
      .fromInputStream(getClass.getResourceAsStream(s"/$fileName"))
      .getLines()
      .drop(1) //skip header
  }
}
