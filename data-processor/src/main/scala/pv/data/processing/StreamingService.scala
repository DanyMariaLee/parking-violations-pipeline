package pv.data.processing

import cats.effect.IO
import org.apache.spark.sql.{Dataset, SparkSession}
import pv.common.output.domain.WrappedNYCData
import pv.data.processing.config.{ConfigReader, DataProcessingConfig}
import pv.data.processing.repository.{DatastreamReader, DatastreamWriter}
import pv.data.processing.schema.SchemaProvider

trait StreamingService
  extends SchemaProvider
    with DatastreamReader
    with DatastreamWriter
    with ConfigReader {

  val config = readConfig.unsafeRunSync()

  def processStream[T](config: DataProcessingConfig,
                       aggregation: Dataset[WrappedNYCData] => Dataset[T],
                       table: String
                      )(implicit ss: SparkSession): IO[Unit] = {

    IO(readStream(config, table))
      .map { stream =>
        writeStream(
          aggregation(stream),
          config.basePath,
          table
        )
          .awaitTermination()
      }
  }

}
