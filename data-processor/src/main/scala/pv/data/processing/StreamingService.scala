package pv.data.processing

import akka.actor.ActorSystem
import cats.effect.IO
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pv.common.output.domain.WrappedNYCData
import pv.data.processing.analytics.AnalyticsService
import pv.data.processing.config.{ConfigReader, DataProcessingConfig}
import pv.data.processing.repository.{DatastreamReader, DatastreamWriter}
import pv.data.processing.schema.SchemaProvider

trait StreamingService
  extends SchemaProvider
    with DatastreamReader
    with DatastreamWriter
    with AnalyticsService
    with ConfigReader {

  val config = readConfig.unsafeRunSync()

  implicit def sparkSession: SparkSession

  implicit def actorSystem: ActorSystem

  def sparkConf = sparkSession.sparkContext.getConf

  def streamingContext = new StreamingContext(sparkConf, Seconds(2))

  def processStream[T](config: DataProcessingConfig,
                       aggregation: Dataset[WrappedNYCData] => Dataset[T],
                       table: String
                      ): IO[Unit] = {

    IO(readStream(config))
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
