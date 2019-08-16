package pv.data.processing.repository

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{Dataset, SparkSession}
import pv.common.output.domain.WrappedNYCData
import pv.data.processing.config.DataProcessingConfig
import pv.data.processing.schema.SchemaProvider

trait DatastreamReader extends SchemaProvider {

  def readStream(config: DataProcessingConfig, table: String)(implicit ss: SparkSession): Dataset[WrappedNYCData] = {
    import ss.implicits._

    ss
      .readStream
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .option("subscribe", config.topic)
      .option("group.id", config.groupId + table)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json($"value".cast("string"), schema).alias("value"))
      .select("value.*")
      .as[WrappedNYCData]
  }
}
