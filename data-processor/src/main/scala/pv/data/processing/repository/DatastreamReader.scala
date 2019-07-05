package pv.data.processing.repository

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{Dataset, SparkSession}
import pv.common.output.domain.WrappedNYCData
import pv.data.processing.config.DataProcessingConfig
import pv.data.processing.schema.SchemaProvider

trait DatastreamReader extends SchemaProvider {

  def readStream(config: DataProcessingConfig)(implicit ss: SparkSession): Dataset[WrappedNYCData] = {
    import ss.implicits._

    ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .option("subscribe", config.topic)
      .option("group.id", config.groupId)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json($"value".cast("string"), schema).alias("value"))
      .select("value.*")
      .as[WrappedNYCData]
  }
}
