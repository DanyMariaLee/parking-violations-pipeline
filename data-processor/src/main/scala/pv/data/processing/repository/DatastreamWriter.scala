package pv.data.processing.repository

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery, Trigger}

trait DatastreamWriter {

  def writeStream[T](ds: Dataset[T],
                     basePath: String,
                     table: String
                    ): StreamingQuery = {
    ds.writeStream
      .format("parquet")
      .outputMode(OutputMode.Append())
      .queryName(table)
      .option("spark.sql.streaming.schemaInference", "true")
      .option("checkpointLocation", s"$basePath$table/check-point/")
      .trigger(Trigger.ProcessingTime(10))
      .start(basePath + table)
  }
}
