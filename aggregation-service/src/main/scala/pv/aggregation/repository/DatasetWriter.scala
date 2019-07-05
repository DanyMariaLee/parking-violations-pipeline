package pv.aggregation.repository

import cats.effect.IO
import org.apache.spark.sql.{Dataset, SaveMode}

trait DatasetWriter {

  def overwrite[T](ds: Dataset[T], path: String): IO[Unit] =
    IO(ds.write.mode(SaveMode.Overwrite).parquet(path))
}
