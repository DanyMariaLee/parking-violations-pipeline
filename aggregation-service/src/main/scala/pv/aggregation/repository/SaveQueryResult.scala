package pv.aggregation.repository

import cats.effect.IO
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

trait SaveQueryResult extends DatasetReader with DatasetWriter {

  def saveResult[In: Encoder, Out: Encoder](basePath: String,
                                            table: String,
                                            query: Dataset[In] => Dataset[Out]
                                           )(implicit ss: SparkSession): IO[Unit] = {
    val readFrom = basePath + "/output/" + table

    read[In](readFrom).flatMap {
      case Some(ds) => overwrite(query(ds), basePath + "/query_result/" + table)
      case _ => IO.pure(Unit)
    }
  }

  def saveTopKResult[In: Encoder, Out: Encoder](basePath: String,
                                                table: String,
                                                query: (Dataset[In], Int) => Dataset[Out],
                                                topK: Int
                                               )(implicit ss: SparkSession): IO[Unit] =
    read[In](basePath + table).flatMap {
      case Some(ds) => overwrite(query(ds, topK), basePath + "/query_result/" + table)
      case _ => IO.pure(Unit)
    }

}
