package pv.aggregation.repository

import cats.effect.IO
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

trait ShowQueryResult extends DatasetReader with DatasetWriter {

  def showResult[In: Encoder, Out: Encoder](basePath: String,
                                            table: String,
                                            query: Dataset[In] => Dataset[Out]
                                           )(implicit ss: SparkSession): IO[Unit] = {
    val readFrom = basePath + "/output/" + table

    read[In](readFrom).flatMap {
      case Some(ds) => IO(query(ds).show())
      case _ => IO.pure(Unit)
    }
  }

  def showTopKResult[In: Encoder, Out: Encoder](basePath: String,
                                                table: String,
                                                query: (Dataset[In], Int) => Dataset[Out],
                                                topK: Int
                                               )(implicit ss: SparkSession): IO[Unit] =
    read[In](basePath + table).flatMap {
      case Some(ds) => IO(query(ds, topK).show())
      case _ => IO.pure(())
    }

}
