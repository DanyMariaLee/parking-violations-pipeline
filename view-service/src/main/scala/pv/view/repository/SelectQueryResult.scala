package pv.view.repository

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import pv.aggregation.repository.{DatasetReader, DatasetWriter}

trait SelectQueryResult extends DatasetReader with DatasetWriter {

  def selectResult[In: Encoder, Out: Encoder](basePath: String,
                                              table: String,
                                              query: Dataset[In] => Dataset[Out]
                                             )(implicit ss: SparkSession): Seq[Out] = {
    select[In](basePath, table)
      .map(query(_).limit(20).collect().toSeq)
      .getOrElse(Nil)
  }

  def selectTopKResult[In: Encoder, Out: Encoder](basePath: String,
                                                  table: String,
                                                  query: (Dataset[In], Int) => Dataset[Out],
                                                  topK: Int
                                                 )(implicit ss: SparkSession): Seq[Out] = {
    select[In](basePath, table)
      .map(query(_, topK).collect().toSeq)
      .getOrElse(Nil)
  }

  private def select[In: Encoder](basePath: String,
                                  table: String)(implicit ss: SparkSession): Option[Dataset[In]] = {
    val readFrom = basePath + "/output/" + table

    read[In](readFrom)
      .unsafeRunSync()
  }

}

