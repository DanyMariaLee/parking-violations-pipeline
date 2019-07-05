package pv.aggregation.repository

import cats.effect.IO
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

trait UpdateDataset extends DatasetReader with DatasetWriter {

  def updateAndOverwrite[T1: Encoder, T2: Encoder](basePath: String,
                                                   aggregateFunction: Dataset[T1] => Dataset[T2],
                                                   tableFrom: String,
                                                   tableTo: Option[String] = None
                                                  )(implicit ss: SparkSession): IO[Unit] = {
    val prefix = "output/"

    val readFrom = basePath + tableFrom
    val writeTo = tableTo.map(t => basePath + prefix + t)
      .getOrElse(basePath + prefix + tableFrom)

    read[T1](readFrom).flatMap {
      case Some(oldDs) =>
        val agg = aggregateFunction(oldDs)
        overwrite(agg, writeTo)
      case _ => IO.pure(Unit)
    }
  }
}
