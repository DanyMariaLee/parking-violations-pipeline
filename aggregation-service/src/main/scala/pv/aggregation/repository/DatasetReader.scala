package pv.aggregation.repository

import cats.effect.IO
import org.apache.spark.sql.{AnalysisException, Dataset, Encoder, SparkSession}

import scala.util.{Failure, Success, Try}

trait DatasetReader {

  def read[T: Encoder](path: String)(implicit ss: SparkSession): IO[Option[Dataset[T]]] = {
    Try(ss.read.parquet(path).as[T]) match {
      case Success(ds) => IO.pure(Option(ds))
      case Failure(e: AnalysisException)
        if e.getMessage.contains("Path does not exist") => IO.pure(None)
      case Failure(e) => IO.raiseError(e)
    }
  }

}
